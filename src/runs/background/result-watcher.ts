import * as fs from "node:fs";
import * as path from "node:path";
import { buildCompletionKey, markSeenWithTtl } from "./completion-dedupe.ts";
import { createFileCoalescer } from "../../shared/file-coalescer.ts";
import {
	SUBAGENT_ASYNC_COMPLETE_EVENT,
	type IntercomEventBus,
	type NestedRunSummary,
	type SubagentResultIntercomChild,
	type SubagentState,
} from "../../shared/types.ts";
import {
	attachNestedChildrenToResultChildren,
	buildSubagentResultIntercomPayload,
	compactNestedResultChildren,
	deliverSubagentResultIntercomEvent,
	resolveSubagentResultStatus,
} from "../../intercom/result-intercom.ts";
import { projectNestedRegistryForRoot, sanitizeSummary } from "../shared/nested-events.ts";

const WATCHER_RESTART_DELAY_MS = 3000;
const POLL_INTERVAL_MS = 3000;

type ResultWatcherFs = Pick<typeof fs, "existsSync" | "readFileSync" | "unlinkSync" | "readdirSync" | "mkdirSync" | "realpathSync" | "watch">;

type ResultWatcherTimers = {
	setTimeout: typeof setTimeout;
	clearTimeout: typeof clearTimeout;
	setInterval: typeof setInterval;
	clearInterval: typeof clearInterval;
};

type ResultWatcherDeps = {
	fs?: ResultWatcherFs;
	timers?: ResultWatcherTimers;
};

type ResultFileChild = {
	agent?: string;
	output?: string;
	error?: string;
	success?: boolean;
	state?: string;
	stopped?: boolean;
	sessionFile?: string;
	artifactPaths?: { outputPath?: string };
	intercomTarget?: string;
	children?: unknown;
};

type ResultFileData = {
	id?: string;
	runId?: string;
	agent?: string;
	success?: boolean;
	state?: string;
	mode?: string;
	summary?: string;
	results?: ResultFileChild[];
	nestedChildren?: unknown;
	sessionId?: string;
	cwd?: string;
	sessionFile?: string;
	asyncDir?: string;
	intercomTarget?: string;
};

function sanitizeNestedResultChildren(value: unknown, resultPath: string, label: string): NestedRunSummary[] | undefined {
	if (value === undefined) return undefined;
	if (!Array.isArray(value)) {
		console.error(`Ignoring invalid nested children in subagent result file '${resultPath}' at ${label}: expected an array.`);
		return undefined;
	}
	const children = value.map((child) => sanitizeSummary(child)).filter((child): child is NestedRunSummary => Boolean(child));
	if (children.length !== value.length) {
		console.error(`Ignoring ${value.length - children.length} invalid nested child record(s) in subagent result file '${resultPath}' at ${label}.`);
	}
	return children.length ? children : undefined;
}

function getErrorCode(error: unknown): string | undefined {
	return typeof error === "object" && error !== null && "code" in error
		? (error as NodeJS.ErrnoException).code
		: undefined;
}

function isNotFoundError(error: unknown): boolean {
	return getErrorCode(error) === "ENOENT";
}

function shouldFallBackToPolling(error: unknown): boolean {
	const code = getErrorCode(error);
	return code === "EMFILE" || code === "ENOSPC";
}

function resolveNativeWatchDir(fsApi: ResultWatcherFs, resultsDir: string): string {
	try {
		return fsApi.realpathSync.native(resultsDir);
	} catch {
		return resultsDir;
	}
}

/* 监听后台 Subagent 写出的结果 JSON 文件，把文件结果转换成内部事件，通知父 Agent，然后删除已消费的结果文件
它是后台子 Agent 与父 Agent 之间的一座“文件消息桥”
后台 Subagent 完成
    ↓
写入 resultsDir/<runId>.json
    ↓
ResultWatcher 发现文件
    ↓
读取、校验、规范化结果
    ↓
emit("subagent:async-complete")
    ↓
AsyncJobTracker / Notify / UI 收到完成事件
    ↓
删除 JSON 文件 
*/
export function createResultWatcher(
	// 扩展内部事件总线，用来发送完成事件到 AsyncJobTracker / Notify / UI
	pi: { events: IntercomEventBus },
	// 共享运行状态，保存当前 Session ID、Watcher、去重记录等
	state: SubagentState,
	// 后台任务结果文件目录
	resultsDir: string,
	// 完成事件去重记录的有效时间
	completionTtlMs: number,
	// 注入文件系统和 Timer API，主要方便测试
	deps: ResultWatcherDeps = {},
): {
	startResultWatcher: () => void; // 启动监听
	primeExistingResults: () => void; // 扫描已有结果
	stopResultWatcher: () => void; // 停止监听
} {
	// 文件系统和 Timer 依赖
	const fsApi = deps.fs ?? fs;
	const timers = deps.timers ?? { setTimeout, clearTimeout, setInterval, clearInterval };

	// 处理一个结果文件，它接收的是文件名，例如：run-abc123.json
	const handleResult = async (file: string) => {
		// 计算完整路径得到：/tmp/pi-subagents/async-subagent-results/run-abc123.json
		const resultPath = path.join(resultsDir, file);
		// 文件不存在就结束，fs.watch 可能对同一个文件产生多个事件。第一次处理已经删除文件后，第二次事件仍可能到达，因此这里直接忽略
		if (!fsApi.existsSync(resultPath)) return;
		try {
			// 读取并解析 JSON 内容，后台 Agent 进程把执行结果写成 JSON，父进程在这里还原成 ResultFileData
			const data = JSON.parse(fsApi.readFileSync(resultPath, "utf-8")) as ResultFileData;
			// 如果 Session ID 不匹配，说明不是当前 Session 的结果，直接忽略
			if (typeof data.sessionId !== "string" || data.sessionId !== state.currentSessionId) return;

			// 确定 Run ID，按优先级取：data.runId、data.id、JSON 文件名。这样可以兼容不同版本的结果文件格式（技术债务）
			const runId = data.runId ?? data.id ?? file.replace(/\.json$/i, "");
			// 给结果补上嵌套子 Agent 信息，因为后台子 Agent 在执行时，会自动把嵌套子 Agent 的信息写入结果文件
			const hasExplicitNestedChildren = data.nestedChildren !== undefined;
			// 清理嵌套子任务信息，移除无效的子任务，并确保格式正确，再压缩成紧凑数组
			let nestedChildren = compactNestedResultChildren(sanitizeNestedResultChildren(data.nestedChildren, resultPath, "nestedChildren"));
			/* 
			关键是区分两种情况：
			- { "nestedChildren": [] } 空数组，表示没有嵌套子任务
			- {} 表示没有嵌套子任务，表示根本没写 nestedChildren，可能是旧格式或遗漏，需要从 Registry 补
			Registry 是另一份专门记录“父子任务关系”的文件
			例如运行过程中记录：
			run-1
			  ├── run-2
			  └── run-3
			但最终结果文件可能只有：
			{
			  "id": "run-1",
			  "summary": "完成"
			}
			没有 nestedChildren。这时：projectNestedRegistryForRoot("run-1")，去 Registry 查询：
			{
			  "rootRunId": "run-1",
			  "children": [
				{ "runId": "run-2" },
				{ "runId": "run-3" }
			  ]
			}
			再把这些 children 补到最终结果中。
			所以“从 Registry 补”就是：最终结果文件没保存父子关系，就从独立的任务关系登记表中恢复。 
			*/
			if (!nestedChildren?.length && !hasExplicitNestedChildren) {
				try {
					nestedChildren = compactNestedResultChildren(projectNestedRegistryForRoot(runId)?.children);
				} catch (error) {
					console.error(`Failed to enrich subagent result file '${resultPath}' with nested registry children; will retry later:`, error);
					return;
				}
			}
			const now = Date.now();
			/* 完成事件去重，根据任务和文件生成唯一完成键，避免重复发送完成事件。
			existsSync() 只能判断“文件现在存不存在”，不能判断“这个结果以前处理过没有”。
			
			关键问题是并发：
			Watcher 触发处理 A
			→ 文件存在
			
			Poller 几乎同时触发处理 B
			→ 文件也存在
			
			A、B 都通过 existsSync()，因为 A 还没处理完、还没删除文件。这时：
			A 调用 markSeenWithTtl
			→ 第一次，返回 false
			→ 正常发送完成事件
			
			B 调用 markSeenWithTtl
			→ 已经见过，返回 true
			→ 删除文件并退出
			没有 markSeenWithTtl()，A 和 B 都可能发送完成事件。

			另外，后台进程也可能在文件删除后又写一次相同结果：
			旧文件已删除
			→ 新的同名文件又出现
			→ existsSync() 仍然是 true

			但 completionKey 已记录，所以仍能识别为重复结果。因此：
			existsSync：防止处理已不存在的文件
			completionSeen：防止重复处理同一个业务结果
			两者解决的问题不同。 */
			const completionKey = buildCompletionKey(data, `result:${file}`);
			if (markSeenWithTtl(state.completionSeen, completionKey, now, completionTtlMs)) {
				fsApi.unlinkSync(resultPath);
				return;
			}

			// 兼容单结果和多结果
			const hasResultChildren = Array.isArray(data.results) && data.results.length > 0;
			const resultChildren = hasResultChildren
				? data.results!
				: [{
					agent: data.agent,
					output: data.summary,
					success: data.success,
				}];
			// 规范化每个子结果
			const normalizedChildren = attachNestedChildrenToResultChildren(runId, resultChildren.map((result = {}, index): SubagentResultIntercomChild => {
				const baseOutput = result.output ?? data.summary;
				const hasRealOutput = typeof baseOutput === "string" && baseOutput.trim().length > 0;
				const output = hasRealOutput ? baseOutput : "(no output)";
				const summary = result.success === false && result.error
					? `${result.error}${hasRealOutput ? `\n\nOutput:\n${baseOutput}` : ""}`
					: output;
				const sessionPath = result.sessionFile ?? (resultChildren.length === 1 ? data.sessionFile : undefined);
				const childNestedChildren = sanitizeNestedResultChildren(result.children, resultPath, `results[${index}].children`);
				const childState = result.state === "paused" || result.state === "stopped"
					? result.state
					: result.stopped === true
						? "stopped"
						: data.state === "paused" || (!hasResultChildren && (data.state === "stopped" || typeof result.success !== "boolean"))
							? data.state
							: undefined;
				return {
					agent: result.agent ?? data.agent ?? `step-${index + 1}`,
					status: resolveSubagentResultStatus({
						success: result.success,
						state: childState,
					}),
					summary,
					index,
					artifactPath: result.artifactPaths?.outputPath,
					...(typeof sessionPath === "string" && fsApi.existsSync(sessionPath) ? { sessionPath } : {}),
					...(result.intercomTarget ? { intercomTarget: result.intercomTarget } : {}),
					...(childNestedChildren ? { children: childNestedChildren } : {}),
				};
			}), nestedChildren);

			const intercomTarget = data.intercomTarget?.trim();
			if (intercomTarget) {
				const mode = data.mode === "single" || data.mode === "parallel" || data.mode === "chain"
					? data.mode
					: resultChildren.length > 1 ? "chain" : "single";
				const payload = buildSubagentResultIntercomPayload({
					to: intercomTarget,
					runId,
					mode,
					source: "async",
					children: normalizedChildren,
					asyncId: data.id,
					asyncDir: data.asyncDir,
				});
				const delivered = await deliverSubagentResultIntercomEvent(pi.events, payload);
				if (!delivered) {
					console.error(`Subagent async grouped result intercom delivery was not acknowledged for '${resultPath}'.`);
				}
			}

			/* 发出后台完成事件，这是最关键的一步。
			事件通常会被以下模块监听：
			AsyncJobTracker
				-> 更新 state.asyncJobs
				-> 刷新 Widget
			
			SubagentNotify
				-> 把完成结果通知父 Agent
			
			wait Tool
				-> 唤醒正在等待的父 Agent 
				
			所以结果 JSON 在这里类似一个“一次性消息”：
			写入 -> 消费 -> 删除
			如果读取或处理过程中出现普通错误，文件不会删除，之后可以重试
			*/
			pi.events.emit(SUBAGENT_ASYNC_COMPLETE_EVENT, {
				...data,
				runId,
				...(nestedChildren?.length ? { nestedChildren } : {}),
				...(Array.isArray(data.results) ? {
					results: hasResultChildren
						? normalizedChildren.map((child, index) => ({
							...data.results![index],
							agent: child.agent,
							status: child.status,
							summary: child.summary,
							index: child.index,
							artifactPath: child.artifactPath,
							sessionPath: child.sessionPath,
							children: child.children,
						}))
						: [],
				} : {}),
			});
			// 处理成功后删除文件
			fsApi.unlinkSync(resultPath);
		} catch (error) {
			if (isNotFoundError(error)) return;
			console.error(`Failed to process subagent result file '${resultPath}':`, error);
		}
	};

	// 作用是把短时间内针对同一个文件的多个事件合并。延迟 50ms 是因为 fs.watch 可能在文件创建、写
	// 入、重命名时连续触发（rename、change、rename），如果立刻读取，文件可能还没写完。延迟和合并
	// 可以降低读取半截 JSON、重复处理和重复通知的风险
	state.resultFileCoalescer = createFileCoalescer((file) => {
		void handleResult(file);
	}, 50);

	// 扫描已有结果，把所有 .json 文件加入 Coalescer 等待处理，它解决 fs.watch 监听不到历史文件的问题
	// 父 Pi 关闭 → 后台子 Agent 完成并写入 JSON → 父 Pi 重新启动 → primeExistingResults 扫描并补处理
	const primeExistingResults = () => {
		try {
			fsApi.readdirSync(resultsDir)
				.filter((f) => f.endsWith(".json"))
				.forEach((file) => state.resultFileCoalescer.schedule(file, 0));
		} catch (error) {
			if (isNotFoundError(error)) return;
			console.error(`Failed to scan subagent result directory '${resultsDir}':`, error);
		}
	};

	// Watcher 不可用时降级轮询
	// 因此系统有两种工作模式：
	// 正常：fs.watch 事件驱动
	// 异常：setInterval 定时扫描
	const startPollingFallback = (reason: unknown) => {
		// 先关闭失效 Watcher
		state.watcher?.close();
		state.watcher = null;
		if (state.watcherRestartTimer) return;

		console.error(
			`Subagent result watcher for '${resultsDir}' fell back to polling because native fs.watch is unavailable (${getErrorCode(reason) ?? "unknown error"}).`,
		);
		// 然后立即扫描一次
		primeExistingResults();
		// 然后每 3 秒扫描一次
		state.watcherRestartTimer = timers.setInterval(primeExistingResults, POLL_INTERVAL_MS);
		state.watcherRestartTimer.unref?.();
	};

	// 普通临时错误不会立即永久降级轮询，而是安排稍后重启 Watcher
	const scheduleRestart = () => {
		if (state.watcherRestartTimer) return;
		state.watcherRestartTimer = timers.setTimeout(() => {
			state.watcherRestartTimer = null;
			try {
				fsApi.mkdirSync(resultsDir, { recursive: true });
				startResultWatcher();
			} catch (error) {
				if (shouldFallBackToPolling(error)) {
					startPollingFallback(error);
					return;
				}
				console.error(`Failed to restart subagent result watcher for '${resultsDir}':`, error);
				scheduleRestart();
			}
		}, WATCHER_RESTART_DELAY_MS);
		state.watcherRestartTimer.unref?.();
	};

	// 保证重复调用不会创建多个 Watcher
	const startResultWatcher = () => {
		if (state.watcher) return;
		if (state.watcherRestartTimer) {
			timers.clearTimeout(state.watcherRestartTimer);
			timers.clearInterval(state.watcherRestartTimer);
			state.watcherRestartTimer = null;
		}
		try {
			const watchDir = resolveNativeWatchDir(fsApi, resultsDir);
			state.watcher = fsApi.watch(watchDir, (ev, file) => {
				if (ev !== "rename" || !file) return;
				const fileName = file.toString();
				if (!fileName.endsWith(".json")) return;
				state.resultFileCoalescer.schedule(fileName);
			});
			state.watcher.on("error", (error) => {
				if (shouldFallBackToPolling(error)) {
					startPollingFallback(error);
					return;
				}
				console.error(`Subagent result watcher failed for '${resultsDir}':`, error);
				state.watcher?.close();
				state.watcher = null;
				scheduleRestart();
			});
			state.watcher.unref?.();
		} catch (error) {
			if (shouldFallBackToPolling(error)) {
				startPollingFallback(error);
				return;
			}
			console.error(`Failed to start subagent result watcher for '${resultsDir}':`, error);
			state.watcher = null;
			scheduleRestart();
		}
	};

	// 停止监听，清理资源
	const stopResultWatcher = () => {
		state.watcher?.close();
		state.watcher = null;
		if (state.watcherRestartTimer) {
			timers.clearTimeout(state.watcherRestartTimer);
			timers.clearInterval(state.watcherRestartTimer);
		}
		state.watcherRestartTimer = null;
		state.resultFileCoalescer.clear();
	};

	return { startResultWatcher, primeExistingResults, stopResultWatcher };
}
