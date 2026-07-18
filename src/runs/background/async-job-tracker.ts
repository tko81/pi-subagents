import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import * as fs from "node:fs";
import * as path from "node:path";
import { renderWidget, widgetRenderKey } from "../../tui/render.ts";
import { formatControlNoticeMessage } from "../shared/subagent-control.ts";
import {
	type AsyncJobState,
	type AsyncStartedEvent,
	type ControlEvent,
	type SubagentState,
	POLL_INTERVAL_MS,
	RESULTS_DIR,
	SUBAGENT_CONTROL_EVENT,
	SUBAGENT_CONTROL_INTERCOM_EVENT,
} from "../../shared/types.ts";
import { readStatus } from "../../shared/utils.ts";
import { normalizeParallelGroups } from "./parallel-groups.ts";
import { reconcileAsyncRun, reconcileNestedAsyncDescendants } from "./stale-run-reconciler.ts";
import { hasLiveNestedDescendants, updateAsyncJobNestedProjection } from "../shared/nested-events.ts";
import { listAsyncRuns, type AsyncRunSummary } from "./async-status.ts";

interface AsyncJobTrackerOptions {
	completionRetentionMs?: number;
	pollIntervalMs?: number;
	resultsDir?: string;
	kill?: (pid: number, signal?: NodeJS.Signals | 0) => boolean;
	now?: () => number;
}

const CONTROL_EVENT_READ_CHUNK_BYTES = 64 * 1024;
const MAX_CONTROL_EVENT_LINE_BYTES = 1024 * 1024;
const CONTROL_EVENT_SCAN_WINDOW_BYTES = 2 * 1024 * 1024;

/* 
可以理解成父 Agent 里的一个后台任务状态管理器

===============================================

整体调用流程：
后台 Runner 启动
    ↓ async-started
handleStarted()
    ↓
state.asyncJobs 新增任务
    ↓
ensurePoller()
    ↓
定期读取 status.json / events.jsonl / 进程状态
    ↓
更新 AsyncJobState 和 Widget
    ↓ async-complete
handleComplete()
    ↓
显示完成状态
    ↓ 10 秒
scheduleCleanup()
    ↓
从 Widget 移除

===============================================

Session 恢复时则是：
session_start
    ↓
resetJobs()
    ↓
restoreActiveJobs()
    ↓
从磁盘重建 state.asyncJobs
    ↓
ensurePoller()

一句话概括：createAsyncJobTracker() 使用“事件快速响应 + 磁盘轮询兜底”的双通道机制，将后台 Runner 
的持久化运行状态同步为父 Agent 的内存任务投影，并负责 UI 展示、控制事件转发、Session 恢复和终态清理

===============================================

1. pi 
是一个对象，它从 ExtensionAPI 类型中挑选（Pick）出 events 这个属性，其他属性全部被丢弃。
它通过 EventBus 发出：
- Subagent 控制事件
- Intercom 通知事件

2. state
整个扩展共享的 SubagentState，主要操作：
- state.asyncJobs
- state.cleanupTimers
- state.poller
- state.lastUiContext
其中最重要的是：
- state.asyncJobs: Map<string, AsyncJobState>
结构近似：runId -> 当前后台任务的内存状态 

3. asyncDirRoot
后台任务状态目录，例如：
/tmp/pi-subagents-xxx/async-subagent-runs/
    ├── run-001/
    │   ├── status.json
    │   └── events.jsonl
    └── run-002/
*/
export function createAsyncJobTracker(pi: Pick<ExtensionAPI, "events">, state: SubagentState, asyncDirRoot: string, options: AsyncJobTrackerOptions = {}): {
	ensurePoller: () => void;
	handleStarted: (data: unknown) => void;
	handleComplete: (data: unknown) => void;
	resetJobs: (ctx?: ExtensionContext) => void;
	restoreActiveJobs: (ctx?: ExtensionContext) => void;
} {
	const completionRetentionMs = options.completionRetentionMs ?? 10000;
	const pollIntervalMs = options.pollIntervalMs ?? POLL_INTERVAL_MS;
	const resultsDir = options.resultsDir ?? RESULTS_DIR;
	/* 它完成两件事：
	把 Map 中的任务转换成数组并渲染，通知终端立即刷新
	默认渲染当前所有后台任务，也可以传空数组清除 Widget 上的任务列表
	*/
	const rerenderWidget = (ctx: ExtensionContext, jobs = Array.from(state.asyncJobs.values())) => {
		renderWidget(ctx, jobs);
		ctx.ui.requestRender?.();
	};
	// 恢复控制事件游标，用于记录已读到的位置，避免重复读取
	const restoredControlEventCursor = (asyncDir: string) => {
		try {
			return fs.statSync(path.join(asyncDir, "events.jsonl")).size;
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return 0;
			throw error;
		}
	};
	// 这段代码把磁盘里的运行摘要 AsyncRunSummary 转成 UI 使用的任务状态
	// 它主要用于 Session 恢复：
	// 磁盘中的 AsyncRunSummary
	// 		↓
	// summaryToJob()
	// 		↓
	// UI 使用的 AsyncJobState
	const summaryToJob = (run: AsyncRunSummary): AsyncJobState => {
		/* 
		假设一条 Chain：
		步骤 0：Agent A
		步骤 1、2、3：并行执行
		步骤 4：Agent E
		parallelGroups 大概是：
		[
			{ start: 1, count: 3 }
		]
		表示：
		从步骤 1 开始，共 3 个步骤并行
		也就是步骤 1、2、3 
		normalizeParallelGroups() 负责过滤越界、无效的组
		*/
		const groups = normalizeParallelGroups(run.parallelGroups, run.steps.length, run.chainStepCount ?? run.steps.length);
		
		/* 
		找到当前步骤所属的并行组
		假设：
		run.currentStep = 2;
		group = { start: 1, count: 3 };
		范围是：
		1 <= currentStep < 4
		步骤 2 在这个并行组里，所以 activeGroup 就是该组
		 */
		const activeGroup = run.currentStep !== undefined
			? groups.find((group) => run.currentStep! >= group.start && run.currentStep! < group.start + group.count)
			: undefined;
		// 决定 UI 显示哪些步骤，Chain 的某一步可能又是一个并行组。Tracker 只把当前活跃组作为主要可见step
		// 这样 Widget 不会同时把整条复杂 Chain 的所有步骤都当成正在执行
		const visibleSteps = activeGroup
			// 并行组存在时，只显示该组内的步骤
			? run.steps.slice(activeGroup.start, activeGroup.start + activeGroup.count).map((step, index) => ({ ...step, index: activeGroup.start + index }))
			// 并行组不存在时，显示所有步骤
			: run.steps.map((step, index) => ({ ...step, index }));
		/* 
		构造 UI 使用的 AsyncJobState 结构
		*/
		return {
			asyncId: run.id,
			asyncDir: run.asyncDir,
			status: run.state,
			sessionId: run.sessionId,
			activityState: run.activityState,
			lastActivityAt: run.lastActivityAt,
			currentTool: run.currentTool,
			currentToolStartedAt: run.currentToolStartedAt,
			currentPath: run.currentPath,
			turnCount: run.turnCount,
			toolCount: run.toolCount,
			mode: run.mode,
			agents: visibleSteps.map((step) => step.agent),
			currentStep: run.currentStep,
			chainStepCount: run.chainStepCount,
			parallelGroups: groups,
			steps: visibleSteps,
			stepsTotal: visibleSteps.length,
			runningSteps: visibleSteps.filter((step) => step.status === "running").length,
			completedSteps: visibleSteps.filter((step) => step.status === "complete" || step.status === "completed").length,
			hasParallelGroups: groups.length > 0,
			activeParallelGroup: Boolean(activeGroup),
			startedAt: run.startedAt,
			updatedAt: run.lastUpdate ?? run.startedAt,
			timeoutMs: run.timeoutMs,
			deadlineAt: run.deadlineAt,
			timedOut: run.timedOut,
			stopped: run.stopped,
			turnBudget: run.turnBudget,
			turnBudgetExceeded: run.turnBudgetExceeded,
			wrapUpRequested: run.wrapUpRequested,
			sessionDir: run.sessionDir,
			outputFile: run.outputFile,
			totalTokens: run.totalTokens,
			sessionFile: run.sessionFile,
			controlEventCursor: restoredControlEventCursor(run.asyncDir),
			nestedChildren: run.nestedChildren,
		};
	};

	// 延迟清理已完成任务，取消某个任务已有的清理 Timer，这是为了防止同一个任务被重复安排多个删除 Timer
	const cancelCleanup = (asyncId: string) => {
		const existingTimer = state.cleanupTimers.get(asyncId);
		if (!existingTimer) return;
		clearTimeout(existingTimer);
		state.cleanupTimers.delete(asyncId);
	};
	// 默认在完成后保留 10 秒，时间到后：为什么不完成后立即删除？因为用户需要短暂看到：
	// worker completed，而不是任务刚完成就在 Widget 中消失
	const scheduleCleanup = (asyncId: string) => {
		cancelCleanup(asyncId);
		const timer = setTimeout(() => {
			state.cleanupTimers.delete(asyncId);
			state.asyncJobs.delete(asyncId);
			if (state.lastUiContext) {
				rerenderWidget(state.lastUiContext);
			}
		}, completionRetentionMs);
		state.cleanupTimers.set(asyncId, timer);
	};

	// 增量读取控制事件，每个后台任务可以持续向下面的文件追加事件：<asyncDir>/events.jsonl
	// Tracker 不会每次从头读取，而是使用：job.controlEventCursor记录上次读到的字节位置
	// 流程是：
	// 读取文件大小
	// 	↓
	// 从 cursor 开始读取新内容
	// 	↓
	// 按换行拆分 JSONL
	// 	↓
	// 只处理 type = subagent.control
	// 	↓
	// 更新 cursor
	const emitNewControlEvents = (job: AsyncJobState) => {
		const eventsPath = path.join(job.asyncDir, "events.jsonl");
		let fd: number;
		try {
			fd = fs.openSync(eventsPath, "r");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
			console.error(`Failed to open async control events for '${job.asyncDir}':`, error);
			return;
		}
		try {
			// 同步获取文件状态信息的方法
			const stat = fs.fstatSync(fd);
			// 上次读到的字节位置
			const savedCursor = job.controlEventCursor;
			// 如果游标没保存，或文件大小比游标还小，说明文件被截断或重建，从头读取
			let cursor = stat.size < (savedCursor ?? 0) ? 0 : (savedCursor ?? 0);
			// 判断是否应该从文件尾部（tail）开始读取，如果是首次读取，而且文件已经很大了（超过扫描窗口）
			// 那就不要从头读全部内容，直接从文件尾部开始读取最近的片段
			// 原因是避免首次启动或恢复时，把大量历史控制事件重新处理一遍
			const startedFromTail = savedCursor === undefined && stat.size > CONTROL_EVENT_SCAN_WINDOW_BYTES;
			// 如果应该从尾部开始读取，那就从文件末尾往前扫描 CONTROL_EVENT_SCAN_WINDOW_BYTES 字节
			if (startedFromTail) cursor = stat.size - CONTROL_EVENT_SCAN_WINDOW_BYTES;
			// 如果文件大小小于等于 cursor，说明没有新内容可读，直接返回
			if (stat.size <= cursor) return;
			// 计算扫描结束位置，最多扫描 CONTROL_EVENT_SCAN_WINDOW_BYTES 字节
			const scanEnd = Math.min(stat.size, cursor + CONTROL_EVENT_SCAN_WINDOW_BYTES);

			// 处理每行控制事件
			const handleLine = (line: string) => {
				if (!line.trim()) return;
				let parsed: unknown;
				try {
					parsed = JSON.parse(line);
				} catch (error) {
					console.error(`Ignoring malformed async control event in '${eventsPath}':`, error);
					return;
				}
				/* 只处理subagent.control
				因为 events.jsonl 里不只有控制事件，还有很多运行日志：
				- subagent.run.started
				- subagent.step.started
				- subagent.tool.started
				- subagent.step.completed
				- subagent.run.completed
				- subagent.control
				emitNewControlEvents() 职责很窄：只把需要主 Agent关注的控制通知转发出去
				
				例如：
				- needs_attention
				- active_long_running
				- 多次 Tool 失败
				
				其他事件不需要它处理：
				普通运行状态 → Poller 读取 status.json
				最终结果 → ResultWatcher 读取 result.json
				完整历史 → events.jsonl 留作日志

				所以它过滤：type === "subagent.control"，避免把所有执行日志都变成父 Agent 通知
				*/
				if (!parsed || typeof parsed !== "object" || (parsed as { type?: unknown }).type !== "subagent.control") return;
				const record = parsed as { event?: ControlEvent; channels?: string[]; childIntercomTarget?: string; noticeText?: string; intercom?: { to?: string; message?: string } };
				if (!record.event || !Array.isArray(record.channels)) return;
				const payload = {
					event: record.event,
					source: "async" as const,
					asyncDir: job.asyncDir,
					childIntercomTarget: record.childIntercomTarget,
					noticeText: record.noticeText ?? formatControlNoticeMessage(record.event, record.childIntercomTarget),
				};
				// 两种事件通道：event 和 intercom
				// event 通道：用于发送控制事件到主进程
				// intercom 通道：用于发送通知消息到其他子进程
				// 发送普通控制通知，例如 needs_attention
				if (record.channels.includes("event")) {
					pi.events.emit(SUBAGENT_CONTROL_EVENT, payload);
				}
				// 通过 Intercom 把消息发送给 Supervisor 或父 Agent
				if (record.event.type !== "active_long_running" && record.channels.includes("intercom") && record.intercom?.to && record.intercom.message) {
					pi.events.emit(SUBAGENT_CONTROL_INTERCOM_EVENT, {
						...payload,
						to: record.intercom.to,
						message: record.intercom.message,
					});
				}
			};
			// 扫描文件，读取新内容
			let readCursor = cursor;
			// 上次完全读取到的位置
			let lastCompleteCursor = cursor;
			// 文件按 Chunk 读取。由于 JSONL 一行可能跨越两个 Chunk，所以使用 lineParts 缓存当前行的部分内容
			let lineParts: Buffer[] = [];
			// 当前行的字节数
			let lineBytes = 0;
			// 是否跳过超长行。因为从文件尾部截取时，起始位置可能落在一行 JSON 的中间，表示如果从中间开始读
			// 取，就先跳过当前残缺行，直到遇到第一个换行符，再开始解析完整 JSONL
			/* 例如原文件：
			{"type":"steer","message":"很长很长..."}\n
			{"type":"interrupt"}\n
			从尾部某个字节开始，可能读到：
			长..."}\n
			{"type":"interrupt"}\n
			第一段不是完整 JSON，不能解析。 */
			let skippingOversizedLine = startedFromTail;
			// 追加行片段
			const appendLineSegment = (segment: Buffer) => {
				if (segment.length === 0 || skippingOversizedLine) return;
				// 超大行保护：如果单行超过 MAX_CONTROL_EVENT_LINE_BYTES，Tracker 会跳过这行，防止异常事件占用过多内存
				// 如果单行异常大，通常说明：文件损坏、错误写入大段日志、恶意输入或协议使用错误。
				if (lineBytes + segment.length > MAX_CONTROL_EVENT_LINE_BYTES) {
					lineParts = [];
					lineBytes = 0;
					skippingOversizedLine = true;
					return;
				}
				lineParts.push(segment);
				lineBytes += segment.length;
			};
			// 扫描文件，读取新内容
			while (readCursor < scanEnd) {
				const toRead = Math.min(CONTROL_EVENT_READ_CHUNK_BYTES, scanEnd - readCursor);
				const buffer = Buffer.alloc(toRead);
				const bytesRead = fs.readSync(fd, buffer, 0, toRead, readCursor);
				if (bytesRead <= 0) break;
				const chunk = bytesRead === buffer.length ? buffer : buffer.subarray(0, bytesRead);
				let lineStart = 0;
				for (let index = 0; index < chunk.length; index++) {
					if (chunk[index] !== 0x0a) continue;
					appendLineSegment(chunk.subarray(lineStart, index));
					if (!skippingOversizedLine && lineBytes > 0) {
						handleLine(Buffer.concat(lineParts, lineBytes).toString("utf-8"));
					}
					lineParts = [];
					lineBytes = 0;
					skippingOversizedLine = false;
					lastCompleteCursor = readCursor + index + 1;
					lineStart = index + 1;
				}
				appendLineSegment(chunk.subarray(lineStart));
				readCursor += bytesRead;
				if (skippingOversizedLine) job.controlEventCursor = readCursor;
			}
			if (lastCompleteCursor > cursor) job.controlEventCursor = lastCompleteCursor;
			else if (scanEnd < stat.size || startedFromTail) job.controlEventCursor = scanEnd;
		} catch (error) {
			console.error(`Failed to read async control events for '${job.asyncDir}':`, error);
		} finally {
			fs.closeSync(fd);
		}
	};

	/* ensurePoller() 的功能：
	定时遍历后台任务
	1. 保存更新前的 Widget 状态
	widgetStateBefore

	2. 读取 events.jsonl 的新增控制事件
	emitNewControlEvents(job)

	3. 对账并更新嵌套子任务
	reconcileNestedDescendants()

	4. 对账当前任务真实状态
	reconcileAsyncRun()

	5. 优先取对账结果，否则读 status.json
	reconciliation.status ?? readStatus()

	6. 把最新状态写入内存 job

	7. 如果任务进入终态，安排延迟清理
	scheduleCleanup()

	8. 比较 Widget 状态是否变化
	widgetRenderKey(job) !== widgetStateBefore */
	const ensurePoller = () => {
		// 如果 Poller 已存在，就不会重复创建。因此它是一个单例轮询器，而不是每个任务创建一个 Timer
		if (state.poller) return;
		state.poller = setInterval(() => {
			// 没有任务时自动停止
			if (state.asyncJobs.size === 0) {
				/* 没有后台任务后：清空 Widget、停止轮询、释放 Timer。
				state.lastUiContext 保存的是主 Agent 的 UI Context，所以可以调用：rerenderWidget 清空 Widget
				子 Agent 进程
				→ 写 status.json / result.json
				
				主 Agent 进程
				→ AsyncJobTracker 轮询文件
				→ 更新 state.asyncJobs
				→ 更新终端 UI */
				if (state.lastUiContext?.hasUI) rerenderWidget(state.lastUiContext, []);
				if (state.poller) {
					clearInterval(state.poller);
					state.poller = null;
				}
				return;
			}

			let widgetChanged = false;
			// 轮询每一个 Job，对每个任务依次完成：
			// 读取新增控制事件 → 同步嵌套子任务 → 检查进程和磁盘状态 → 更新内存 Job → 决定是否安排清理 → 必要时更新 Widget
			for (const job of state.asyncJobs.values()) {
				// 记录 Widget 更新前的状态，用于后续比较
				const widgetStateBefore = widgetRenderKey(job);
				// 处理嵌套子任务时，如果失败，会设置这个标志
				let nestedRefreshFailed = false;
				// 处理嵌套子任务
				const refreshNestedProjection = () => {
					try {
						// 更新内存 Job 的嵌套子任务状态
						updateAsyncJobNestedProjection(job);
					} catch (error) {
						nestedRefreshFailed = true;
						console.error(`Failed to refresh nested async descendants for '${job.asyncDir}':`, error);
					}
				};
				// 处理嵌套子任务
				const reconcileNestedDescendants = () => {
					try {
						if (job.nestedRoute) reconcileNestedAsyncDescendants(job.nestedRoute, { resultsDir, kill: options.kill, now: options.now });
					} catch (error) {
						nestedRefreshFailed = true;
						console.error(`Failed to refresh nested async descendants for '${job.asyncDir}':`, error);
					}
					// 更新内存 Job 的嵌套子任务状态
					refreshNestedProjection();
				};
				try {
					// 增量读取刚才讲过的：<asyncDir>/events.jsonl
					emitNewControlEvents(job);
					// 处理嵌套子任务
					reconcileNestedDescendants();
					// 调和真实运行状态
					// “Reconcile”可以理解为对账：
					// status.json 说 running
					// 但可能：
					// - 进程实际已经不存在
					// - 结果文件已经生成
					// - 超时时间已经到达
					// Tracker 需要综合这些信息，推导出真实状态，而不能只相信某一个字段。
					const reconciliation = reconcileAsyncRun(job.asyncDir, {
						resultsDir,
						kill: options.kill,
						now: options.now,
						startedRun: {
							runId: job.asyncId,
							pid: job.pid,
							sessionId: job.sessionId,
							mode: job.mode,
							agents: job.agents,
							chainStepCount: job.chainStepCount,
							parallelGroups: job.parallelGroups,
							startedAt: job.startedAt,
							sessionFile: job.sessionFile,
						},
					});
					// 优先使用调和结果，没有时直接读取状态文件
					const status = reconciliation.status ?? readStatus(job.asyncDir);
					// 更新内存投影，拿到新状态后，把它同步到 job
					if (status) {
						const previousStatus = job.status;
						job.status = status.state;
						if (job.status !== "complete" && job.status !== "failed" && job.status !== "paused" && job.status !== "stopped") cancelCleanup(job.asyncId);
						job.sessionId = status.sessionId ?? job.sessionId;
						job.activityState = status.activityState;
						job.lastActivityAt = status.lastActivityAt ?? job.lastActivityAt;
						job.currentTool = status.currentTool;
						job.currentToolStartedAt = status.currentToolStartedAt;
						job.currentPath = status.currentPath;
						job.turnCount = status.turnCount ?? job.turnCount;
						job.toolCount = status.toolCount ?? job.toolCount;
						job.mode = status.mode;
						job.currentStep = status.currentStep ?? job.currentStep;
						job.chainStepCount = status.chainStepCount ?? job.chainStepCount;
						job.startedAt = status.startedAt ?? job.startedAt;
						if (status.lastUpdate !== undefined) job.updatedAt = status.lastUpdate;
						if (status.steps?.length) {
							const groups = normalizeParallelGroups(status.parallelGroups, status.steps.length, status.chainStepCount ?? status.steps.length);
							job.parallelGroups = groups.length ? groups : job.parallelGroups;
							job.hasParallelGroups = groups.length > 0 || job.hasParallelGroups;
							const activeGroup = status.currentStep !== undefined
								? groups.find((group) => status.currentStep! >= group.start && status.currentStep! < group.start + group.count)
								: undefined;
							const visibleSteps = activeGroup
								? status.steps.slice(activeGroup.start, activeGroup.start + activeGroup.count).map((step, index) => ({ ...step, index: activeGroup.start + index }))
								: status.steps.map((step, index) => ({ ...step, index }));
							job.activeParallelGroup = Boolean(activeGroup);
							job.agents = visibleSteps.map((step) => step.agent);
							job.steps = visibleSteps;
							refreshNestedProjection();
							job.stepsTotal = visibleSteps.length;
							job.runningSteps = visibleSteps.filter((step) => step.status === "running").length;
							job.completedSteps = visibleSteps.filter((step) => step.status === "complete" || step.status === "completed").length;
							if (status.state === "complete") job.completedSteps = visibleSteps.length;
						}
						job.sessionDir = status.sessionDir ?? job.sessionDir;
						job.outputFile = status.outputFile ?? job.outputFile;
						job.totalTokens = status.totalTokens ?? job.totalTokens;
						job.timeoutMs = status.timeoutMs ?? job.timeoutMs;
						job.deadlineAt = status.deadlineAt ?? job.deadlineAt;
						job.timedOut = status.timedOut ?? job.timedOut;
						job.stopped = status.stopped ?? job.stopped;
						job.turnBudget = status.turnBudget ?? job.turnBudget;
						job.turnBudgetExceeded = status.turnBudgetExceeded ?? job.turnBudgetExceeded;
						job.wrapUpRequested = status.wrapUpRequested ?? job.wrapUpRequested;
						job.sessionFile = status.sessionFile ?? job.sessionFile;
						// 终态清理：如果状态变成 complete、failed、paused、stopped，并且没有仍在运行的嵌套子任务，就调用清理逻辑。
						if ((job.status === "complete" || job.status === "failed" || job.status === "paused" || job.status === "stopped") && !nestedRefreshFailed && !hasLiveNestedDescendants(job.nestedChildren) && (previousStatus !== job.status || !state.cleanupTimers.has(job.asyncId))) {
							scheduleCleanup(job.asyncId);
						}
						// 只在 UI 内容变化时刷新，不是每次轮询都重绘，而是先比较影响 Widget 的关键字段。这样可以减少终端闪烁和无意义渲染。
						if (widgetRenderKey(job) !== widgetStateBefore) widgetChanged = true;
						continue;
					}
					if (job.status === "queued") {
						job.status = "running";
						job.updatedAt = Date.now();
					}
				} catch (error) {
					if (job.status !== "failed") {
						console.error(`Failed to read async status for '${job.asyncDir}':`, error);
						job.status = "failed";
						job.updatedAt = Date.now();
					}
					if (!hasLiveNestedDescendants(job.nestedChildren) && !state.cleanupTimers.has(job.asyncId)) {
						scheduleCleanup(job.asyncId);
					}
				}
				if (widgetRenderKey(job) !== widgetStateBefore) widgetChanged = true;
			}
			// 如果 Widget 内容变化，就更新 UI
			if (widgetChanged && state.lastUiContext?.hasUI) rerenderWidget(state.lastUiContext);
		}, pollIntervalMs);
		// 表示 Poller 本身不会强行阻止 Node.js 进程退出。如果不调用 unref()，主进程退出时会等待 Poller 停止，导致进程无法正常退出。
		state.poller.unref?.();
	};
	
	/* 快速响应开始事件
	后台 Runner 启动后会发送 async-started 事件。
	Tracker 收到后：
	检查 id。
	检查事件是否属于当前 Session。
	构造 AsyncJobState。
	写入 state.asyncJobs。
	启动 Poller。
	立即更新 Widget。 */
	const handleStarted = (data: unknown) => {
		const info = data as AsyncStartedEvent;
		if (!info.id) return;
		if (typeof state.currentSessionId === "string" && info.sessionId !== state.currentSessionId) return;
		const now = Date.now();
		const asyncDir = info.asyncDir ?? path.join(asyncDirRoot, info.id);
		const rawAgents = info.agents?.length ? info.agents : info.chain && info.chain.length > 0 ? info.chain : info.agent ? [info.agent] : undefined;
		const validParallelGroups = normalizeParallelGroups(info.parallelGroups, Number.MAX_SAFE_INTEGER, info.chainStepCount ?? Number.MAX_SAFE_INTEGER);
		const firstGroup = validParallelGroups.find((group) => group.start === 0);
		const firstGroupCount = firstGroup?.count;
		const agents = firstGroupCount && firstGroupCount > 0
			? rawAgents?.slice(0, firstGroupCount)
			: rawAgents;
		/* 这里先写成 queued，Poller 读取到实际运行状态后再更新成 running。
		为什么既有 handleStarted() 又有 Poller？
		handleStarted()：低延迟，任务一启动 UI 立即显示。
		Poller：可靠性兜底，事件丢失后仍可恢复。 */
		state.asyncJobs.set(info.id, {
			asyncId: info.id,
			asyncDir,
			status: "queued",
			pid: typeof info.pid === "number" ? info.pid : undefined,
			...(typeof info.sessionId === "string" ? { sessionId: info.sessionId } : {}),
			mode: info.mode ?? (info.chain ? "chain" : "single"),
			agents,
			chainStepCount: info.chainStepCount,
			parallelGroups: validParallelGroups,
			nestedRoute: info.nestedRoute,
			stepsTotal: firstGroupCount ?? agents?.length,
			hasParallelGroups: validParallelGroups.length > 0,
			activeParallelGroup: Boolean(firstGroupCount && firstGroupCount > 0),
			startedAt: now,
			updatedAt: now,
			timeoutMs: info.timeoutMs,
			deadlineAt: info.deadlineAt,
			turnBudget: info.turnBudget,
			controlEventCursor: 0,
		});
		ensurePoller();
		if (state.lastUiContext) {
			rerenderWidget(state.lastUiContext);
		}
	};

	/* 快速响应完成事件，收到完成事件后：
	更新 stopped
	更新时间
	更新运行目录
	刷新嵌套状态
	重绘 Widget
	安排延迟清理
	同样，它是“快速通道”，Poller 仍是兜底通道。 */
	const handleComplete = (data: unknown) => {
		const result = data as { id?: string; success?: boolean; state?: AsyncJobState["status"]; asyncDir?: string; sessionId?: string; stopped?: boolean };
		if (typeof state.currentSessionId === "string" && result.sessionId !== state.currentSessionId) return;
		const asyncId = result.id;
		if (!asyncId) return;
		const job = state.asyncJobs.get(asyncId);
		let nestedRefreshFailed = false;
		if (job) {
			job.status = result.state ?? (result.success ? "complete" : "failed");
			job.stopped = result.stopped ?? job.stopped;
			job.updatedAt = Date.now();
			if (result.asyncDir) job.asyncDir = result.asyncDir;
			try {
				updateAsyncJobNestedProjection(job);
			} catch (error) {
				nestedRefreshFailed = true;
				console.error(`Failed to refresh nested async descendants for '${job.asyncDir}':`, error);
			}
		}
		if (state.lastUiContext) {
			rerenderWidget(state.lastUiContext);
		}
		if (!nestedRefreshFailed && !hasLiveNestedDescendants(job?.nestedChildren)) scheduleCleanup(asyncId);
	};

	/* 切换 Session 前清空内存
	它会清理：
	所有延迟清理 Timer
	asyncJobs
	前台控制状态
	最近的前台任务 ID
	尚未处理的结果文件
	UI Widget
	注意：它清理的是父 Agent 的跟踪状态，通常不会杀死真实后台子进程。 */
	const resetJobs = (ctx?: ExtensionContext) => {
		for (const timer of state.cleanupTimers.values()) {
			clearTimeout(timer);
		}
		state.cleanupTimers.clear();
		state.asyncJobs.clear();
		state.foregroundControls?.clear();
		state.lastForegroundControlId = null;
		state.resultFileCoalescer.clear();
		if (ctx?.hasUI) {
			state.lastUiContext = ctx;
			rerenderWidget(ctx, []);
		}
	};

	/* 恢复当前 Session 的任务，它扫描磁盘：
	只恢复：
	当前 Session
	状态为 queued/running
	如果恢复到任务，就重新启动 Poller 并显示 Widget
	这使得：
	Pi 重启
	或重新打开历史 Session
			↓
	读取后台运行目录
			↓
	恢复 state.asyncJobs
			↓
	继续跟踪原来的后台任务 */
	const restoreActiveJobs = (ctx?: ExtensionContext) => {
		if (ctx?.hasUI) state.lastUiContext = ctx;
		if (!state.currentSessionId) return;
		let runs: AsyncRunSummary[];
		try {
			runs = listAsyncRuns(asyncDirRoot, { states: ["queued", "running"], sessionId: state.currentSessionId, resultsDir, kill: options.kill, now: options.now });
		} catch (error) {
			console.error(`Failed to restore active async jobs from '${asyncDirRoot}':`, error);
			return;
		}
		for (const run of runs) {
			state.asyncJobs.set(run.id, summaryToJob(run));
		}
		if (runs.length === 0) return;
		ensurePoller();
		if (state.lastUiContext?.hasUI) rerenderWidget(state.lastUiContext);
	};

	return { ensurePoller, handleStarted, handleComplete, resetJobs, restoreActiveJobs };
}
