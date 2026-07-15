/**
 * Subagent Tool
 *
 * Full-featured subagent with sync and async modes.
 * - Sync (default): Streams output, renders markdown, tracks usage
 * - Async: Background execution, emits events when done
 *
 * Modes: single (agent + task), parallel (tasks[]), chain (chain[] with {previous})
 * Toggle: async parameter (default: false, configurable via config.json)
 *
 * Config file: ~/.pi/agent/extensions/subagent/config.json
 *   { "asyncByDefault": true, "forceTopLevelAsync": true, "maxSubagentDepth": 1, "intercomBridge": { "mode": "always", "instructionFile": "./intercom-bridge.md" }, "worktreeSetupHook": "./scripts/setup-worktree.mjs" }
 */

import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import {
	type ExtensionAPI,
	type ExtensionContext,
	keyText,
	type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import {
	Box,
	type Component,
	Container,
	Spacer,
	Text,
	truncateToWidth,
	visibleWidth,
	wrapTextWithAnsi,
} from "@earendil-works/pi-tui";
import { discoverAgents } from "../agents/agents.ts";
import { createNativeSupervisorChannel } from "../intercom/native-supervisor-channel.ts";
import { createAsyncJobTracker } from "../runs/background/async-job-tracker.ts";
import registerSubagentNotify, { type SubagentNotifyDetails } from "../runs/background/notify.ts";
import { createResultWatcher } from "../runs/background/result-watcher.ts";
import { inspectSubagentStatus } from "../runs/background/run-status.ts";
import { createScheduledRunManager } from "../runs/background/scheduled-runs.ts";
import { resolveWaitToolConfig, waitForSubagents } from "../runs/background/wait.ts";
import { createSubagentExecutor, type SubagentParamsLike } from "../runs/foreground/subagent-executor.ts";
import { SUBAGENT_CHILD_ENV, SUBAGENT_PARENT_SESSION_ENV } from "../runs/shared/pi-args.ts";
import { cleanupAllArtifactDirs, cleanupOldArtifacts, getArtifactsDir } from "../shared/artifacts.ts";
import { formatDuration, shortenPath } from "../shared/formatters.ts";
import { resolveCurrentSessionId } from "../shared/session-identity.ts";
import { cleanupOldChainDirs } from "../shared/settings.ts";
import {
	ASYNC_DIR,
	DEFAULT_ARTIFACT_CONFIG,
	type Details,
	RESULTS_DIR,
	SLASH_RESULT_TYPE,
	SLASH_TEXT_RESULT_TYPE,
	SUBAGENT_ASYNC_COMPLETE_EVENT,
	SUBAGENT_ASYNC_STARTED_EVENT,
	SUBAGENT_CONTROL_EVENT,
	type SubagentState,
	WIDGET_KEY,
} from "../shared/types.ts";
import { registerPromptTemplateDelegationBridge } from "../slash/prompt-template-bridge.ts";
import { registerSlashSubagentBridge } from "../slash/slash-bridge.ts";
import { registerSlashCommands } from "../slash/slash-commands.ts";
import {
	clearSlashSnapshots,
	getSlashRenderableSnapshot,
	resolveSlashMessageDetails,
	restoreSlashFinalSnapshots,
	type SlashMessageDetails,
} from "../slash/slash-live-state.ts";
import { clearLegacyResultAnimationTimer, renderSubagentResult, renderWidget } from "../tui/render.ts";
import { registerMainWatchdog } from "../watchdog/register-main.ts";
import { loadConfig } from "./config.ts";
import {
	clearPendingForegroundControlNotices,
	formatSubagentControlNotice,
	handleSubagentControlNotice,
	SUBAGENT_CONTROL_MESSAGE_TYPE,
	type SubagentControlMessageDetails,
} from "./control-notices.ts";
import { registerSubagentRpcBridge } from "./rpc.ts";
import { SubagentParams, WaitParams } from "./schemas.ts";
import { buildSubagentToolDescription } from "./tool-description.ts";

export { loadConfig } from "./config.ts";

/**
 * Derive subagent session base directory from parent session file.
 * If parent session is ~/.pi/agent/sessions/abc123.jsonl,
 * returns ~/.pi/agent/sessions/abc123/ as the base.
 * Callers add runId to create the actual session root: abc123/{runId}/
 * Falls back to a unique temp directory if no parent session.
 */
function getSubagentSessionRoot(parentSessionFile: string | null): string {
	if (parentSessionFile) {
		const baseName = path.basename(parentSessionFile, ".jsonl");
		const sessionsDir = path.dirname(parentSessionFile);
		return path.join(sessionsDir, baseName);
	}
	return fs.mkdtempSync(path.join(os.tmpdir(), "pi-subagent-session-"));
}

function expandTilde(p: string): string {
	return p.startsWith("~/") ? path.join(os.homedir(), p.slice(2)) : p;
}

/**
 * Create a directory and verify it is actually accessible.
 * On Windows with Azure AD/Entra ID, directories created shortly after
 * wake-from-sleep can end up with broken NTFS ACLs (null DACL) when the
 * cloud SID cannot be resolved without network connectivity. This leaves
 * the directory completely inaccessible to the creating user.
 */
function ensureAccessibleDir(dirPath: string): void {
	fs.mkdirSync(dirPath, { recursive: true });
	try {
		fs.accessSync(dirPath, fs.constants.R_OK | fs.constants.W_OK);
	} catch {
		try {
			fs.rmSync(dirPath, { recursive: true, force: true });
		} catch {
			// Best effort: retry mkdir/access even if cleanup fails.
		}
		fs.mkdirSync(dirPath, { recursive: true });
		fs.accessSync(dirPath, fs.constants.R_OK | fs.constants.W_OK);
	}
}

function isSlashResultRunning(result: { details?: Details }): boolean {
	return (
		result.details?.progress?.some((entry) => entry.status === "running") ||
		result.details?.results.some((entry) => entry.progress?.status === "running") ||
		false
	);
}

// Drives the inline running-indicator braille animation for foreground subagent
// results. Foreground runs receive progress only on child events, so the glyph
// (derived from progress fields) would freeze between events. While a result is
// running we tick a frame counter + invalidate() every 80ms so renderSubagentResult
// can blend the frame into runningGlyph and produce a smooth spinner.
function subagentResultIsRunning(result: { details?: Details }): boolean {
	return (
		result.details?.progress?.some((entry) => entry.status === "running") ||
		result.details?.results.some((entry) => entry.progress?.status === "running") ||
		false
	);
}

function ensureSubagentResultAnimation(context: { state: Record<string, unknown>; invalidate?: () => void }): void {
	const state = context.state as { subagentResultAnimationTimer?: ReturnType<typeof setInterval>; frame?: number };
	if (state.subagentResultAnimationTimer) return;
	if (typeof context.invalidate !== "function") return;
	if (state.frame === undefined) state.frame = 0;
	state.subagentResultAnimationTimer = setInterval(() => {
		state.frame = ((state.frame ?? 0) + 1) % 10;
		try {
			context.invalidate();
		} catch {}
	}, 80);
}

function isSlashResultError(result: { details?: Details }): boolean {
	return (
		result.details?.results.some((entry) => entry.exitCode !== 0 && entry.progress?.status !== "running") || false
	);
}

function isStaleExtensionContextError(error: unknown): boolean {
	return error instanceof Error && error.message.includes("Extension context no longer active");
}

function rebuildSlashResultContainer(
	container: Container,
	result: AgentToolResult<Details>,
	options: { expanded: boolean },
	theme: ExtensionContext["ui"]["theme"],
): void {
	container.clear();
	container.addChild(new Spacer(1));
	const boxTheme = isSlashResultRunning(result)
		? "toolPendingBg"
		: isSlashResultError(result)
			? "toolErrorBg"
			: "toolSuccessBg";
	const box = new Box(1, 1, (text: string) => theme.bg(boxTheme, text));
	box.addChild(renderSubagentResult(result, options, theme));
	container.addChild(box);
}

function createSlashResultComponent(
	details: SlashMessageDetails,
	options: { expanded: boolean },
	theme: ExtensionContext["ui"]["theme"],
): Container {
	const container = new Container();
	let lastVersion = -1;
	container.render = (width: number): string[] => {
		const snapshot = getSlashRenderableSnapshot(details);
		if (snapshot.version !== lastVersion || isSlashResultRunning(snapshot.result)) {
			lastVersion = snapshot.version;
			rebuildSlashResultContainer(container, snapshot.result, options, theme);
		}
		return Container.prototype.render.call(container, width);
	};
	return container;
}

function parseSubagentNotifyContent(content: string): SubagentNotifyDetails | undefined {
	const lines = content.split("\n");
	const header = lines[0] ?? "";
	const match = header.match(/^Background task (completed|failed|paused): \*\*(.+?)\*\*(?:\s+(\([^)]*\)))?$/);
	if (!match) return undefined;
	const body = lines.slice(2);
	let sessionIndex = -1;
	for (let i = body.length - 1; i >= 1; i--) {
		if (body[i - 1]?.trim() === "" && /^(Session|Session file|Session share error):\s+/.test(body[i]!)) {
			sessionIndex = i;
			break;
		}
	}
	const sessionLine = sessionIndex >= 0 ? body[sessionIndex] : undefined;
	const resultLines = sessionIndex >= 0 ? body.slice(0, sessionIndex) : body;
	const resultPreview = resultLines.join("\n").trim() || "(no output)";
	let sessionLabel: string | undefined;
	let sessionValue: string | undefined;
	if (sessionLine) {
		const separator = sessionLine.indexOf(":");
		sessionLabel = sessionLine.slice(0, separator).toLowerCase();
		sessionValue = sessionLine.slice(separator + 1).trim();
	}
	return {
		agent: match[2]!,
		status: match[1] as SubagentNotifyDetails["status"],
		...(match[3] ? { taskInfo: match[3] } : {}),
		resultPreview,
		...(sessionLabel && sessionValue ? { sessionLabel, sessionValue } : {}),
	};
}

class SubagentControlNoticeComponent implements Component {
	constructor(
		private readonly details: SubagentControlMessageDetails,
		private readonly theme: ExtensionContext["ui"]["theme"],
	) {}

	invalidate(): void {}

	render(width: number): string[] {
		const eventLabel = this.details.event.type.replaceAll("_", " ");
		if (width < 3) return [truncateToWidth(`Subagent ${eventLabel}`, width)];
		const bodyWidth = Math.max(1, width - 2);
		const borderChar = "─";
		const header = ` ⚠ Subagent ${eventLabel}: ${this.details.event.agent} `;
		const headerText = truncateToWidth(header, bodyWidth, "");
		const headerPadding = Math.max(0, bodyWidth - visibleWidth(headerText));
		const lines = [this.theme.fg("accent", `╭${headerText}${borderChar.repeat(headerPadding)}╮`)];

		for (const line of wrapTextWithAnsi(formatSubagentControlNotice(this.details), bodyWidth)) {
			const text = truncateToWidth(line, bodyWidth, "");
			const padding = Math.max(0, bodyWidth - visibleWidth(text));
			lines.push(this.theme.fg("accent", `│${text}${" ".repeat(padding)}│`));
		}
		lines.push(this.theme.fg("accent", `╰${borderChar.repeat(bodyWidth)}╯`));
		return lines;
	}
}

/* 
registerSubagentExtension 是扩展注册入口；它执行后创建的共享状态、执行器、Tool、事件监听
器、watcher 等运行对象，可以在概念上称为一个扩展实例，“扩展实例”不是这里某个具体类的实例，而
是一种概念性说法。
扩展模块只是被定义或加载，还没有接入 Pi 的 Tool、Command 和事件系统，因此无法作为 Pi 扩展
提供能力。

registerSubagentExtension()
    ├── 创建 state
    ├── 创建 executor
    ├── 注册 Tool
    ├── 注册事件监听器
    └── 返回 undefined

扩展实例（概念）
    ├── SubagentState
    ├── config
    ├── executor
    ├── watcher
    ├── poller
    ├── Tool Definition
    ├── Event Handler
    └── cleanup

/reload 的真实过程是：
	向旧扩展发送 session_shutdown
		↓
	旧扩展停止 watcher、poller、timer、事件订阅
		↓
	清除扩展模块缓存
		↓
	重新读取最新配置和资源
		↓
	重新执行 registerSubagentExtension(pi)
		↓
	创建新的 state、executor 和监听器
		↓
	旧对象失去引用后，由 JavaScript垃圾回收器回收
*/
export default function registerSubagentExtension(pi: ExtensionAPI): void {
	/* 
	子 Agent 进程不会再次注册 subagent 工具，这是控制子 Agent 递归派生的第一层保护
	SUBAGENT_CHILD_ENV 是一个环境变量名。父 Agent 使用 spawn() 创建子 Agent 时
	会给子进程设置：PI_SUBAGENT_CHILD=1
	具体变量名由 SUBAGENT_CHILD_ENV 常量封装

	子进程加载扩展时也会执行 registerSubagentExtension()，所以这里必须判断：
	- 当前是主 Agent
   		-> 继续注册 subagent 工具
	- 当前是子 Agent
		-> 立即 return
		-> 不注册 subagent 工具

	因此，默认情况下子 Agent 看不到 subagent Tool，也就不能继续派生下一层子 Agent。
	这里的 return 跳过的是整个扩展注册流程，不只是跳过某个 Tool。 
	*/
	if (process.env[SUBAGENT_CHILD_ENV] === "1") {
		return;
	}
	/* 
	清理上一次扩展运行时 
	globalThis 是当前 Node.js 进程的全局对象。
	它的生命周期比当前扩展实例更长：
	Pi Node.js 进程
		└── globalThis
			  ├── 第一次加载的扩展
			  ├── /reload 后的新扩展
			  └── 其他运行时全局数据
	as Record<string, unknown> 是 TypeScript 类型断言，表示把它当作一个可以使用字符串索引的对象：
	globalStore["任意字符串"] 
	*/
	const globalStore = globalThis as Record<string, unknown>;
	// 定义一个全局存储键
	const runtimeCleanupStoreKey = "__piSubagentRuntimeCleanup";
	/* 	
	读取上一次扩展加载时留下的清理函数，因为 Pi 支持重新加载扩展。重新加载 TypeScript 模块不等于销毁
	整个 Node.js 进程，旧扩展创建的资源可能还活着。

	操作	       Pi进程  扩展代码	   当前会话
	/reload	   通常不退出  重新加载	   通常保留
	退出再启动	创建新进程	重新加载  根据启动方式决定是否恢复

	如果不清理，就可能出现：
	第一次加载：创建 watcher A
	/reload
	第二次加载：创建 watcher B

	一个结果文件变化
		-> watcher A 处理一次
		-> watcher B 又处理一次
	*/
	const previousRuntimeCleanup = globalStore[runtimeCleanupStoreKey];
	if (typeof previousRuntimeCleanup === "function") {
		try {
			previousRuntimeCleanup();
		} catch {
			// 尽力而为：清理失败不应该阻止新扩展加载，所以捕获错误后继续初始化
		}
	}

	// 第三块：准备目录、读取配置和创建状态
	// 这两个目录位于系统临时目录，分别用于：
	// RESULTS_DIR：保存后台子 Agent 的最终结果和完成通知。
	// ASYNC_DIR：保存后台运行的状态、事件和元数据。
	ensureAccessibleDir(RESULTS_DIR);
	ensureAccessibleDir(ASYNC_DIR);
	cleanupOldChainDirs();

	// config 是运行策略，从下面的位置读取配置，~/.pi/agent/extensions/subagent/config.json
	// 配置文件不存在 -> 返回 {}
	// 配置正确       -> 返回解析后的对象
	// 配置错误       -> 打印错误，返回 {}
	// 所以配置损坏不会阻止 Pi 启动，而是退回默认行为。
	const config = loadConfig();
	/* 
	把“环境变量配置”和“配置文件配置”合并，最终得到一个确定的 { enabled: boolean }。
	后面会注册一个 wait Tool，用于等待后台子 Agent：
	启动多个 async 子 Agent
		-> 主 Agent继续做其他工作
		-> 调用 wait
		-> 等待一个或全部后台任务完成

	用户提供的配置可能缺少某些字段，resolveWaitToolConfig() 会结合环境变量和默认值，最终生成一份
	字段明确、可以直接使用的配置，不过对当前 waitTool 来说，只有一个配置字段：
	interface ResolvedWaitToolConfig {
		enabled: boolean;
	}
	*/
	const waitToolConfig = resolveWaitToolConfig(config.waitTool);

	// 判断子 Agent 是否默认后台运行，只有用户明确配置 true 才启用，未配置默认为 false，同步执行
	const asyncByDefault = config.asyncByDefault === true;

	/* 
	获取临时 artifact 目录。
	这里传入 null，因为扩展刚加载时还没有收到 session_start，暂时不知道当前 Session 文件路径，所以先使用公共临时 artifact 目录。
	Artifact 主要保存：
	过长的子 Agent 输出
	transcript
	中间结果
	被截断内容的完整版本 
	*/
	const tempArtifactsDir = getArtifactsDir(null);

	// 根据默认保留天数清理过期 Artifact，防止临时文件无限增长
	cleanupAllArtifactDirs(DEFAULT_ARTIFACT_CONFIG.cleanupDays);

	// 这是当前扩展实例的共享运行状态，后面的 executor、后台 tracker、watcher、UI renderer
	// 和生命周期监听器都会持有同一个 state 引用
	const state: SubagentState = {
		// 主 Agent 当前工作目录。初始化时还不知道，等 session_start 后设置为：state.baseCwd = ctx.cwd
		baseCwd: "",
		// 当前父 Agent 的 Session ID，同样在 session_start 时填充
		currentSessionId: null,
		// 它用于把子 Agent 任务与当前父会话绑定。
		subagentInProgress: false,
		/* 
		表示当前是否正在执行子 Agent，主要用于防止冲突、辅助状态判断和生命周期控制
		记录当前 Session 已经派生了多少次子 Agent：
		sessionId：计数属于哪个会话。
		count：当前会话的 spawn 数量。
		切换 Session 后会重置，避免不同会话共用计数
		 */
		subagentSpawns: { sessionId: null, count: 0 },
		// 保存后台任务：runId -> AsyncJobState，后台任务是子 Agent 的运行状态，每个子 Agent 都有一个唯一的 runId
		// 用于后台任务列表、wait Tool、状态恢复和完成通知
		asyncJobs: new Map(),
		// 保存可恢复的前台运行：runId -> ForegroundResumeRun 其中会记录模式、工作目录、子任务状态、Session 文件和输出等
		foregroundRuns: new Map(),
		/* 
		保存正在运行的前台任务控制信息，例如：
		- 当前执行哪个 Agent
		- 当前任务索引
		- 当前 Tool
		- turn 数量
		- token 数量
		- interrupt 函数
		它主要供 watchdog、状态查询、暂停和中断使用。
		*/
		foregroundControls: new Map(),
		// 记录最近一次前台任务的控制 ID，便于用户没有明确指定 ID 时操作“最近的任务”
		lastForegroundControlId: null,
		// 保存尚未发送的前台控制通知及其定时器，用于合并或延迟“运行过久”“需要注意”等通知
		pendingForegroundControlNotices: new Map(),
		// 保存延迟清理任务的 timer，确保 Session 关闭时能够统一取消
		cleanupTimers: new Map(),
		// 保存最近一次可用的 ExtensionContext。后台 watcher 收到文件变化时，没有当前
		// Tool 调用传入的 ctx，所以需要通过这里找到 UI，更新 widget 或发送通知
		lastUiContext: null,
		// 后台任务轮询器。没有后台任务时为 null，有任务时可能通过 setInterval 定期检查状态
		poller: null,
		// 记录已经处理过的完成事件：runId -> 完成时间
		// 用于去重，避免文件 watcher、轮询器和 EventBus 同时发现完成后重复通知
		completionSeen: new Map(),
		// 分别保存：
		// 结果目录的文件 watcher
		// watcher 异常退出后的重启 timer
		watcher: null,
		watcherRestartTimer: null,
		// 这是一个初始的空实现。真实的结果文件合并器稍后才会安装。先提供空对象，是为了让其他
		// 代码任何时候都能安全调用
		resultFileCoalescer: {
			schedule: () => false,
			clear: () => {},
		},
	};

	/* 创建 Supervisor Channel：supervisorChannel 是父 Agent和子 Agent控制信息之间的通信通道
	它需要：
	- pi，用于接入 Pi 的事件系统和消息能力。
	- state，用于读取或更新当前子 Agent运行状态。
	它主要处理的不是普通任务结果，而是控制信息，例如：
	- 子 Agent运行过久
	- 子 Agent需要父 Agent关注
	- 子 Agent阻塞等待决定
	- 子 Agent请求控制或上报状态
	这里只是创建通道对象，还没有启动：supervisorChannel.start();
	真正启动是在后面的 session_start 中。 */
	const supervisorChannel = createNativeSupervisorChannel(pi, state);

	/* 创建主 Watchdog
	Watchdog 用来监督子 Agent运行过程，例如：
	- 子 Agent长时间没有进展
	- 连续工具调用失败
	- Token 或 Turn 达到阈值
	- 任务可能偏离目标
	- 需要生成警告或控制通知

	registerMainWatchdog(pi) 会：
	1. 创建 MainWatchdogRuntime。
	2. 注册 Watchdog 相关命令或 Tool Action。
	3. 返回可供 executor 调用的运行时对象。
	后面创建 executor 时会传进去：watchdog: mainWatchdog
	这样 executor 在执行子 Agent期间就能注册、更新或移除监控对象。
	这一步仍然没有创建子 Agent。 */
	const mainWatchdog = registerMainWatchdog(pi);

	// 创建结果文件 Watcher，返回三个函数
	const { startResultWatcher, primeExistingResults, stopResultWatcher } = createResultWatcher(
		pi, // Watcher 发现后台任务完成后，需要通过 Pi 发送完成通知、触发 EventBus 事件并更新消息或 UI。
		state, // 例如发现结果文件后，要把对应任务状态改成完成
		/* 后台子 Agent完成后，会把结果写入结果目录。Watcher 监听的就是这个目录。
		大致关系：
		后台子 Agent
			↓
		写入 RESULTS_DIR/<runId>.json
			↓
		ResultWatcher发现文件变化
			↓
		读取结果
			↓
		通知父 Agent */
		RESULTS_DIR,
		10 * 60 * 1000, // 这是结果文件相关的保留或去重时间参数，避免同一结果长期被重复处理
	);
	// 从这里开始，扩展正式监听 RESULTS_DIR。但它监听的是后台任务结果文件，不是启动子 Agent
	// startResultWatcher() → fs.watch(RESULTS_DIR) → 等待未来产生结果文件
	startResultWatcher();

	// 处理已经存在的结果，真正的 handleResult() 会把结果转化为完成事件，成功消费或确认重复后删除对应 JSON 文件
	primeExistingResults();

	// 定义运行时清理函数
	const runtimeCleanup = () => {
		// 释放 Watchdog 创建的运行资源
		mainWatchdog.dispose();
		// 停止监听结果目录，防止 reload 后旧 Watcher和新 Watcher同时处理文件
		stopResultWatcher();
		// 停止计划任务 Manager 创建的 Timer
		scheduledRunManager.stop();
		// 关闭父子控制通信通道
		supervisorChannel.dispose();
		// 清除尚未发送的前台任务控制通知以及对应 Timer
		clearPendingForegroundControlNotices(state);
		// 如果后台轮询器正在运行：
		// - 通过 clearInterval() 停止它。
		// - 把引用设置为 null。
		// 只调用 clearInterval() 不把字段设为 null，其他代码可能仍然误认为 poller 存在
		if (state.poller) {
			clearInterval(state.poller);
			state.poller = null;
		}
	};
	// 把 Cleanup 保存到全局对象
	globalStore[runtimeCleanupStoreKey] = runtimeCleanup;

	/* 创建后台任务 Tracker
	1. ensurePoller
	确保后台轮询器存在：
	没有 poller
		-> 创建 setInterval
	已经有 poller
		-> 不重复创建

	2.handleStarted
	处理后台子 Agent启动事件：
	收到 SUBAGENT_ASYNC_STARTED_EVENT
		↓
	把任务放入 state.asyncJobs
		↓
	启动 poller
		↓
	更新 UI

	3. handleComplete
	处理后台子 Agent完成事件：
	收到完成事件
		↓
	找到对应 asyncJob
		↓
	改为 complete / failed
		↓
	更新 UI
		↓
	安排延迟清理

	4. resetJobs
	清除当前扩展实例内存中的任务跟踪状态：
	state.asyncJobs.clear();
	state.foregroundControls.clear();
	主要用于 Session 切换或重新初始化。

	5. restoreActiveJobs
	扫描 ASYNC_DIR，恢复仍处于：
	queued
	running
	状态的后台任务。
	它让 reload 后的新扩展实例重新追踪旧后台子 Agent。 
	*/
	const { ensurePoller, handleStarted, handleComplete, resetJobs, restoreActiveJobs } = createAsyncJobTracker(
		pi,
		state,
		// 这里不是最终结果目录，而是后台任务的运行状态目录：
		// ASYNC_DIR
		// 	└── <runId>
		// 		  ├── status.json
		// 		  ├── events.jsonl
		// 		  └── 运行元数据
		// 到这里仍然没有启动子 Agent，只是创建了追踪后台任务的能力
		ASYNC_DIR,
	);

	// 声明 executorExecute 占位变量，这里使用 let，因为现在还没有真正的 executor，稍后才能赋值
	// 代码使用“延迟绑定”：
	// 先声明 executorExecute = undefined
	// 	↓
	// 创建 scheduledRunManager
	// 	↓
	// 创建 executor
	// 	↓
	// executorExecute = executor.execute
	let executorExecute:
		| ((
				// 本次调用的标识符。普通 Tool Call会由 Pi提供 Tool Call ID；计划任务没有普通
				// Tool Call ID，所以后面使用 randomUUID() 创建。
				id: string,
				// AI 或其他入口传入的参数，例如：
				// {
				// 	agent: "worker",
				// 	task: "分析模块",
				// 	async: true
				// }
				// 也可能是 Parallel、Chain、Status、Interrupt 等管理参数。
				params: SubagentParamsLike,
				// 中断信号。当父 Agent取消当前操作时，可以通过：signal.aborted通知执行器停止等待或终止相应工作
				signal: AbortSignal,
				// 流式更新回调（可选，允许 undefined）：前台子 Agent 运行时，可不断回传当前输出、当前 Tool、Token、Turn、进度
				onUpdate: ((r: AgentToolResult<Details>) => void) | undefined,
				// 当前 Pi 扩展上下文，包含：cwd、当前模型、SessionManager、UI、当前 Tool 集合、会话状态。
				ctx: ExtensionContext,
		  ) => Promise<AgentToolResult<Details>>) // 说明执行器异步返回一个标准 Tool Result
		| undefined;

	// 它负责管理计划任务，例如：10分钟后启动子 Agent、指定时间启动子 Agent、查看计划任务、取消计划任务。
	const scheduledRunManager = createScheduledRunManager({
		config, // ScheduledRunManager 根据配置判断：是否启用计划任务、最大待执行数量、最大延迟时间、其他调度策略。
		/* 
		ScheduledRunManager 到时间后，并不自己了解如何运行子 Agent。
		它只调用外部注入的：launch(...)
		这是一种依赖注入：
		- ScheduledRunManager 只负责“何时启动”
		- executor负责“怎样启动” 
		*/
		launch: (params, ctx, signal) => {
			// 检查执行器是否已经准备好，因为 executorExecute 初始是 undefined，所以调用前必须检查。
			// 正常情况下，计划任务真正触发时，后面的 executor 已经创建完成。
			// 这个判断主要防止极端初始化竞争或异常状态。
			if (!executorExecute) {
				// 返回标准错误结果
				return Promise.resolve({
					content: [{ type: "text", text: "Scheduled subagent launch is unavailable (executor not ready)." }],
					isError: true,
					// 说明这是计划任务管理操作的结果，不是 Single、Parallel 或 Chain 执行结果
					details: { mode: "management" as const, results: [] },
				});
			}
			// 调用真正执行函数，计划任务不是普通 AI Tool Call，没有 Pi提供的 Tool Call ID，因此创建一个唯一 ID。
			// undefined 这里对应 onUpdate，计划任务后台触发时，没有当前前台 Tool UI需要持续接收更新，因此不传流式更新回调
			return executorExecute(randomUUID(), params, signal, undefined, ctx);
		},
	});

	// 普通工厂函数，它的职责是：接收各种依赖 → 在闭包中创建异步 execute 函数 → 把 execute 放进对象 → 返回这个对象
	// 创建真正的 executor，这里的“创建执行器”仍然不是执行子 Agent，它是一个包含 execute 异步函数的对象：
	/* {
		execute: (
			id: string,
			params: SubagentParamsLike,
			signal: AbortSignal,
			onUpdate: (...) | undefined,
			ctx: ExtensionContext,
		) => Promise<AgentToolResult<Details>>;
	} */
	const executor = createSubagentExecutor({
		pi,
		state,
		config,
		asyncByDefault,
		// 接入计划任务管理，subagent Tool不仅支持执行任务，也支持管理 Action，例如计划任务
		// executor 发现这是计划任务 Action 后，不自己处理，而是转交 scheduledRunManager.handleToolCall(...)
		handleScheduledRunAction: (params, ctx) => scheduledRunManager.handleToolCall(params, ctx),
		watchdog: mainWatchdog, // 执行子 Agent时可以使用主 Watchdog监控运行状态
		tempArtifactsDir, // 执行器遇到过长输出、transcript 或其他 Artifact 时，知道保存到哪里
		getSubagentSessionRoot, // executor 到真正执行任务、拿到父 Session文件后才调用它，这样可以为每次运行动态计算子 Agent Session目录
		expandTilde, // 传入路径展开函数
		discoverAgents, // 传入 Agent发现函数，发现并加载所有可用的子 Agent
	});
	/* 
	现在 executor 已经创建完成，大致结构是：
	executor = {
		execute: async (
			id,
			params,
			signal,
			onUpdate,
			ctx,
		) => {
			// 解析 Action
			// 解析 Single / Parallel / Chain
			// 决定前台或后台
			// 检查深度和并发
			// 最终调用 spawn
		},
	};
	但目前还没有调用 executor.execute()，所以没有创建子 Agent 

	最终形成的关系
	createSubagentExecutor()
		↓
	executor
		└── execute()

	普通 subagent Tool
		└── executor.execute()

	ScheduledRunManager
		└── launch()
			└── executorExecute
					└── executor.execute()

	executor.execute()
		├── 使用 pi
		├── 读写 state
		├── 读取 config
		├── 调用 watchdog
		├── 发现 Agent配置
		├── 创建 Session目录
		└── 最终启动子 Agent
	这一段最关键的两个结论：
	createSubagentExecutor() 只是创建执行函数，不会立即启动子 Agent。
	executorExecute 是为了解决 ScheduledRunManager 和 executor 相互依赖而设置的延迟绑定变量。
	*/
	executorExecute = executor.execute; // 完成延迟绑定，解决了前面的循环依赖

	pi.registerMessageRenderer<SlashMessageDetails>(SLASH_RESULT_TYPE, (message, options, theme) => {
		const details = resolveSlashMessageDetails(message.details);
		if (!details) return undefined;
		return createSlashResultComponent(details, options, theme);
	});

	pi.registerMessageRenderer<undefined>(SLASH_TEXT_RESULT_TYPE, (message, _options, _theme) => {
		const content =
			typeof message.content === "string"
				? message.content
				: message.content
						.filter((entry) => entry.type === "text")
						.map((entry) => entry.text)
						.join("\n");
		return new Text(content, 0, 0);
	});

	pi.registerMessageRenderer<SubagentNotifyDetails>("subagent-notify", (message, options, theme) => {
		const content = typeof message.content === "string" ? message.content : "";
		const details = (message.details as SubagentNotifyDetails | undefined) ?? parseSubagentNotifyContent(content);
		if (!details) return new Text(content, 0, 0);
		const icon =
			details.status === "completed"
				? theme.fg("success", "✓")
				: details.status === "paused"
					? theme.fg("warning", "■")
					: theme.fg("error", "✗");
		const parts: string[] = [];
		if (details.taskInfo) parts.push(details.taskInfo);
		if (details.durationMs !== undefined) parts.push(formatDuration(details.durationMs));
		let text = `${icon} ${theme.bold(details.agent)} ${theme.fg("dim", details.status)}`;
		if (parts.length > 0)
			text += ` ${theme.fg("dim", "·")} ${parts.map((part) => theme.fg("dim", part)).join(` ${theme.fg("dim", "·")} `)}`;
		const trimmedPreview = details.resultPreview.trim();
		const previewLines = options.expanded
			? trimmedPreview.split("\n").filter((line) => line.trim())
			: [trimmedPreview.split("\n", 1)[0] ?? ""].filter((line) => line.trim());
		for (const line of previewLines.length > 0 ? previewLines : ["(no output)"]) {
			text += `\n  ${theme.fg("dim", `⎿  ${line}`)}`;
		}
		if (!options.expanded && trimmedPreview.includes("\n")) {
			const expandKey = keyText("app.tools.expand");
			text += `\n  ${theme.fg("dim", `${expandKey} full notification`)}`;
		}
		if (details.sessionLabel && details.sessionValue) {
			text += `\n  ${theme.fg("muted", `${details.sessionLabel}: ${shortenPath(details.sessionValue)}`)}`;
		}
		return new Text(text, 0, 0);
	});

	pi.registerMessageRenderer<SubagentControlMessageDetails>(
		SUBAGENT_CONTROL_MESSAGE_TYPE,
		(message, _options, theme) => {
			const details = message.details as SubagentControlMessageDetails | undefined;
			if (!details?.event) return undefined;
			const content = typeof message.content === "string" ? message.content : undefined;
			return new SubagentControlNoticeComponent(
				{ ...details, noticeText: formatSubagentControlNotice(details, content) },
				theme,
			);
		},
	);
	// 这是对 executor.execute() 的一层很薄的包装，它没有实现子 Agent 执行逻辑，只是在调用 executor 前调整 UI
	const executeSubagentCollapsed = (
		id: string,
		params: SubagentParamsLike,
		signal: AbortSignal,
		onUpdate: ((result: AgentToolResult<Details>) => void) | undefined,
		ctx: ExtensionContext,
	) => {
		/* ctx.hasUI 表示当前是不是交互式终端，把 Tool详情默认设置成折叠状态。
		原因是子 Agent执行时可能产生大量：
		中间输出
		Tool Call
		多个并行任务
		Token和进度信息
		如果全部默认展开，终端会被大量内容占满。
		这只影响显示，不影响执行结果和上下文。 */
		if (ctx.hasUI) ctx.ui.setToolsExpanded(false);
		/* 
		这里才进入前面创建的 Subagent Executor。
		调用链：
		executeSubagentCollapsed()
			↓
		executor.execute()
			↓
		解析 params
			↓
		判断 Action / Single / Parallel / Chain
			↓
		判断 foreground / async
			↓
		检查并发、深度和预算
			↓
		最终 spawn 子 Agent
		因为 executor.execute() 返回 Promise，这个包装函数也会返回同一个 Promise 
		*/
		return executor.execute(id, params, signal, onUpdate, ctx);
	};

	const slashBridge = registerSlashSubagentBridge({
		events: pi.events,
		getContext: () => state.lastUiContext,
		execute: (id, params, signal, onUpdate, ctx) => executeSubagentCollapsed(id, params, signal, onUpdate, ctx),
	});

	const promptTemplateBridge = registerPromptTemplateDelegationBridge({
		events: pi.events,
		getContext: () => state.lastUiContext,
		execute: async (requestId, request, signal, ctx, onUpdate) => {
			if (request.tasks && request.tasks.length > 0) {
				return executeSubagentCollapsed(
					requestId,
					{
						tasks: request.tasks,
						context: request.context,
						cwd: request.cwd,
						worktree: request.worktree,
						async: false,
						clarify: false,
					},
					signal,
					onUpdate,
					ctx,
				);
			}
			return executeSubagentCollapsed(
				requestId,
				{
					agent: request.agent,
					task: request.task,
					context: request.context,
					cwd: request.cwd,
					model: request.model,
					async: false,
					clarify: false,
				},
				signal,
				onUpdate,
				ctx,
			);
		},
	});

	const rpcBridge = registerSubagentRpcBridge({
		events: pi.events,
		getContext: () => state.lastUiContext,
		execute: (id, params, signal, onUpdate, ctx) => executor.execute(id, params, signal, onUpdate, ctx),
	});

	/* 
	计算 Parallel 的实际任务数量，这个函数只用于 UI显示，不负责真正执行并发任务
	这个函数只用于 UI显示，不负责真正执行并发任务。
	为什么不能直接使用：tasks.length
	因为每个 task 还可以有：count
	例如：
	tasks: [
		{
			agent: "worker",
			task: "实现不同方案",
			count: 3,
		},
		{
			agent: "reviewer",
			task: "审查结果",
		},
	]
	数组长度是 2，但实际会派生：
	3个 worker
	1个 reviewer
	实际数量是 4。 
	*/
	function effectiveParallelTaskCount(tasks: Array<{ count?: unknown }> | undefined): number {
		// 没有任务时返回 0
		if (!tasks || tasks.length === 0) return 0;
		// 使用 reduce 累计数量
		return tasks.reduce((total, task) => {
			// 只有满足三个条件，才使用用户传入的 count：是数字、是整数、大于等于 1。
			const count =
				typeof task.count === "number" && Number.isInteger(task.count) && task.count >= 1 ? task.count : 1;
			return total + count;
		}, 0);
	}

	// 定义 Subagent Tool
	const tool: ToolDefinition<typeof SubagentParams, Details> = {
		name: "subagent",
		label: "Subagent",
		// 为什么传入 config？因为一些功能可能没有开启，需要动态改造 Description，例如：
		// {
		//   "scheduledRuns": {
		//     "enabled": false
		//   }
		// }
		// 如果计划任务没有开启，就不应该在 Tool Description中鼓励模型调用 schedule。
		// 这个 description 会进入 LLM可见的 Tool Schema，是 AI判断何时调用 subagent 的重要信息。
		description: buildSubagentToolDescription(config),
		/* 
		它既用于告诉模型参数格式，也用于校验调用参数
		使用了 typebox 定义的 Schema
		TypeBox 最核心的价值就在于：只写一遍代码，就能同时得到 TypeScript 的静态类型和 JSON Schema 校验规则
		而 TypeBox 的做法是用 JavaScript 代码来描述类型定义，就像这样：
		// 这不是一个普通对象，而是一个“类型定义 + 运行时值”
		const UserSchema = Type.Object({
		id: Type.String(),
		name: Type.String()
		})
		这样做的好处是，这个 UserSchema 在代码运行时依然“活着”，可以被程序读取和使用
		TypeBox 的专长，是把你用它的 API 定义的类型结构，直接构建成一个标准的 JSON Schema 对象
		也就是一个符合 JSON Schema 规范的大 JSON 对象
		比如上面的 UserSchema，其本质就是一个长这样的 JSON 对象：
		{
			"type": "object",
			"properties": {
			  "id": { "type": "string" },
			  "name": { "type": "string" }
			},
			"required": ["id", "name"]
		} */
		parameters: SubagentParams,

		// 真正响应 Function Call
		// 当 AI调用 subagent Tool时，Pi Agent Loop调用这个函数
		// 参数由 Pi提供：
		// id       -> Tool Call ID
		// params   -> AI生成的 arguments
		// signal   -> 当前运行的中断信号
		// onUpdate -> Tool流式更新回调
		// ctx      -> ExtensionContext
		execute(id, params, signal, onUpdate, ctx) {
			// 所以 Tool执行链是：
			// AI Function Call
			// 	↓
			// tool.execute()
			// 	↓
			// executeSubagentCollapsed()
			// 	↓
			// 折叠 UI
			// 	↓
			// executor.execute()
			// tool.execute() 本身没有实现 Single、Parallel 或 Chain，只负责把调用转发给统一 executor
			return executeSubagentCollapsed(id, params, signal, onUpdate, ctx);
		},
		// 显示 Tool调用，这个函数只负责终端显示，不参与 Tool执行，也不会发送给 LLM作为结果
		// args 是 AI传入的 Tool参数。theme 用于设置颜色、粗体和其他终端样式
		renderCall(args, theme) {
			// 管理 Action分支，如果存在 action，说明这不是普通执行请求，而是管理操作
			if (args.action) {
				// 确定显示目标
				const target = args.agent || args.chainName || "";
				// 构造管理操作文本
				return new Text(
					`${theme.fg("toolTitle", theme.bold("subagent "))}${args.action}${target ? ` ${theme.fg("accent", target)}` : ""}`,
					0,
					0,
				);
			}
			// 判断是不是 Parallel
			const isParallel = (args.tasks?.length ?? 0) > 0;
			// 计算 Parallel实际数量
			const parallelCount = effectiveParallelTaskCount(args.tasks as Array<{ count?: unknown }> | undefined);
			// 生成 Async标签
			const asyncLabel = args.async === true && args.clarify !== true ? theme.fg("warning", " [async]") : "";
			if (args.chain?.length)
				return new Text(
					`${theme.fg("toolTitle", theme.bold("subagent "))}chain (${args.chain.length})${asyncLabel}`,
					0,
					0,
				);
			if (isParallel)
				return new Text(
					`${theme.fg("toolTitle", theme.bold("subagent "))}parallel (${parallelCount})${asyncLabel}`,
					0,
					0,
				);
			return new Text(
				`${theme.fg("toolTitle", theme.bold("subagent "))}${theme.fg("accent", args.agent || "?")}${asyncLabel}`,
				0,
				0,
			);
		},

		renderResult(result, options, theme, context) {
			if (subagentResultIsRunning(result)) {
				ensureSubagentResultAnimation(context);
			} else {
				clearLegacyResultAnimationTimer(context);
			}
			const frame = (context.state as { frame?: number } | undefined)?.frame ?? 0;
			return renderSubagentResult(result, options, theme, frame);
		},
	};
	// 正式注册 Tool
	pi.registerTool(tool);

	const waitTool: ToolDefinition<typeof WaitParams, Details> = {
		name: "wait",
		label: "Wait",
		description: `Block until background (async) subagent runs started in this session finish, then return.

Use this after launching async subagents when you have no independent work left and must not end your turn — for example inside a skill that has to run to completion, or any non-interactive run (\`pi -p ...\`) where the whole task is a single turn and ending it would abandon the still-running children.

• { } — return as soon as the FIRST active run finishes (default). Ideal for a rolling fleet: launch N, wait, spawn a replacement for the one that finished, wait again — keeping N in flight.
• { all: true } — block until EVERY active run in this session is finished.
• { id: "..." } — wait for one specific run (id or prefix) to finish.
• { timeoutMs: 600000 } — stop waiting after N ms (the runs keep going regardless; default 30 min)

wait also returns when a run needs attention (a child that went idle or blocked for a decision), not only on completion — so a stuck child never stalls the loop; the summary names the run(s) to inspect/nudge/resume/interrupt. It wakes the instant a completion or control event arrives (subscribed to Pi's event bus, with a poll fallback that reconciles crashed runners), keeps the turn alive for normal notification delivery, and resolves early if the turn is aborted.${waitToolConfig.enabled ? "" : "\n\nConfigured behavior: wait is disabled by config.waitTool or PI_SUBAGENT_WAIT_TOOL_ENABLED and returns immediately without blocking."}`,
		parameters: WaitParams,
		execute(_id, params, signal, _onUpdate, _ctx) {
			return waitForSubagents(params, signal, { state, events: pi.events, enabled: waitToolConfig.enabled });
		},
	};
	pi.registerTool(waitTool);

	registerSlashCommands(pi, state);

	const eventUnsubscribeStoreKey = "__piSubagentEventUnsubscribes";
	const controlNoticeSeenStoreKey = "__piSubagentVisibleControlNotices";
	const previousEventUnsubscribes = globalStore[eventUnsubscribeStoreKey];
	if (Array.isArray(previousEventUnsubscribes)) {
		for (const unsubscribe of previousEventUnsubscribes) {
			if (typeof unsubscribe !== "function") continue;
			try {
				unsubscribe();
			} catch {
				// Best effort cleanup for stale handlers from an older reload.
			}
		}
	}
	registerSubagentNotify(pi, state, { batchConfig: config.completionBatch });

	const existingVisibleControlNotices = globalStore[controlNoticeSeenStoreKey];
	const visibleControlNotices =
		existingVisibleControlNotices instanceof Set ? (existingVisibleControlNotices as Set<string>) : new Set<string>();
	globalStore[controlNoticeSeenStoreKey] = visibleControlNotices;
	const controlEventHandler = (payload: unknown) => {
		handleSubagentControlNotice({
			pi,
			state,
			visibleControlNotices,
			details: payload as SubagentControlMessageDetails,
		});
	};
	const eventUnsubscribes = [
		pi.events.on(SUBAGENT_ASYNC_STARTED_EVENT, handleStarted),
		pi.events.on(SUBAGENT_ASYNC_COMPLETE_EVENT, handleComplete),
		pi.events.on(SUBAGENT_CONTROL_EVENT, controlEventHandler),
		rpcBridge.dispose,
	];
	globalStore[eventUnsubscribeStoreKey] = eventUnsubscribes;

	pi.on("tool_result", (event, ctx) => {
		if (event.toolName !== "subagent") return;
		if (!ctx.hasUI) return;
		state.lastUiContext = ctx;
		if (state.asyncJobs.size > 0) {
			renderWidget(ctx, Array.from(state.asyncJobs.values()));
			ctx.ui.requestRender?.();
			ensurePoller();
		}
	});

	const cleanupSessionArtifacts = (ctx: ExtensionContext) => {
		try {
			const sessionFile = ctx.sessionManager.getSessionFile();
			if (sessionFile) {
				cleanupOldArtifacts(getArtifactsDir(sessionFile), DEFAULT_ARTIFACT_CONFIG.cleanupDays);
			}
		} catch {
			// Cleanup failures should not block session lifecycle events.
		}
	};

	const resetSessionState = (ctx: ExtensionContext) => {
		state.baseCwd = ctx.cwd;
		state.currentSessionId = resolveCurrentSessionId(ctx.sessionManager);
		state.subagentSpawns = { sessionId: state.currentSessionId, count: 0 };
		// Set PI_SUBAGENT_PARENT_SESSION for permission-system forwarding.
		// Only set in the root session (the interactive UI session), not in
		// child subagent processes — children inherit the parent's value
		// through the process environment at spawn time and must not overwrite
		// it with their own session identity.
		if (!process.env[SUBAGENT_CHILD_ENV]) {
			const sessionId = ctx.sessionManager.getSessionId();
			if (sessionId) {
				process.env[SUBAGENT_PARENT_SESSION_ENV] = sessionId;
			}
		}
		state.lastUiContext = ctx;
		cleanupSessionArtifacts(ctx);
		clearPendingForegroundControlNotices(state);
		resetJobs(ctx);
		restoreActiveJobs(ctx);
		scheduledRunManager.bindSession(ctx);
		restoreSlashFinalSnapshots(ctx.sessionManager.getEntries());
		primeExistingResults();
	};

	pi.on("session_start", (_event, ctx) => {
		resetSessionState(ctx);
		rpcBridge.emitReady(ctx);
		supervisorChannel.start();
	});

	pi.on("session_shutdown", () => {
		delete process.env[SUBAGENT_PARENT_SESSION_ENV];
		for (const unsubscribe of eventUnsubscribes) {
			try {
				unsubscribe();
			} catch {
				// Best effort cleanup during shutdown.
			}
		}
		if (globalStore[eventUnsubscribeStoreKey] === eventUnsubscribes) {
			delete globalStore[eventUnsubscribeStoreKey];
		}
		stopResultWatcher();
		scheduledRunManager.stop();
		if (state.poller) clearInterval(state.poller);
		state.poller = null;
		clearPendingForegroundControlNotices(state);
		for (const timer of state.cleanupTimers.values()) {
			clearTimeout(timer);
		}
		state.cleanupTimers.clear();
		state.asyncJobs.clear();
		clearSlashSnapshots();
		slashBridge.cancelAll();
		slashBridge.dispose();
		promptTemplateBridge.cancelAll();
		promptTemplateBridge.dispose();
		supervisorChannel.dispose();
		if (globalStore[runtimeCleanupStoreKey] === runtimeCleanup) {
			delete globalStore[runtimeCleanupStoreKey];
		}
		try {
			if (state.lastUiContext?.hasUI) {
				state.lastUiContext.ui.setWidget(WIDGET_KEY, undefined);
			}
		} catch (error) {
			if (!isStaleExtensionContextError(error)) throw error;
		}
	});
}
