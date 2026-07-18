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
	整个 Node.js 进程，旧扩展创建的资源（例如 watcher、poller、timer、事件订阅）可能还活着

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
	// RESULTS_DIR：保存后台子 Agent 的最终结果和完成通知
	// ASYNC_DIR：保存后台运行的状态、事件和元数据
	// 之所以放在系统临时目录，是因为事件量可能很大，而且它只是运行期可观测数据，不承诺长期保留。macOS或扩展以后都可能清理它
	ensureAccessibleDir(RESULTS_DIR);
	ensureAccessibleDir(ASYNC_DIR);
	cleanupOldChainDirs();

	// config 是运行策略，从下面的位置读取配置，~/.pi/agent/extensions/subagent/config.json
	// 配置文件不存在 -> 返回 {}
	// 配置正确       -> 返回解析后的对象
	// 配置错误       -> 打印错误，返回 {}
	// 所以配置损坏不会阻止 Pi 启动，而是退回默认行为
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
		/* ctx.hasUI 表示当前是交互式终端，需要把 Tool 详情默认设置成折叠状态
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

	//===============================================
	// slashBridge 和 promptTemplateBridge 都是把“非 Tool 调用”接到 Subagent Executor
	//===============================================
	
	/* 注册 Slash 桥接器，它负责处理 Slash 命令的执行
	处理用户在终端输入的 Slash 命令：
	/subagent worker 分析代码
	→ Slash Bridge
	→ executor.execute()
	调用者是用户。
	 */
	const slashBridge = registerSlashSubagentBridge({
		events: pi.events,
		// Slash Bridge 是通过事件触发的，没有普通 Tool Call 自动传入的 ctx，每次执行都拿到最新 Session 的 Context
		getContext: () => state.lastUiContext,
		execute: (id, params, signal, onUpdate, ctx) => executeSubagentCollapsed(id, params, signal, onUpdate, ctx),
	});

	/* 注册 Prompt Template 桥接器，它负责处理 Prompt Template 的执行
	处理 Prompt Template 中声明的 Subagent 委托：
	---
	subagent: worker
	model: kimi
	---

	分析 $@
	执行时：
	Prompt Template
	→ PromptTemplate Bridge
	→ executor.execute()
	调用者是 Prompt 模板系统
	 */
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

	// 正式注册 Tool:这一行才把前面创建的 tool 加入 Pi Tool Registry
	// 执行前：
	// tool只是 index.ts 中的局部对象
	// AI看不到它
	// Agent Loop也找不到它
	// 执行后：
	// Pi Tool Registry
	// 	└── subagent
	// 		  ├── description
	// 		  ├── parameters
	// 		  ├── execute
	// 		  ├── renderCall
	// 		  └── renderResult
	// 之后请求 LLM时，Pi会把 subagent 的：
	// name
	// description
	// parameters
	// 转换成模型可见的 Function Tool。
	pi.registerTool(tool);

	// 定义 wait Tool，用来等待异步子 Agent完成
	// 模型最终会生成：
	// {
	// 	"name": "wait",
	// 	"arguments": {
	// 	  "all": true
	// 	}
	// }
	const waitTool: ToolDefinition<typeof WaitParams, Details> = {
		name: "wait",
		label: "Wait",
		/* 阻塞，直到本次会话中启动的后台（异步）子代理任务全部完成，然后返回。

		在你启动异步子代理之后使用，当你没有其他独立工作可做，且不能结束当前轮次时——比如在一个必须运行完成的 skill 内部
		或者在任何一个非交互式执行（pi -p ...）中，整个任务只有一个轮次，结束它会导致仍在运行的子任务被遗弃。
		
		• { } —— 等待当前任务集合中第一个任务完成，适合滚动并发：
			启动 4 个任务
			-> wait 等到一个完成
			-> 再补一个
			-> 始终保持 4 个任务运行
		
		• { all: true } —— 阻塞，直到本次会话中所有活跃任务全部完成。
		
		• { id: "..." } —— 等待某个特定任务（通过完整 id 或前缀匹配）完成。
		
		• { timeoutMs: 600000 } —— 等待期间持续监听和检查，在 N 毫秒后停止等待（任务会继续运行不受影响；默认 30 分钟）。
		
		wait 不仅会在任务完成时返回，当某个任务需要关注时（比如子任务进入空闲状态，或卡住等待决策）也会返回——这样卡住的子
		任务永远不会阻塞整个循环；返回的摘要信息中会指出需要检查/催促/恢复/中断的任务。
		
		它在完成事件或控制事件到达的瞬间就会被唤醒（通过订阅 Pi 的事件总线，并配合轮询兜底机制来处理崩溃的 runner），保持当
		前轮次存活以确保正常通知能够送达，如果轮次被中止则提前 resolve。
		
		配置行为：如果通过 config.waitTool 或 PI_SUBAGENT_WAIT_TOOL_ENABLED 禁用了 wait，则它会立即返回，不阻塞。 */
		description: `Block until background (async) subagent runs started in this session finish, then return.

Use this after launching async subagents when you have no independent work left and must not end your turn — for example inside a skill that has to run to completion, or any non-interactive run (\`pi -p ...\`) where the whole task is a single turn and ending it would abandon the still-running children.

• { } — return as soon as the FIRST active run finishes (default). Ideal for a rolling fleet: launch N, wait, spawn a replacement for the one that finished, wait again — keeping N in flight.
• { all: true } — block until EVERY active run in this session is finished.
• { id: "..." } — wait for one specific run (id or prefix) to finish.
• { timeoutMs: 600000 } — stop waiting after N ms (the runs keep going regardless; default 30 min)

wait also returns when a run needs attention (a child that went idle or blocked for a decision), not only on completion — so a stuck child never stalls the loop; the summary names the run(s) to inspect/nudge/resume/interrupt. It wakes the instant a completion or control event arrives (subscribed to Pi's event bus, with a poll fallback that reconciles crashed runners), keeps the turn alive for normal notification delivery, and resolves early if the turn is aborted.${waitToolConfig.enabled ? "" : "\n\nConfigured behavior: wait is disabled by config.waitTool or PI_SUBAGENT_WAIT_TOOL_ENABLED and returns immediately without blocking."}`,
		parameters: WaitParams, // 参数类型是定义 /extension/schemas.ts L299 
		execute(_id, params, signal, _onUpdate, _ctx) {
			// state 读取当前 Session 的 asyncJobs 等运行状态。
			// pi.events 订阅后台任务完成和控制事件。相比纯轮询，任务完成时可以立即唤醒
			// enabled 如果禁用，立即返回，不真正阻塞
			// waitForSubagents() 内部还保留轮询兜底：EventBus 事件唤醒+定期扫描磁盘状态，即使后台进程崩溃、完成事件丢失，也有机会从持久化状态发现变化
			return waitForSubagents(params, signal, { state, events: pi.events, enabled: waitToolConfig.enabled });
		},
	};
	// 到这里 wait 才真正注册到 Pi，之后 LLM 才能看到并调用它
	pi.registerTool(waitTool);

	// 注册 Slash 命令，这里注册用户在终端直接输入的 Subagent 命令
	// Slash 命令和 Tool 两者最终可以共享同一份 state，所以 Slash 命令能够查看和控制 Tool 启动的任务
	registerSlashCommands(pi, state);

	/* 准备热重载清理键
	它们会作为 globalThis 的字段名。
	eventUnsubscribeStoreKey：保存事件取消订阅函数。
	controlNoticeSeenStoreKey：保存已经显示过的控制通知。
	使用全局对象是因为 /reload 会创建新扩展实例，但 Node 进程和 globalThis 可能仍然存在 */
	const eventUnsubscribeStoreKey = "__piSubagentEventUnsubscribes";
	const controlNoticeSeenStoreKey = "__piSubagentVisibleControlNotices";
	// 清理上一次加载的事件订阅，读取旧扩展留下的取消订阅函数数组，逐个处理旧订阅
	const previousEventUnsubscribes = globalStore[eventUnsubscribeStoreKey];
	if (Array.isArray(previousEventUnsubscribes)) {
		for (const unsubscribe of previousEventUnsubscribes) {
			// 全局数据的类型不可信，调用前先检查
			if (typeof unsubscribe !== "function") continue;
			try {
				// 取消旧监听器，失败不会阻止新扩展加载，如果不清理，/reload 后一个完成事件可能被处理多次
				unsubscribe();
			} catch {
				// Best effort cleanup for stale handlers from an older reload.
			}
		}
	}
	// 注册后台完成通知，它主要订阅后台任务完成事件，然后向父 Agent发送可见通知
	registerSubagentNotify(pi, state, { batchConfig: config.completionBatch });

	// 恢复通知去重集合，尝试读取旧实例保存的去重集合，如果已经是 Set 就继续复用，否则新建。
	// 为什么热重载后还要复用？因为控制事件可能已经显示过。若 /reload 后清空去重记录，同一个 needs_attention 事件可能再次弹出。
	const existingVisibleControlNotices = globalStore[controlNoticeSeenStoreKey];
	const visibleControlNotices =
		existingVisibleControlNotices instanceof Set ? (existingVisibleControlNotices as Set<string>) : new Set<string>();
	// 把集合写回全局对象，供当前实例和未来重载实例使用
	globalStore[controlNoticeSeenStoreKey] = visibleControlNotices;
	// 包装控制事件处理器，EventBus 传入的数据先视为 unknown
	const controlEventHandler = (payload: unknown) => {
		// 处理控制事件，payload 是控制事件的详细信息
		handleSubagentControlNotice({
			pi,
			state,
			visibleControlNotices,
			details: payload as SubagentControlMessageDetails,
		});
	};
	/* 订阅三个后台事件，数组中保存的是取消订阅函数

	监听 subagent:async-started。收到后，handleStarted 会：
	- 将任务加入 state.asyncJobs
	- 记录 PID、模式、Agent 和超时
	- 启动 Poller
	- 更新 Widget

	监听 subagent:async-complete。收到后会：
	- 更新任务状态为 complete/failed
	- 更新完成时间
	- 刷新嵌套子任务
	- 重绘 Widget
	- 安排清理

	监听 subagent:control-event，处理长时间运行或需要干预等控制通知 

	Widget 是终端中显示后台任务状态的区域。
	刷新前可能是：
	researcher  running
	scout-1   running
	scout-2   running
	收到完成事件后刷新为：
	researcher  running
	scout-1   complete
	scout-2   running
	*/
	const eventUnsubscribes = [
		pi.events.on(SUBAGENT_ASYNC_STARTED_EVENT, handleStarted),
		pi.events.on(SUBAGENT_ASYNC_COMPLETE_EVENT, handleComplete),
		pi.events.on(SUBAGENT_CONTROL_EVENT, controlEventHandler),
		rpcBridge.dispose, // 它不是事件订阅，而是一个清理函数。放进相同数组，是为了在 Session 关闭或热重载时统一调用
	];
	// 保存当前实例的清理函数，下一次 /reload 可以先取消这些旧订阅
	globalStore[eventUnsubscribeStoreKey] = eventUnsubscribes;

	// 监听 subagent Tool Result，这是 Pi 生命周期事件，不是扩展自定义 EventBus
	// 机制					事件来源		   事件名称
	// Agent.subscribe() Agent Loop	固定的   AgentEvent
	// pi.on()			  Pi 扩展生命周期	  Pi 预定义事件
	// pi.events()		   扩展自己发布		  自定义频道
	pi.on("tool_result", (event, ctx) => {
		// 只处理 subagent Tool，不处理 bash、read、wait 等其他工具
		if (event.toolName !== "subagent") return;
		// 非交互模式没有终端 Widget，这种情况不做 UI 更新
		if (!ctx.hasUI) return;
		// 保存最新可用 UI Context，供之后异步完成事件使用
		state.lastUiContext = ctx;
		// 只有存在后台任务才显示 Widget。同步子 Agent 已经直接返回结果，不需要后台状态栏
		if (state.asyncJobs.size > 0) {
			// 将 Map 中的后台任务转换成数组并渲染，例如显示：
			// worker running  2m  turns=4  tools=7
			renderWidget(ctx, Array.from(state.asyncJobs.values()));
			// 请求终端立即重绘
			ctx.ui.requestRender?.();
			// 确保后台状态轮询器已经启动
			// 子 Agent会把最新状态写入自己的运行目录，例如：
			// asyncDir/
			// ├── status.json
			// ├── events.jsonl
			// ├── output-0.log
			// └── result.json
			// poller轮询器周期性读取这些文件，把最新状态同步到：
			// state.asyncJobs
			// 然后刷新终端 Widget。
			ensurePoller();
		}
	});



	/* 
	下面这段代码负责 Subagent 扩展与 Pi Session 的绑定和解绑
	Session 启动
	-> 绑定当前 cwd/sessionId
	-> 清空上一个 Session 的内存状态
	-> 从磁盘恢复当前 Session 的后台任务
	-> 恢复 UI 和计划任务
	-> 启动 RPC/Supervisor

	Session 关闭
	-> 取消事件订阅
	-> 停止 Watcher/Poller/Timer
	-> 清空内存和 UI
	-> 释放通信通道 
	*/

	// 定义一个清理当前 Session 过期产物的函数
	const cleanupSessionArtifacts = (ctx: ExtensionContext) => {
		try {
			// 获取当前 Session 的 JSONL 文件
			const sessionFile = ctx.sessionManager.getSessionFile();
			if (sessionFile) { // 有些内存 Session 可能没有文件，因此先判断
				// 根据父 Session 文件计算对应的 Artifact 目录
				// 里面可能保存：子 Agent 的完整输出、Transcript、被截断的原始结果、中间执行产物。cleanupDays 表示只清理超过保留天数的旧文件。
				cleanupOldArtifacts(getArtifactsDir(sessionFile), DEFAULT_ARTIFACT_CONFIG.cleanupDays);
			}
		} catch {
			// Cleanup failures should not block session lifecycle events.
		}
	};

	// 它是 Session 启动时的核心函数，名字虽然是 reset，实际职责是：清理旧 Session 的内存投影 + 绑定并恢复当前 Session。
	const resetSessionState = (ctx: ExtensionContext) => {
		// 子 Agent没有显式指定 cwd 时，可以使用父 Session 的工作目录
		state.baseCwd = ctx.cwd;
		// 确定当前 Session 身份，通常优先使用 Session 文件路径，没有文件时使用 Session ID
		// 后台任务会记录所属 sessionId。恢复任务时只恢复属于当前 Session 的任务，避免多个会话串数据
		state.currentSessionId = resolveCurrentSessionId(ctx.sessionManager);
		// 重置子 Agent 派生计数 记录当前 Session 已启动多少个子 Agent。
		// 切换到新 Session 后计数重新开始，用于限制单个 Session 的最大 spawn 数量。
		state.subagentSpawns = { sessionId: state.currentSessionId, count: 0 };
		/* 设置父 Session 环境变量，判断当前进程是不是根 Agent。只有根 Agent才能设置：SUBAGENT_PARENT_SESSION_ENV
		父进程之后使用 spawn() 启动子 Agent时，子进程会继承这个变量。
		它主要用于：
		父子 Session 关联
		权限请求转发
		Supervisor 消息路由
		判断任务属于哪个根会话
		为什么子 Agent不能覆盖？
		根 Session A
		  -> 子 Agent B
			  -> 嵌套子 Agent C
		B 和 C 都应该保留“根 Session 是 A”，而不是把父 Session 改成自己。 */
		if (!process.env[SUBAGENT_CHILD_ENV]) {
			const sessionId = ctx.sessionManager.getSessionId();
			if (sessionId) {
				// PI_SUBAGENT_PARENT_SESSION=<根Session ID>
				process.env[SUBAGENT_PARENT_SESSION_ENV] = sessionId;
			}
		}
		// 保存最新 UI Context 后台任务完成时，执行回调的不是某个 pi.on(...) 生命周期 Hook
		// 因此回调参数里通常没有 ExtensionContext ctx；但更新终端 UI 又需要 ctx.ui，所以提前保存最近一次拿到的 ctx。
		state.lastUiContext = ctx;
		// 清理当前 Session 的旧 Artifact
		cleanupSessionArtifacts(ctx);
		// 清除待发送的控制通知，取消上一个 Session 还没发送的通知 Timer
		// 例如：任务长时间运行、任务需要干预、子 Agent 疑似卡住。避免旧通知出现在新 Session 中。
		clearPendingForegroundControlNotices(state);
		// 清空内存任务状态，它会清除：state.asyncJobs、前台控制状态、旧任务清理 Timer
		// Widget 中的旧任务、待处理的结果文件。这里是先清空，再恢复当前 Session。
		resetJobs(ctx);
		// 恢复后台任务,从持久化目录读取属于当前 Session 且状态为 queued 或 running 的后台任务
		// 然后重新加入 state.asyncJobs。如果恢复到后台任务，还会重新启动 Poller 和 Widget
		// 这里不会重新 spawn 子 Agent，只是恢复对已有进程或任务状态的追踪。
		restoreActiveJobs(ctx);
		// 绑定计划任务
		scheduledRunManager.bindSession(ctx);
		// 恢复 Slash 命令结果 从 JSONL Session 历史中读取已保存的 Slash Subagent 结果。
		// 这样重新打开 Session 后，之前的结果仍然可以正确渲染。
		restoreSlashFinalSnapshots(ctx.sessionManager.getEntries());
		// 扫描遗漏的结果文件
		// 主动扫描结果目录中已经存在的 .json 文件。
		// 因为子 Agent可能在这些阶段完成：
		// Pi 关闭期间
		// Watcher 尚未启动时
		// Session 切换期间
		// 扩展 /reload 期间
		// 这些文件不会触发新的 fs.watch 事件，因此启动时必须补扫一次。
		primeExistingResults();
	};

	// 订阅 Pi 的 Session 启动事件。_event 没被使用，所以用下划线标记。
	pi.on("session_start", (_event, ctx) => {
		// 先完成当前 Session 的清理、绑定和状态恢复
		resetSessionState(ctx);
		// 通过 RPC Bridge 发出“当前 Subagent 扩展已准备完成”的信号。
		// 外部 RPC 调用方收到 Ready 后，才适合发起任务。
		rpcBridge.emitReady(ctx);
		// 启动父子 Agent Supervisor 通道，用于：子 Agent 联系父 Agent、权限和决策请求、控制消息、
		// needs_attention、父 Agent 回复子 Agent。注意，这里只启动通信设施，没有创建具体子 Agent。
		supervisorChannel.start();
	});

	// Session 关闭时，释放当前扩展占用的资源
	pi.on("session_shutdown", () => {
		// 清理父 Session 环境变量，防止后续 Session 错误继承旧 Session ID
		delete process.env[SUBAGENT_PARENT_SESSION_ENV];
		// 取消 EventBus 订阅，包括：后台完成通知、轮询器、计划任务、Slash 结果、结果文件监视器
		for (const unsubscribe of eventUnsubscribes) {
			try {
				unsubscribe();
			} catch {
				// Best effort cleanup during shutdown.
			}
		}
		// 清理全局订阅记录
		if (globalStore[eventUnsubscribeStoreKey] === eventUnsubscribes) {
			delete globalStore[eventUnsubscribeStoreKey];
		}
		// 停止结果 Watcher
		stopResultWatcher();
		// 停止计划任务管理器
		scheduledRunManager.stop();
		// 停止 Poller 轮询器
		if (state.poller) clearInterval(state.poller);
		state.poller = null;
		// 清理前台通知与任务 Timer
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
