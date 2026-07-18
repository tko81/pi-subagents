import * as fs from "node:fs";
import * as path from "node:path";
import { formatDuration, formatModelThinking, formatTokens, shortenPath } from "../../shared/formatters.ts";
import { formatActivityLabel, formatParallelOutcome } from "../../shared/status-format.ts";
import { type ActivityState, type AsyncJobStep, type AsyncParallelGroupStatus, type AsyncStatus, type CostSummary, type NestedRunSummary, type SubagentRunMode, type TokenUsage, type TurnBudgetState } from "../../shared/types.ts";
import { readStatus } from "../../shared/utils.ts";
import { attachRootChildrenToSteps, buildNestedRouteIndex, type NestedRoute, projectNestedEvents } from "../shared/nested-events.ts";
import { formatNestedRunStatusLines } from "../shared/nested-render.ts";
import { flatToLogicalStepIndex, normalizeParallelGroups } from "./parallel-groups.ts";
import { reconcileAsyncRun, reconcileNestedAsyncDescendants } from "./stale-run-reconciler.ts";

interface AsyncRunStepSummary {
	// 当前步骤在整个任务中的索引
	index: number;
	// 执行该步骤的 Agent 名称
	agent: string;
	// 步骤的可读名称
	label?: string;
	// 当前步骤所属的执行阶段
	phase?: string;
	// 该步骤对应的输出名称
	outputName?: string;
	// 是否要求结构化输出
	structured?: boolean;
	// 当前步骤状态，例如 queued、running、complete、failed
	status: AsyncJobStep["status"];
	// 活动状态，例如长时间运行或需要人工注意
	activityState?: ActivityState;
	// 最近一次活动时间
	lastActivityAt?: number;
	// 当前正在调用的 Tool
	currentTool?: string;
	// 当前 Tool 的参数摘要
	currentToolArgs?: string;
	// 当前 Tool 的开始时间
	currentToolStartedAt?: number;
	// 当前正在操作的路径
	currentPath?: string;
	// 最近完成的 Tool 调用记录
	recentTools?: Array<{ tool: string; args: string; endMs: number }>;
	// 最近产生的文本输出
	recentOutput?: string[];
	// 已执行的 LLM Turn 数
	turnCount?: number;
	// 已调用的 Tool 数
	toolCount?: number;
	// 父 Agent 发送的 steer 消息次数
	steerCount?: number;
	// 最近一次 steer 的时间
	lastSteerAt?: number;
	// 当前步骤的运行时长
	durationMs?: number;
	// 当前步骤的 Token 使用量
	tokens?: TokenUsage;
	// 当前步骤的模型调用费用
	totalCost?: CostSummary;
	// 当前步骤加载的 Skills
	skills?: string[];
	// 当前步骤使用的模型
	model?: string;
	// 当前步骤的 Thinking / Reasoning 等级
	thinking?: string;
	// 主模型失败时尝试过的模型列表
	attemptedModels?: string[];
	// 当前步骤的失败原因
	error?: string;
	// 是否因超时结束
	timedOut?: boolean;
	// 是否被主动停止
	stopped?: boolean;
	// 当前步骤的 Turn 预算状态
	turnBudget?: TurnBudgetState;
	// 是否超过 Turn 预算
	turnBudgetExceeded?: boolean;
	// 是否已要求 Agent 进入收尾阶段
	wrapUpRequested?: boolean;
	// 当前步骤派生的嵌套子任务
	children?: NestedRunSummary[];
}

/* 
AsyncRunSummary =
任务身份
+ 当前状态
+ 进度
+ 超时和预算
+ Chain/Parallel 步骤
+ Session 文件
+ Token/费用
+ 嵌套子任务 

从后台任务文件读取出来的运行摘要，主要从 status.json 读取
用于：
扫描任务目录
恢复 Session
判断任务是否仍在运行
保存完整 Chain/Parallel 状态
*/
export interface AsyncRunSummary {
	// 本次运行的唯一 ID
	id: string;
	// 该任务的后台状态目录
	asyncDir: string;
	// 所属父 Session
	sessionId?: string;
	// 运行状态
	state: "queued" | "running" | "complete" | "failed" | "paused" | "stopped";
	// 失败原因
	error?: string;
	// 长时间运行或需要人工注意
	activityState?: ActivityState;
	// 最后一次活动时间
	lastActivityAt?: number;
	// 当前正在使用的工具
	currentTool?: string;
	// 当前工具开始时间
	currentToolStartedAt?: number;
	// 当前操作的文件路径
	currentPath?: string;
	// LLM 已执行轮数
	turnCount?: number;
	// Tool 调用次数
	toolCount?: number;
	// 父 Agent 发送 steer 消息次数
	// 子 Agent 运行过程中，父 Agent 可以发送 steer 消息来调整方向
	steerCount?: number;
	// 最近一次 steer 时间
	lastSteerAt?: number;
	// 运行模式：Single、Parallel 或 Chain
	mode: SubagentRunMode;
	// 子 Agent 工作目录，通常是扩展的临时目录
	cwd?: string;
	// 任务开始时间
	startedAt: number;
	// 状态最后更新时间
	lastUpdate?: number;
	// 任务结束时间
	endedAt?: number;
	// 最大运行时间
	timeoutMs?: number;
	// 绝对截止时间
	deadlineAt?: number;
	// 是否因超时结束
	timedOut?: boolean;
	// 是否因用户停止
	stopped?: boolean;
	// Turn 预算和当前使用状态
	turnBudget?: TurnBudgetState;
	// 是否超过 Turn 上限
	turnBudgetExceeded?: boolean;
	// 是否已要求 Agent 收尾总结
	wrapUpRequested?: boolean;
 
	/* 
	例如：
	Chain:
	步骤 0：planner
	步骤 1、2、3：并行 worker
	步骤 4：reviewer

	这时：
	currentStep = 2;
	chainStepCount = 5;
	parallelGroups = [{ start: 1, count: 3 }]; 
	*/

	// 当前执行到第几个步骤
	currentStep?: number;
	// Chain 总步骤数
	chainStepCount?: number;
	// 等待追加到 Chain 的任务数量
	pendingAppends?: number;
	// Chain 中哪些步骤组成并行组
	parallelGroups?: AsyncParallelGroupStatus[];

	/* 
	step 是一次 Run 中可独立跟踪的最小执行单元，执行时通常会为该 step 启动独立的 Pi 子进程

	1. Single

	run-1
	└── step 0：worker 修复登录问题

	只有一个 step。

	2. Parallel

	run-1
	├── step 0：分析前端
	├── step 1：分析后端
	└── step 2：分析测试

	多个 step 同时执行，没有前后依赖。

	3. Chain

	run-1
	├── step 0：planner 制定方案
	├── step 1：worker 实现
	└── step 2：reviewer 检查

	多个 step 按顺序执行，后一步可以使用前一步结果。

	4. Chain + Parallel Group

	run-1
	├── step 0：planner 制定方案
	├── step 1：worker 修改前端 ┐
	├── step 2：worker 修改后端 ┘ 同时执行
	└── step 3：reviewer 检查

	index 是 step 在整个 Run 中的稳定编号，不一定表示依赖关系。

	AsyncRunStepSummary 就是记录每个 step 的：

	Agent、任务状态、当前 Tool、Token、耗时、输出和错误

	而 children 表示这个 step 内部又派生出的嵌套 Subagent Run。 
	*/
	// 每个子任务/步骤的状态摘要
	steps: AsyncRunStepSummary[];

	// 子 Agent Session 目录
	sessionDir?: string;
	// 最终输出文件
	outputFile?: string;
	// Input、Output 等 Token 总量
	totalTokens?: TokenUsage;
	// 模型调用总费用
	totalCost?: CostSummary;
	// 子 Agent JSONL Session 文件
	sessionFile?: string;

	// 当前任务派生的嵌套子任务
	nestedChildren?: NestedRunSummary[];
	// 嵌套任务恢复或解析时的警告
	nestedWarnings?: string[];
}

interface AsyncRunListOptions {
	states?: Array<AsyncRunSummary["state"]>;
	sessionId?: string;
	limit?: number;
	resultsDir?: string;
	kill?: (pid: number, signal?: NodeJS.Signals | 0) => boolean;
	now?: () => number;
	reconcile?: boolean;
}

function getErrorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

function isNotFoundError(error: unknown): boolean {
	return typeof error === "object"
		&& error !== null
		&& "code" in error
		&& (error as NodeJS.ErrnoException).code === "ENOENT";
}

function isAsyncRunDir(root: string, entry: string): boolean {
	const entryPath = path.join(root, entry);
	try {
		return fs.statSync(entryPath).isDirectory();
	} catch (error) {
		if (isNotFoundError(error)) return false;
		throw new Error(`Failed to inspect async run path '${entryPath}': ${getErrorMessage(error)}`, {
			cause: error instanceof Error ? error : undefined,
		});
	}
}

function outputFileMtime(outputFile: string | undefined): number | undefined {
	if (!outputFile) return undefined;
	try {
		return fs.statSync(outputFile).mtimeMs;
	} catch (error) {
		if (isNotFoundError(error)) return undefined;
		throw new Error(`Failed to inspect async output file '${outputFile}': ${getErrorMessage(error)}`, {
			cause: error instanceof Error ? error : undefined,
		});
	}
}

function deriveAsyncActivityState(asyncDir: string, status: AsyncStatus): { activityState?: ActivityState; lastActivityAt?: number } {
	if (status.state !== "running") return { activityState: status.activityState, lastActivityAt: status.lastActivityAt };
	const outputPath = status.outputFile ? (path.isAbsolute(status.outputFile) ? status.outputFile : path.join(asyncDir, status.outputFile)) : undefined;
	const currentStep = typeof status.currentStep === "number" ? status.steps?.[status.currentStep] : undefined;
	return {
		activityState: status.activityState,
		lastActivityAt: status.lastActivityAt ?? outputFileMtime(outputPath) ?? currentStep?.lastActivityAt ?? currentStep?.startedAt ?? status.startedAt,
	};
}

function statusToSummary(asyncDir: string, status: AsyncStatus & { cwd?: string }, nestedWarnings: string[] = [], nestedRoute?: NestedRoute): AsyncRunSummary {
	if (status.sessionId !== undefined && typeof status.sessionId !== "string") {
		throw new Error(`Invalid async status '${path.join(asyncDir, "status.json")}': sessionId must be a string.`);
	}
	const { activityState, lastActivityAt } = deriveAsyncActivityState(asyncDir, status);
	const steps = status.steps ?? [];
	const chainStepCount = status.chainStepCount ?? steps.length;
	const parallelGroups = normalizeParallelGroups(status.parallelGroups, steps.length, chainStepCount);
	let nestedChildren: NestedRunSummary[] = [];
	if (nestedWarnings.length === 0 && nestedRoute) {
		try {
			// The route is resolved by the caller via buildNestedRouteIndex, so this
			// avoids a fresh scan of the nested-events directory per run.
			nestedChildren = projectNestedEvents(nestedRoute)?.children ?? [];
		} catch (error) {
			nestedWarnings.push(`Nested status unavailable: ${getErrorMessage(error)}`);
		}
	}
	const summarizedSteps = steps.map((step, index) => {
		const stepActivityState = step.activityState;
		const stepLastActivityAt = step.lastActivityAt;
		return {
			index,
			agent: step.agent,
			...(step.label ? { label: step.label } : {}),
			...(step.phase ? { phase: step.phase } : {}),
			...(step.outputName ? { outputName: step.outputName } : {}),
			...(step.structured ? { structured: step.structured } : {}),
			status: step.status,
			...(stepActivityState ? { activityState: stepActivityState } : {}),
			...(stepLastActivityAt ? { lastActivityAt: stepLastActivityAt } : {}),
			...(step.currentTool ? { currentTool: step.currentTool } : {}),
			...(step.currentToolArgs ? { currentToolArgs: step.currentToolArgs } : {}),
			...(step.currentToolStartedAt ? { currentToolStartedAt: step.currentToolStartedAt } : {}),
			...(step.currentPath ? { currentPath: step.currentPath } : {}),
			...(step.recentTools ? { recentTools: step.recentTools.map((tool) => ({ ...tool })) } : {}),
			...(step.recentOutput ? { recentOutput: [...step.recentOutput] } : {}),
			...(step.turnCount !== undefined ? { turnCount: step.turnCount } : {}),
			...(step.toolCount !== undefined ? { toolCount: step.toolCount } : {}),
			...(step.steerCount !== undefined ? { steerCount: step.steerCount } : {}),
			...(step.lastSteerAt !== undefined ? { lastSteerAt: step.lastSteerAt } : {}),
			...(step.durationMs !== undefined ? { durationMs: step.durationMs } : {}),
			...(step.tokens ? { tokens: step.tokens } : {}),
			...(step.totalCost ? { totalCost: step.totalCost } : {}),
			...(step.skills ? { skills: step.skills } : {}),
			...(step.model ? { model: step.model } : {}),
			...(step.thinking ? { thinking: step.thinking } : {}),
			...(step.attemptedModels ? { attemptedModels: step.attemptedModels } : {}),
			...(step.error ? { error: step.error } : {}),
			...(step.timedOut !== undefined ? { timedOut: step.timedOut } : {}),
			...(step.stopped !== undefined ? { stopped: step.stopped } : {}),
			...(step.turnBudget ? { turnBudget: step.turnBudget } : {}),
			...(step.turnBudgetExceeded !== undefined ? { turnBudgetExceeded: step.turnBudgetExceeded } : {}),
			...(step.wrapUpRequested !== undefined ? { wrapUpRequested: step.wrapUpRequested } : {}),
			...(step.children?.length ? { children: step.children } : {}),
		};
	});
	attachRootChildrenToSteps(status.runId || path.basename(asyncDir), summarizedSteps, nestedChildren);
	return {
		id: status.runId || path.basename(asyncDir),
		asyncDir,
		...(status.sessionId ? { sessionId: status.sessionId } : {}),
		state: status.state,
		...(status.error ? { error: status.error } : {}),
		activityState,
		lastActivityAt,
		currentTool: status.currentTool,
		currentToolStartedAt: status.currentToolStartedAt,
		currentPath: status.currentPath,
		turnCount: status.turnCount,
		toolCount: status.toolCount,
		steerCount: status.steerCount,
		lastSteerAt: status.lastSteerAt,
		mode: status.mode,
		cwd: status.cwd,
		startedAt: status.startedAt,
		lastUpdate: status.lastUpdate,
		endedAt: status.endedAt,
		...(status.timeoutMs !== undefined ? { timeoutMs: status.timeoutMs } : {}),
		...(status.deadlineAt !== undefined ? { deadlineAt: status.deadlineAt } : {}),
		...(status.timedOut !== undefined ? { timedOut: status.timedOut } : {}),
		...(status.stopped !== undefined ? { stopped: status.stopped } : {}),
		...(status.turnBudget ? { turnBudget: status.turnBudget } : {}),
		...(status.turnBudgetExceeded !== undefined ? { turnBudgetExceeded: status.turnBudgetExceeded } : {}),
		...(status.wrapUpRequested !== undefined ? { wrapUpRequested: status.wrapUpRequested } : {}),
		currentStep: status.currentStep,
		...(status.chainStepCount !== undefined ? { chainStepCount: status.chainStepCount } : {}),
		...(status.pendingAppends !== undefined ? { pendingAppends: status.pendingAppends } : {}),
		...(parallelGroups.length ? { parallelGroups } : {}),
		steps: summarizedSteps,
		...(nestedChildren.length ? { nestedChildren } : {}),
		...(nestedWarnings.length ? { nestedWarnings } : {}),
		...(status.sessionDir ? { sessionDir: status.sessionDir } : {}),
		...(status.outputFile ? { outputFile: status.outputFile } : {}),
		...(status.totalTokens ? { totalTokens: status.totalTokens } : {}),
		...(status.totalCost ? { totalCost: status.totalCost } : {}),
		...(status.sessionFile ? { sessionFile: status.sessionFile } : {}),
	};
}

function sortRuns(runs: AsyncRunSummary[]): AsyncRunSummary[] {
	const rank = (state: AsyncRunSummary["state"]): number => {
		switch (state) {
			case "running": return 0;
			case "queued": return 1;
			case "failed": return 2;
			case "stopped": return 2;
			case "paused": return 2;
			case "complete": return 3;
		}
	};
	return [...runs].sort((a, b) => {
		const byState = rank(a.state) - rank(b.state);
		if (byState !== 0) return byState;
		const aTime = a.lastUpdate ?? a.endedAt ?? a.startedAt;
		const bTime = b.lastUpdate ?? b.endedAt ?? b.startedAt;
		return bTime - aTime;
	});
}

export function listAsyncRuns(asyncDirRoot: string, options: AsyncRunListOptions = {}): AsyncRunSummary[] {
	let entries: string[];
	try {
		entries = fs.readdirSync(asyncDirRoot).filter((entry) => isAsyncRunDir(asyncDirRoot, entry));
	} catch (error) {
		if (isNotFoundError(error)) return [];
		throw new Error(`Failed to list async runs in '${asyncDirRoot}': ${getErrorMessage(error)}`, {
			cause: error instanceof Error ? error : undefined,
		});
	}

	const allowedStates = options.states ? new Set(options.states) : undefined;
	const runs: AsyncRunSummary[] = [];
	// Route resolution for every run shares a single index built from the
	// nested-events directory, so the per-run lookup is O(1) instead of scanning
	// the directory once per run. The index is built lazily on first use, so
	// load-time restoration (which only wants queued/running runs) skips it
	// entirely when no active runs match.
	let nestedRouteIndex: Map<string, NestedRoute> | undefined;
	const resolveNestedRoute = (rootRunId: string): NestedRoute | undefined => {
		if (!nestedRouteIndex) nestedRouteIndex = buildNestedRouteIndex();
		return nestedRouteIndex.get(rootRunId);
	};
	for (const entry of entries) {
		const asyncDir = path.join(asyncDirRoot, entry);
		const reconciliation = options.reconcile === false
			? undefined
			: reconcileAsyncRun(asyncDir, { resultsDir: options.resultsDir, kill: options.kill, now: options.now });
		const status = (reconciliation?.status ?? readStatus(asyncDir)) as (AsyncStatus & { cwd?: string }) | null;
		if (!status) continue;
		// Filter before the nested-route lookup: the lookup builds an index over
		// the nested-events directory, so deferring it for filtered-out runs keeps
		// restoration at load from scanning that directory when no active runs
		// match.
		if (allowedStates && !allowedStates.has(status.state)) continue;
		if (options.sessionId && status.sessionId !== options.sessionId) continue;
		const nestedWarnings: string[] = [];
		let nestedRoute: NestedRoute | undefined;
		try {
			nestedRoute = resolveNestedRoute(status.runId || path.basename(asyncDir));
			if (nestedRoute) reconcileNestedAsyncDescendants(nestedRoute, { resultsDir: options.resultsDir, kill: options.kill, now: options.now });
		} catch (error) {
			nestedWarnings.push(`Nested status unavailable: ${getErrorMessage(error)}`);
		}
		const summary = statusToSummary(asyncDir, status, nestedWarnings, nestedRoute);
		runs.push(summary);
	}

	const sorted = sortRuns(runs);
	return options.limit !== undefined ? sorted.slice(0, options.limit) : sorted;
}

function formatActivityFacts(input: { activityState?: ActivityState; lastActivityAt?: number; currentTool?: string; currentToolStartedAt?: number; currentPath?: string; turnCount?: number; toolCount?: number; steerCount?: number; lastSteerAt?: number; turnBudget?: TurnBudgetState; turnBudgetExceeded?: boolean; wrapUpRequested?: boolean }): string | undefined {
	const facts: string[] = [];
	if (input.currentTool && input.currentToolStartedAt !== undefined) facts.push(`tool ${input.currentTool} ${formatDuration(Math.max(0, Date.now() - input.currentToolStartedAt))}`);
	else if (input.currentTool) facts.push(`tool ${input.currentTool}`);
	if (input.currentPath) facts.push(shortenPath(input.currentPath));
	if (input.turnCount !== undefined) facts.push(`${input.turnCount} turns`);
	if (input.turnBudgetExceeded && input.turnBudget) facts.push(`turn budget exceeded ${input.turnBudget.turnCount}/${input.turnBudget.maxTurns}+${input.turnBudget.graceTurns}`);
	else if (input.wrapUpRequested && input.turnBudget) facts.push(`wrap-up requested ${input.turnBudget.turnCount}/${input.turnBudget.maxTurns}`);
	else if (input.turnBudget) facts.push(`turn budget ${input.turnBudget.turnCount}/${input.turnBudget.maxTurns}+${input.turnBudget.graceTurns}`);
	if (input.toolCount !== undefined) facts.push(`${input.toolCount} tools`);
	if (input.steerCount !== undefined) facts.push(`${input.steerCount} steers`);
	if (typeof input.lastSteerAt === "number" && Number.isFinite(input.lastSteerAt)) facts.push(`last steer ${new Date(input.lastSteerAt).toISOString()}`);
	const activity = formatActivityLabel(input.lastActivityAt, input.activityState);
	return activity || facts.length ? [activity, ...facts].filter(Boolean).join(" | ") : undefined;
}

function formatStepLine(step: AsyncRunStepSummary): string {
	const display = step.label ? `${step.label} (${step.agent})` : step.agent;
	const phase = step.phase ? `[${step.phase}] ` : "";
	const parts = [`${step.index + 1}. ${phase}${display}`, step.status];
	const activity = formatActivityFacts(step);
	if (activity) parts.push(activity);
	const modelThinking = formatModelThinking(step.model, step.thinking);
	if (modelThinking) parts.push(modelThinking);
	if (step.durationMs !== undefined) parts.push(formatDuration(step.durationMs));
	if (step.tokens) parts.push(`${formatTokens(step.tokens.total)} tok`);
	return parts.join(" | ");
}

export function formatAsyncRunOutputPath(run: Pick<AsyncRunSummary, "asyncDir" | "outputFile">): string | undefined {
	if (!run.outputFile) return undefined;
	return path.isAbsolute(run.outputFile) ? run.outputFile : path.join(run.asyncDir, run.outputFile);
}

export function formatAsyncRunProgressLabel(run: Pick<AsyncRunSummary, "mode" | "state" | "currentStep" | "chainStepCount" | "parallelGroups" | "steps">): string {
	const stepCount = run.steps.length || 1;
	const chainStepCount = run.chainStepCount ?? stepCount;
	const groups = normalizeParallelGroups(run.parallelGroups, run.steps.length, chainStepCount);
	const activeGroup = run.currentStep !== undefined
		? groups.find((group) => run.currentStep! >= group.start && run.currentStep! < group.start + group.count)
		: undefined;
	if (activeGroup) {
		const groupSteps = run.steps.slice(activeGroup.start, activeGroup.start + activeGroup.count);
		const groupLabel = formatParallelOutcome(groupSteps, activeGroup.count, { showRunning: run.state === "running" });
		if (run.mode === "parallel") return groupLabel;
		return `step ${activeGroup.stepIndex + 1}/${chainStepCount} · parallel group: ${groupLabel}`;
	}
	if (run.mode === "parallel") return formatParallelOutcome(run.steps, stepCount, { showRunning: run.state === "running" });
	if (run.mode === "chain" && run.currentStep !== undefined && groups.length > 0) {
		const logicalStep = flatToLogicalStepIndex(run.currentStep, chainStepCount, groups);
		return `step ${logicalStep + 1}/${chainStepCount}`;
	}
	return run.currentStep !== undefined ? `step ${run.currentStep + 1}/${stepCount}` : `steps ${stepCount}`;
}

function formatRunHeader(run: AsyncRunSummary): string {
	const stepLabel = formatAsyncRunProgressLabel(run);
	const cwd = run.cwd ? shortenPath(run.cwd) : shortenPath(run.asyncDir);
	const activity = formatActivityFacts(run);
	const pending = run.pendingAppends ? ` | ${run.pendingAppends} pending append${run.pendingAppends === 1 ? "" : "s"}` : "";
	return `${run.id} | ${run.state}${activity ? ` | ${activity}` : ""} | ${run.mode} | ${stepLabel}${pending} | ${cwd}`;
}

export function formatAsyncRunList(runs: AsyncRunSummary[], heading = "Active async runs"): string {
	if (runs.length === 0) return `No ${heading.toLowerCase()}.`;

	const lines = [`${heading}: ${runs.length}`, ""];
	for (const run of runs) {
		lines.push(`- ${formatRunHeader(run)}`);
		for (const step of run.steps) {
			lines.push(`  ${formatStepLine(step)}`);
			lines.push(...formatNestedRunStatusLines(step.children, { indent: "    ", maxLines: 12 }));
		}
		const attached = new Set(run.steps.flatMap((step) => step.children?.map((child) => child.id) ?? []));
		const unattached = run.nestedChildren?.filter((child) => !attached.has(child.id)) ?? [];
		lines.push(...formatNestedRunStatusLines(unattached, { indent: "  ", maxLines: 12 }));
		if (run.error) lines.push(`  Error: ${run.error}`);
		for (const warning of run.nestedWarnings ?? []) lines.push(`  Warning: ${warning}`);
		const outputPath = formatAsyncRunOutputPath(run);
		if (outputPath) lines.push(`  output: ${shortenPath(outputPath)}`);
		if (run.sessionFile) lines.push(`  session: ${shortenPath(run.sessionFile)}`);
		lines.push("");
	}
	return lines.join("\n").trimEnd();
}
