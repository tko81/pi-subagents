/**
 * Async execution logic for subagent tool
 */

import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { AgentConfig } from "../../agents/agents.ts";
import { applyThinkingSuffix } from "../shared/pi-args.ts";
import { injectOutputPathSystemPrompt, injectSingleOutputInstruction, normalizeSingleOutputOverride, resolveSingleOutputPath, validateFileOnlyOutputMode } from "../shared/single-output.ts";
import { buildChainInstructions, isDynamicParallelStep, isParallelStep, resolveStepBehavior, suppressProgressForReadOnlyTask, writeInitialProgressFile, type ChainStep, type ResolvedStepBehavior, type SequentialStep, type StepOverrides } from "../../shared/settings.ts";
import type { RunnerStep } from "../shared/parallel-utils.ts";
import { resolvePiPackageRoot } from "../shared/pi-spawn.ts";
import { buildSkillInjection, normalizeSkillInput, resolveSkillsWithFallback } from "../../agents/skills.ts";
import { buildAgentMemoryInjection } from "../../agents/agent-memory.ts";
import { PI_CODING_AGENT_PACKAGE_ROOT_ENV, resolveChildCwd } from "../../shared/utils.ts";
import { buildModelCandidates, resolveModelCandidate, resolveSubagentModelOverride, type AvailableModelInfo, type ParentModel } from "../shared/model-fallback.ts";
import type { ModelScopeConfig } from "../shared/model-scope.ts";
import { resolveEffectiveThinking } from "../../shared/model-info.ts";
import { resolveExpectedWorktreeAgentCwd } from "../shared/worktree.ts";
import { buildWorkflowGraphSnapshot } from "../shared/workflow-graph.ts";
import { ChainOutputValidationError, validateChainOutputBindings } from "../shared/chain-outputs.ts";
import { createStructuredOutputRuntime } from "../shared/structured-output.ts";
import { resolveEffectiveAcceptance } from "../shared/acceptance.ts";
import {
	type AcceptanceInput,
	type ArtifactConfig,
	type Details,
	type MaxOutputConfig,
	type NestedRouteInfo,
	type ResolvedControlConfig,
	type ResolvedTurnBudget,
	type ResolvedToolBudget,
	type SubagentRunMode,
	ASYNC_DIR,
	RESULTS_DIR,
	SUBAGENT_ASYNC_STARTED_EVENT,
	SUBAGENT_LIFECYCLE_ARTIFACT_VERSION,
	TEMP_ROOT_DIR,
	getAsyncConfigPath,
	resolveChildMaxSubagentDepth,
} from "../../shared/types.ts";
import { nestedResultsPath, resolveInheritedNestedRouteFromEnv, resolveNestedParentAddressFromEnv, writeNestedEvent } from "../shared/nested-events.ts";
import { initialTurnBudgetState } from "../shared/turn-budget.ts";
import { validateToolBudgetConfig } from "../shared/tool-budget.ts";
import type { ImportedAsyncRoot } from "./chain-root-attachment.ts";

const require = createRequire(import.meta.url);
const piPackageRoot = resolvePiPackageRoot();

function resolveJitiCliFromPackageJson(packageJsonPath: string): string | undefined {
	if (!fs.existsSync(packageJsonPath)) return undefined;
	const packageRoot = path.dirname(packageJsonPath);
	const pkg = JSON.parse(fs.readFileSync(packageJsonPath, "utf-8")) as {
		bin?: string | Record<string, string>;
	};
	const binField = pkg.bin;
	const binPath = typeof binField === "string"
		? binField
		: binField?.jiti ?? Object.values(binField ?? {})[0];
	const candidates = [binPath, "lib/jiti-cli.mjs"].filter((candidate): candidate is string => Boolean(candidate));
	for (const candidate of candidates) {
		const cliPath = path.resolve(packageRoot, candidate);
		if (fs.existsSync(cliPath)) return cliPath;
	}
	return undefined;
}

function resolveJitiCliPath(): string | undefined {
	const candidates: Array<() => string | undefined> = [
		() => require.resolve("jiti/package.json"),
		() => piPackageRoot
			? createRequire(path.join(piPackageRoot, "package.json")).resolve("jiti/package.json")
			: undefined,
		() => {
			if (!process.argv[1]) return undefined;
			const piEntry = fs.realpathSync(process.argv[1]);
			return createRequire(piEntry).resolve("jiti/package.json");
		},
		() => piPackageRoot ? path.join(piPackageRoot, "node_modules", "jiti", "package.json") : undefined,
	];
	for (const candidate of candidates) {
		try {
			const packageJsonPath = candidate();
			if (!packageJsonPath) continue;
			const cliPath = resolveJitiCliFromPackageJson(packageJsonPath);
			if (cliPath) return cliPath;
		} catch {
			// Candidate not available in this install, continue probing.
		}
	}
	return undefined;
}

const jitiCliPath = resolveJitiCliPath();

interface AsyncExecutionContext {
	pi: ExtensionAPI;
	cwd: string;
	currentSessionId: string;
	/** Parent session id used by permission-system ask forwarding. */
	parentSessionId?: string;
	currentModelProvider?: string;
	currentModel?: ParentModel;
	/** Optional model-scope enforcement resolved from subagent settings. */
	modelScope?: ModelScopeConfig;
}

/*
 * executeAsyncChain 的完整输入快照。
 * runAsyncPath 在父 Pi 进程中组装这些字段，executeAsyncChain 将其编译为 RunnerStep 和 SubagentRunConfig，
 * detached subagent-runner 随后只依赖这份快照执行，不再读取父 Agent 当前 turn 中的临时状态。
 */
interface AsyncChainParams {
	// 用户层执行计划。元素可以是串行步骤、静态并行组或动态 fanout 组。
	chain: ChainStep[];
	// 整条链的原始任务文本，用于补齐首步 Prompt、进度说明和只读任务判断。
	task?: string;
	/*
	 * 把一个已经存在的异步根任务接到当前 chain 最前面。
	 * Runner 不会重新执行该任务，而是等待其 resultPath，并把结果保存为可选 outputName 供后续步骤引用。
	 */
	attachRoot?: ImportedAsyncRoot & { agent: string; outputName?: string; label?: string };
	// 决定状态和返回值展示为 chain 还是 parallel；single 不走此接口。
	resultMode?: Exclude<SubagentRunMode, "single">;
	// 当前作用域发现的全部 Agent 角色，构建每个 RunnerStep 时按名称查找角色配置。
	agents: AgentConfig[];
	// 父 Pi 的会话、模型和 ExtensionAPI 环境，用于继承默认模型并发布异步启动事件。
	ctx: AsyncExecutionContext;
	// 当前可用模型目录，用于解析角色模型、显式覆盖和 fallback 候选。
	availableModels?: AvailableModelInfo[];
	// 后台 Runner 的目标工作目录；未指定时由 ctx.cwd 继承。
	cwd?: string;
	// 父 Agent 最终接收的汇总文本截断规则；完整输出仍可保存在 Artifact。
	maxOutput?: MaxOutputConfig;
	// 输入、输出、metadata、transcript 和 progress 等 Artifact 的根目录。
	artifactsDir?: string;
	// 控制 Artifact 是否启用，以及具体保存哪些种类的文件。
	artifactConfig: ArtifactConfig;
	// 是否在运行结束后导出最后会话并创建可分享链接。
	shareEnabled: boolean;
	// 整场后台运行的会话根目录；每个异步 run 和 flat step 会在其下继续分目录。
	sessionRoot?: string;
	// 需要注入整条 chain 中各 Agent 的公共 Skill 名称。
	chainSkills?: string[];
	/*
	 * 按 flat step 索引提供会话文件。静态 parallel 会占多个索引，动态 fanout 会按最大展开数预留索引。
	 * 某项为 undefined 表示该步骤创建 fresh session；有路径通常表示使用父会话 fork 出来的 session。
	 */
	sessionFilesByFlatIndex?: (string | undefined)[];
	// 与 flat step 一一对应的 thinking 覆盖，通常从 fork 会话中恢复每个步骤原有的 thinking level。
	thinkingOverridesByFlatIndex?: (AgentConfig["thinking"] | undefined)[];
	// progress.md 的显式目录；未传时根据 Artifact、parallel 模式或 Runner cwd 推导。
	progressDir?: string;
	// 动态 fanout 未在步骤上声明 maxItems 时使用的全局最大展开数量。
	dynamicFanoutMaxItems?: number;
	// 当前任务树允许的最大子 Agent 深度，将传给每个叶子步骤用于限制继续派生。
	maxSubagentDepth: number;
	// 创建 parallel worktree 后执行的可选初始化脚本，例如安装依赖或准备构建环境。
	worktreeSetupHook?: string;
	// worktree 初始化脚本的最长运行时间。
	worktreeSetupHookTimeoutMs?: number;
	// Git worktree 的自定义存放根目录。
	worktreeBaseDir?: string;
	// 长时间运行、无活动和连续失败等控制面检测与通知策略。
	controlConfig?: ResolvedControlConfig;
	// Runner 向父编排器发送控制通知和最终消息时使用的 intercom 地址。
	controlIntercomTarget?: string;
	// 根据 Agent 名称和 flat index 为每个叶子子 Agent 生成唯一 intercom 地址。
	childIntercomTarget?: (agent: string, index: number) => string | undefined;
	// 嵌套任务树的事件与控制路由，使祖先能够观察和控制该后台 run 及其后代。
	nestedRoute?: NestedRouteInfo;
	/*
	 * 预留的链级验收输入。当前 executeAsyncChain 没有读取该字段；
	 * 实际 chain 验收来自各 ChainStep/parallel group 自身的 acceptance 配置。
	 */
	acceptance?: AcceptanceInput;
	// 整场后台运行的墙钟超时，启动时还会转换为绝对 deadlineAt。
	timeoutMs?: number;
	// 整场运行的 LLM turn 预算，Runner 会跟踪并在达到硬限制时终止当前子进程。
	turnBudget?: ResolvedTurnBudget;
	/*
	 * 本次 subagent 调用显式指定的默认工具预算。
	 * 优先级为 ChainStep > 本字段 > Agent 配置 > configToolBudget。
	 */
	toolBudget?: ResolvedToolBudget;
	// 扩展全局配置中的工具预算，只有步骤、调用和 Agent 都没指定时才作为最后 fallback。
	configToolBudget?: ResolvedToolBudget;
	// 整场异步运行同时执行的子 Agent 总上限，多个 parallel group 共享这一限制。
	globalConcurrencyLimit?: number;
}

interface AsyncSingleParams {
	agent: string;
	task?: string;
	agentConfig: AgentConfig;
	ctx: AsyncExecutionContext;
	cwd?: string;
	maxOutput?: MaxOutputConfig;
	artifactsDir?: string;
	artifactConfig: ArtifactConfig;
	shareEnabled: boolean;
	sessionRoot?: string;
	sessionFile?: string;
	skills?: string[];
	output?: string | boolean;
	outputMode?: "inline" | "file-only";
	outputBaseDir?: string;
	modelOverride?: string;
	thinkingOverride?: AgentConfig["thinking"];
	availableModels?: AvailableModelInfo[];
	maxSubagentDepth: number;
	worktreeSetupHook?: string;
	worktreeSetupHookTimeoutMs?: number;
	worktreeBaseDir?: string;
	controlConfig?: ResolvedControlConfig;
	controlIntercomTarget?: string;
	childIntercomTarget?: (agent: string, index: number) => string | undefined;
	nestedRoute?: NestedRouteInfo;
	acceptance?: AcceptanceInput;
	timeoutMs?: number;
	turnBudget?: ResolvedTurnBudget;
	toolBudget?: ResolvedToolBudget;
	configToolBudget?: ResolvedToolBudget;
}

interface AsyncExecutionResult {
	content: Array<{ type: "text"; text: string }>;
	details: Details;
	isError?: boolean;
}

export interface AsyncRunnerStepBuildParams {
	chain: ChainStep[];
	task?: string;
	attachRoot?: ImportedAsyncRoot & { agent: string; outputName?: string; label?: string };
	resultMode?: SubagentRunMode;
	agents: AgentConfig[];
	ctx: AsyncExecutionContext;
	availableModels?: AvailableModelInfo[];
	cwd?: string;
	chainSkills?: string[];
	sessionFilesByFlatIndex?: (string | undefined)[];
	thinkingOverridesByFlatIndex?: (AgentConfig["thinking"] | undefined)[];
	progressDir?: string;
	dynamicFanoutMaxItems?: number;
	maxSubagentDepth: number;
	worktreeBaseDir?: string;
	asyncDir: string;
	outputBaseDir?: string;
	validateOutputBindings?: boolean;
	toolBudget?: ResolvedToolBudget;
	configToolBudget?: ResolvedToolBudget;
}

export type AsyncRunnerStepBuildResult =
	| {
		steps: RunnerStep[];
		runnerCwd: string;
		workflowGraph: ReturnType<typeof buildWorkflowGraphSnapshot>;
		eventChain: ChainStep[];
		originalTask?: string;
	}
	| { error: string };

export function formatAsyncStartedMessage(headline: string): string {
	return [
		headline,
		"",
		"The async run is detached. Do not run sleep timers or polling loops just to wait for it.",
		"If you have independent work, continue that work. When you have nothing left to do until the async result arrives, call wait() — it blocks until the run finishes and delivers the completion here. Only if you are certain you will get another turn (an interactive session where the user will prompt you again) can you instead stop and let Pi wake you; inside a skill that must run to completion, or in a non-interactive run, there is no next turn, so use wait().",
		"Use subagent({ action: \"status\", id: \"...\" }) when you need a one-shot status/result or to inspect a blocked/stale run. To block until completion, prefer wait(). Do not poll in a loop just to wait.",
	].join("\n");
}

/**
 * Check if jiti is available for async execution
 */
export function isAsyncAvailable(): boolean {
	return jitiCliPath !== undefined;
}

function isNodeExecutableName(execPath: string): boolean {
	const basename = path.basename(execPath).toLowerCase();
	return basename === "node" || basename === "node.exe" || basename === "nodejs" || basename === "nodejs.exe";
}

function canUseCurrentNodeExecutable(execPath: string): boolean {
	try {
		fs.accessSync(execPath, process.platform === "win32" ? fs.constants.F_OK : fs.constants.X_OK);
		return true;
	} catch {
		return false;
	}
}

function resolveAsyncRunnerNodeCommand(): string {
	if (isNodeExecutableName(process.execPath) && canUseCurrentNodeExecutable(process.execPath)) {
		return process.execPath;
	}
	return process.platform === "win32" ? "node.exe" : "node";
}

export function resolveAsyncRunnerLogPaths(cfg: object): { stdoutPath: string; stderrPath: string } | undefined {
	const asyncDir = typeof (cfg as { asyncDir?: unknown }).asyncDir === "string"
		? (cfg as { asyncDir: string }).asyncDir
		: undefined;
	if (!asyncDir) return undefined;
	return {
		stdoutPath: path.join(asyncDir, "runner.stdout.log"),
		stderrPath: path.join(asyncDir, "runner.stderr.log"),
	};
}

function closeFd(fd: number | undefined): void {
	if (fd === undefined) return;
	try {
		fs.closeSync(fd);
	} catch {
		// Best-effort cleanup; child process already owns its duplicated stdio fd.
	}
}

/*
 * 创建并脱离一个后台 Runner 进程。
 * 父进程先把完整配置写入临时 JSON，再用 Node + jiti 执行 subagent-runner.ts；成功后只返回 pid，
 * 不等待 Runner 结束。Runner 会自行读取并删除配置文件、执行任务、更新状态并写入最终结果。
 */
function spawnRunner(cfg: object, suffix: string, cwd: string): { pid?: number; error?: string } {
	// jiti 负责让普通 Node 进程直接加载 TypeScript Runner；没有它就无法启动源码形式的后台入口。
	if (!jitiCliPath) {
		return { error: "upstream jiti for TypeScript execution could not be found; ensure package dependencies are installed" };
	}

	// spawn 的 cwd 必须真实存在且是目录，提前检查能返回比底层 ENOENT 更明确的错误。
	try {
		const cwdStats = fs.statSync(cwd);
		if (!cwdStats.isDirectory()) {
			return { error: `cwd is not a directory: ${cwd}` };
		}
	} catch {
		return { error: `cwd does not exist: ${cwd}` };
	}

	/*
	 * 后台配置不能通过闭包传递，因为新 Runner 是独立 OS 进程。
	 * 这里把 cfg 序列化到用户级临时目录，并把路径作为命令行参数交给 Runner；
	 * suffix 通常是 runId，用来避免多个并发后台任务覆盖同一配置文件。
	 */
	fs.mkdirSync(TEMP_ROOT_DIR, { recursive: true });
	const cfgPath = getAsyncConfigPath(suffix);
	fs.writeFileSync(cfgPath, JSON.stringify(cfg));
	// Runner 源码与本文件同包发布；jiti 在子进程启动时完成 TypeScript 转译和加载。
	const runner = path.join(path.dirname(fileURLToPath(import.meta.url)), "subagent-runner.ts");
	// 优先复用当前可执行的 Node，特殊宿主环境中再回退到 PATH 里的 node/node.exe。
	const nodeCommand = resolveAsyncRunnerNodeCommand();

	/*
	 * detached 进程不能继续把 stdout/stderr 接到父 Pi 的终端，否则父进程退出后管道可能断开或相互阻塞。
	 * 有 asyncDir 时把两个流追加到 runner.stdout.log 和 runner.stderr.log；没有时丢弃输出。
	 */
	const logPaths = resolveAsyncRunnerLogPaths(cfg);
	let stdoutFd: number | undefined;
	let stderrFd: number | undefined;
	try {
		if (logPaths) {
			// 父进程先打开文件描述符，spawn 时 Node 会把它们复制给子进程作为 stdout/stderr。
			fs.mkdirSync(path.dirname(logPaths.stdoutPath), { recursive: true });
			stdoutFd = fs.openSync(logPaths.stdoutPath, "a");
			stderrFd = fs.openSync(logPaths.stderrPath, "a");
		}
		/*
		 * detached=true 让 Runner 成为独立进程组；stdin 必须忽略，stdout/stderr 指向日志文件。
		 * 只额外注入 Pi 包根目录，使 Runner 能找到与父进程相同的 coding-agent 安装；
		 * 其余环境变量原样继承，包括认证、嵌套路由和子 Agent 标记。
		 */
		const proc = spawn(nodeCommand, [jitiCliPath, runner, cfgPath], {
			cwd,
			detached: true,
			// 决定子进程的 stdin、stdout、stderr 指向哪里，ignore 表示忽略，stdoutFd 和 stderrFd 表示指向文件描述符
			stdio: ["ignore", stdoutFd ?? "ignore", stderrFd ?? "ignore"],
			windowsHide: true,
			env: {
				...process.env,
				...(piPackageRoot ? { [PI_CODING_AGENT_PACKAGE_ROOT_ENV]: piPackageRoot } : {}),
			},
		});
		/*
		 * spawn 已把 FD 复制给子进程，父进程必须立即关闭自己的副本。
		 * 否则日志文件会多一个持有者，还可能让文件描述符随多次异步启动持续泄漏。
		 */
		closeFd(stdoutFd);
		closeFd(stderrFd);
		// 这是 spawn 后的异步错误，只能记日志；同步启动错误会进入下面 catch 并返回给调用方。
		// 是在给子进程（proc）注册一个事件监听器，用于捕获子进程在启动或运行过程中发生的异步错误。它是 Node.js 中处理子进程异常的标准方式
		proc.on("error", (error) => {
			console.error(`[pi-subagents] async spawn failed: ${error.message}`);
		});
		// 没有 pid 就没有可供 status/wait/stop 定位的后台任务，因此启动必须判定为失败。
		if (typeof proc.pid !== "number") {
			return { error: `async runner did not produce a pid for cwd: ${cwd}` };
		}
		/*
		 * unref() 移除父 Node 事件循环对该 child 进程的引用
		 * 父 Pi 不需要等待 Runner，可以继续当前 turn 或正常退出；后台 Runner 仍独立运行到任务结束
		 * 
		 * 如果不调用 unref()：父 Pi 想退出 → Node 发现子进程句柄还存在 → 可能无法正常退出
		 */
		proc.unref();
		// 返回子进程的 pid，表示启动成功
		return { pid: proc.pid };
	} catch (error) {
		// 无论失败发生在打开日志还是 spawn，都关闭父进程已经打开的 FD，并把错误转成返回值。
		closeFd(stdoutFd);
		closeFd(stderrFd);
		return { error: error instanceof Error ? error.message : String(error) };
	}
}

function formatAsyncStartError(mode: SubagentRunMode, message: string): AsyncExecutionResult {
	return {
		content: [{ type: "text", text: message }],
		isError: true,
		details: { mode, results: [] },
	};
}

const UNAVAILABLE_SUBAGENT_SKILL_ERROR = "Skills not found: pi-subagents";

class UnavailableSubagentSkillError extends Error {}
class AsyncStartValidationError extends Error {}

export function buildAsyncRunnerSteps(id: string, params: AsyncRunnerStepBuildParams): AsyncRunnerStepBuildResult {
	const {
		chain,
		agents,
		ctx,
		cwd,
		sessionFilesByFlatIndex,
		thinkingOverridesByFlatIndex,
		maxSubagentDepth,
		worktreeBaseDir,
		asyncDir,
	} = params;
	const outputBaseDir = params.outputBaseDir;
	const resultMode = params.resultMode ?? "chain";
	const chainSkills = params.chainSkills ?? [];
	const availableModels = params.availableModels;
	const runnerCwd = resolveChildCwd(ctx.cwd, cwd);
	const progressDir = params.progressDir ?? runnerCwd;
	const graphChain: ChainStep[] = params.attachRoot
		? [{
				agent: params.attachRoot.agent,
				task: `Attach async root ${params.attachRoot.runId}`,
				label: params.attachRoot.label ?? `Attached root ${params.attachRoot.runId}`,
				...(params.attachRoot.outputName ? { as: params.attachRoot.outputName } : {}),
			}, ...chain]
		: chain;
	const firstStep = chain[0];
	const originalTask = params.task ?? (firstStep
		? (isParallelStep(firstStep)
			? firstStep.parallel[0]?.task
			: isDynamicParallelStep(firstStep)
				? firstStep.parallel.task
				: (firstStep as SequentialStep).task)
		: undefined);
	try {
		if (params.validateOutputBindings !== false) {
			validateChainOutputBindings(chain, { maxItems: params.dynamicFanoutMaxItems });
		}
	} catch (error) {
		if (error instanceof ChainOutputValidationError) return { error: error.message };
		throw error;
	}
	const workflowGraph = buildWorkflowGraphSnapshot({ runId: id, mode: resultMode, steps: graphChain });

	for (const s of chain) {
		const stepAgents = isParallelStep(s)
			? s.parallel.map((t) => t.agent)
			: isDynamicParallelStep(s)
				? [s.parallel.agent]
				: [(s as SequentialStep).agent];
		for (const agentName of stepAgents) {
			if (!agents.find((x) => x.name === agentName)) {
				return { error: `Unknown agent: ${agentName}` };
			}
		}
	}

	let progressInstructionCreated = false;
	const buildStepOverrides = (s: SequentialStep): StepOverrides => {
		const stepSkillInput = normalizeSkillInput(s.skill);
		return {
			...(s.output !== undefined ? { output: s.output } : {}),
			...(s.outputMode !== undefined ? { outputMode: s.outputMode } : {}),
			...(s.reads !== undefined ? { reads: s.reads } : {}),
			...(s.progress !== undefined ? { progress: s.progress } : {}),
			...(stepSkillInput !== undefined ? { skills: stepSkillInput } : {}),
			...(s.model ? { model: s.model } : {}),
		};
	};
	const buildSeqStep = (s: SequentialStep, sessionFile?: string, behaviorCwd?: string, progressPrecreated = false, resolvedBehavior?: ResolvedStepBehavior, flatIndex?: number) => {
		const a = agents.find((x) => x.name === s.agent)!;
		const toolBudgetInput = s.toolBudget ?? params.toolBudget ?? a.toolBudget ?? params.configToolBudget;
		const resolvedToolBudget = validateToolBudgetConfig(toolBudgetInput, s.toolBudget ? "toolBudget" : a.toolBudget ? "agent.toolBudget" : "config.toolBudget");
		if (resolvedToolBudget.error) throw new AsyncStartValidationError(resolvedToolBudget.error);
		const stepCwd = resolveChildCwd(runnerCwd, s.cwd);
		const instructionCwd = behaviorCwd ?? stepCwd;
		const behavior = suppressProgressForReadOnlyTask(resolvedBehavior ?? resolveStepBehavior(a, buildStepOverrides(s), chainSkills), s.task, originalTask);
		const skillNames = behavior.skills === false ? [] : behavior.skills;
		const { resolved: resolvedSkills, missing: missingSkills } = resolveSkillsWithFallback(skillNames, stepCwd, ctx.cwd);
		if (missingSkills.includes("pi-subagents")) throw new UnavailableSubagentSkillError(UNAVAILABLE_SUBAGENT_SKILL_ERROR);

		let systemPrompt = a.systemPrompt?.trim() ?? "";
		if (resolvedSkills.length > 0) {
			const injection = buildSkillInjection(resolvedSkills);
			systemPrompt = systemPrompt ? `${systemPrompt}\n\n${injection}` : injection;
		}
		const memoryInjection = buildAgentMemoryInjection(a, stepCwd);
		if (memoryInjection) {
			systemPrompt = systemPrompt ? `${systemPrompt}\n\n${memoryInjection}` : memoryInjection;
		}

		const readInstructions = buildChainInstructions({ ...behavior, output: false, progress: false }, instructionCwd, false);
		const isFirstProgressAgent = behavior.progress && !progressPrecreated && !progressInstructionCreated;
		if (behavior.progress) progressInstructionCreated = true;
		const progressInstructions = buildChainInstructions({ ...behavior, output: false, reads: false }, progressDir, isFirstProgressAgent);
		const outputPath = resolveSingleOutputPath(behavior.output, ctx.cwd, instructionCwd, outputBaseDir);
		systemPrompt = injectOutputPathSystemPrompt(systemPrompt, outputPath);
		const validationError = validateFileOnlyOutputMode(behavior.outputMode, outputPath, `Async step (${s.agent})`);
		if (validationError) throw new AsyncStartValidationError(validationError);
		let taskTemplate = s.task ?? "{previous}";
		taskTemplate = taskTemplate.replace(/\{task\}/g, originalTask ?? "");
		taskTemplate = taskTemplate.replace(/\{chain_dir\}/g, runnerCwd);
		const task = injectSingleOutputInstruction(`${readInstructions.prefix}${taskTemplate}${progressInstructions.suffix}`, outputPath);

		const requestedModel = behavior.model ?? a.model;
		const primaryModel = resolveSubagentModelOverride(requestedModel, ctx.currentModel, availableModels, ctx.currentModelProvider, { scope: ctx.modelScope, source: behavior.model ? "explicit" : "inherited" });
		const thinkingOverride = flatIndex === undefined ? undefined : thinkingOverridesByFlatIndex?.[flatIndex];
		const effectiveThinking = thinkingOverride ?? a.thinking;
		const model = applyThinkingSuffix(primaryModel, effectiveThinking, thinkingOverride !== undefined);
		return {
			parentSessionId: ctx.parentSessionId ?? ctx.currentSessionId,
			agent: s.agent,
			task,
			phase: s.phase,
			label: s.label,
			outputName: s.as,
			structured: Boolean(s.outputSchema),
			cwd: stepCwd,
			model,
			thinking: resolveEffectiveThinking(model, effectiveThinking),
			modelCandidates: buildModelCandidates(primaryModel, a.fallbackModels, availableModels, ctx.currentModelProvider, { scope: ctx.modelScope }).map((candidate) =>
				applyThinkingSuffix(candidate, effectiveThinking, thinkingOverride !== undefined),
			),
			tools: a.tools,
			extensions: a.extensions,
			subagentOnlyExtensions: a.subagentOnlyExtensions,
			mcpDirectTools: a.mcpDirectTools,
			completionGuard: a.completionGuard,
			systemPrompt,
			systemPromptMode: a.systemPromptMode,
			inheritProjectContext: a.inheritProjectContext,
			inheritSkills: a.inheritSkills,
			skills: resolvedSkills.map((r) => r.name),
			outputPath,
			outputMode: behavior.outputMode,
			sessionFile,
			maxSubagentDepth: resolveChildMaxSubagentDepth(maxSubagentDepth, a.maxSubagentDepth),
			effectiveAcceptance: resolveEffectiveAcceptance({
				explicit: s.acceptance,
				agentName: s.agent,
				task: s.task,
				mode: resultMode,
				async: true,
				dynamic: false,
			}),
			...(s.outputSchema ? { structuredOutputSchema: s.outputSchema } : {}),
			...(s.outputSchema ? { structuredOutput: createStructuredOutputRuntime(s.outputSchema, path.join(asyncDir, "structured-output")) } : {}),
			...(resolvedToolBudget.budget ? { toolBudget: resolvedToolBudget.budget } : {}),
		};
	};

	let flatStepIndex = 0;
	const nextFlatStep = (): { index: number; sessionFile?: string; thinkingOverride?: AgentConfig["thinking"] } => {
		const index = flatStepIndex;
		const sessionFile = sessionFilesByFlatIndex?.[flatStepIndex];
		const thinkingOverride = thinkingOverridesByFlatIndex?.[flatStepIndex];
		flatStepIndex++;
		return {
			index,
			...(sessionFile ? { sessionFile } : {}),
			...(thinkingOverride ? { thinkingOverride } : {}),
		};
	};

	try {
		const builtSteps = chain.map((s, stepIndex) => {
			if (isParallelStep(s)) {
				const parallelBehaviors = s.parallel.map((task) => {
					const agent = agents.find((candidate) => candidate.name === task.agent)!;
					return suppressProgressForReadOnlyTask(resolveStepBehavior(agent, buildStepOverrides(task), chainSkills), task.task, originalTask);
				});
				const progressPrecreated = parallelBehaviors.some((behavior) => behavior.progress);
				if (progressPrecreated) {
					if (!s.worktree || params.progressDir) writeInitialProgressFile(progressDir);
					progressInstructionCreated = true;
				}
				return {
					parallel: s.parallel.map((t, taskIndex) => {
						let behaviorCwd: string | undefined;
						if (s.worktree) {
							try {
								behaviorCwd = resolveExpectedWorktreeAgentCwd(runnerCwd, `${id}-s${stepIndex}`, taskIndex, worktreeBaseDir);
							} catch {
								behaviorCwd = undefined;
							}
						}
						const staticStep = nextFlatStep();
						return buildSeqStep(t, staticStep.sessionFile, behaviorCwd, progressPrecreated, parallelBehaviors[taskIndex], staticStep.index);
					}),
					concurrency: s.concurrency,
					failFast: s.failFast,
					worktree: s.worktree,
				};
			}
			if (isDynamicParallelStep(s)) {
				const agent = agents.find((candidate) => candidate.name === s.parallel.agent)!;
				const behavior = suppressProgressForReadOnlyTask(resolveStepBehavior(agent, buildStepOverrides(s.parallel), chainSkills), s.parallel.task, originalTask);
				const progressPrecreated = behavior.progress;
				if (progressPrecreated) {
					writeInitialProgressFile(progressDir);
					progressInstructionCreated = true;
				}
				const maxItems = s.expand.maxItems ?? params.dynamicFanoutMaxItems ?? 0;
				const dynamicFlatSteps = Array.from({ length: maxItems }, () => nextFlatStep());
				return {
					expand: s.expand,
					parallel: buildSeqStep(s.parallel as SequentialStep, undefined, undefined, progressPrecreated, behavior),
					collect: s.collect,
					concurrency: s.concurrency,
					failFast: s.failFast,
					phase: s.phase,
					label: s.label,
					sessionFiles: dynamicFlatSteps.map((step) => step.sessionFile),
					thinkingOverrides: dynamicFlatSteps.map((step) => step.thinkingOverride),
					effectiveAcceptance: resolveEffectiveAcceptance({
						explicit: s.acceptance,
						agentName: s.parallel.agent,
						task: s.parallel.task,
						mode: resultMode,
						async: true,
						dynamicGroup: true,
					}),
				};
			}
			const staticStep = nextFlatStep();
			return buildSeqStep(s as SequentialStep, staticStep.sessionFile, undefined, false, undefined, staticStep.index);
		});
		const steps = params.attachRoot
			? [{
					agent: params.attachRoot.agent,
					task: "",
					label: params.attachRoot.label ?? `Attached root ${params.attachRoot.runId}`,
					outputName: params.attachRoot.outputName,
					importAsyncRoot: {
						runId: params.attachRoot.runId,
						asyncDir: params.attachRoot.asyncDir,
						resultPath: params.attachRoot.resultPath,
						index: params.attachRoot.index,
					},
					inheritProjectContext: false,
					inheritSkills: false,
				}, ...builtSteps]
			: builtSteps;
		return { steps, runnerCwd, workflowGraph, eventChain: graphChain, ...(originalTask !== undefined ? { originalTask } : {}) };
	} catch (error) {
		if (error instanceof UnavailableSubagentSkillError || error instanceof AsyncStartValidationError) return { error: error.message };
		throw error;
	}
}

/*
 * 把一条 chain 转换成后台 Runner 可执行的配置，并启动 detached Runner 进程。
 * 该函数只负责“编译计划 + 启动 + 注册”：不会等待 chain 中任何子 Agent 完成；启动成功后立即把
 * runId、asyncDir 和 workflowGraph 返回父 Agent，真正执行由 subagent-runner.ts 中的 runSubagent 完成。
 */
export function executeAsyncChain(
	id: string,
	params: AsyncChainParams,
): AsyncExecutionResult {
	/*
	 * id 由 runAsyncPath 预先生成，是整场后台运行的唯一身份。
	 * params 已包含标准化后的 chain、角色、模型、会话、Artifact、控制通道和预算；
	 * 这里先取出启动 Runner 时最常用的字段，其他选项仍通过 params 读取。
	 */
	const {
		chain,
		agents,
		ctx,
		cwd,
		maxOutput,
		artifactsDir,
		artifactConfig,
		shareEnabled,
		sessionRoot,
		sessionFilesByFlatIndex,
		thinkingOverridesByFlatIndex,
		maxSubagentDepth,
		worktreeSetupHook,
		worktreeSetupHookTimeoutMs,
		worktreeBaseDir,
		controlConfig,
		controlIntercomTarget,
		childIntercomTarget,
		nestedRoute,
	} = params;
	// tasks 并行简写也复用本函数，因此 resultMode 可能是 parallel；默认才是 chain。
	const resultMode = params.resultMode ?? "chain";
	/*
	 * 顶层异步任务写入公共 ASYNC_DIR；若当前代码本身运行在子 Agent 中，则继承 nested route，
	 * 把目录放到对应 rootRunId 下。这样祖先 watcher 能定位孙级任务，同时避免不同任务树互相混淆。
	 */
	const inheritedNestedRoute = resolveInheritedNestedRouteFromEnv();
	const nestedAddress = inheritedNestedRoute ? resolveNestedParentAddressFromEnv() : undefined;
	const asyncDir = inheritedNestedRoute
		? path.join(TEMP_ROOT_DIR, "nested-subagent-runs", inheritedNestedRoute.rootRunId, id)
		: path.join(ASYNC_DIR, id);
	// asyncDir 是 status、events、stdout/stderr 和每步输出的根目录，必须在 spawn 前创建成功。
	try {
		fs.mkdirSync(asyncDir, { recursive: true });
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		return {
			content: [{ type: "text", text: `Failed to create async run directory '${asyncDir}': ${message}` }],
			isError: true,
			details: { mode: resultMode, results: [] },
		};
	}

	/*
	 * buildAsyncRunnerSteps 是“编译阶段”。它把用户层 ChainStep 转成 RunnerStep，并完成角色解析、
	 * Prompt/Skill/Memory 注入、模型 fallback、fresh/fork session、输出路径、验收策略、worktree 预期目录
	 * 和 workflowGraph 构建。此时尚未创建 detached Runner，也没有调用任何 LLM。
	 */
	const built = buildAsyncRunnerSteps(id, {
		chain,
		task: params.task,
		attachRoot: params.attachRoot,
		resultMode,
		agents,
		ctx,
		availableModels: params.availableModels,
		cwd,
		chainSkills: params.chainSkills,
		sessionFilesByFlatIndex,
		thinkingOverridesByFlatIndex,
		progressDir: params.progressDir ?? (artifactsDir ? path.join(artifactsDir, "progress", id) : resultMode === "parallel" ? path.join(asyncDir, "progress") : undefined),
		outputBaseDir: artifactsDir ? path.join(artifactsDir, "outputs", id) : undefined,
		dynamicFanoutMaxItems: params.dynamicFanoutMaxItems,
		maxSubagentDepth,
		worktreeBaseDir,
		asyncDir,
		toolBudget: params.toolBudget,
		configToolBudget: params.configToolBudget,
	});
	if ("error" in built) {
		// 编译失败时 Runner 从未启动，删除刚创建的空运行目录，不给 watcher 留下幽灵任务。
		try {
			fs.rmSync(asyncDir, { recursive: true, force: true });
		} catch {
			// Best-effort cleanup for validation failures before the runner is spawned.
		}
		return formatAsyncStartError(resultMode, built.error);
	}
	// steps 给 Runner 执行；eventChain 保留用户可理解的结构，供启动事件和 TUI 摘要使用。
	const { steps, runnerCwd, workflowGraph, eventChain } = built;
	// deadlineAt 使用绝对时间，Runner 即使稍后启动也不会重新获得一整段 timeout。
	const deadlineAt = params.timeoutMs !== undefined ? Date.now() + params.timeoutMs : undefined;
	const initialTurnBudget = params.turnBudget ? initialTurnBudgetState(params.turnBudget) : undefined;
	/*
	 * intercom 地址按 flat step 编号，而 chain 中的 parallel step 会展开成多个 flat steps。
	 * attachRoot 和尚未 materialize 的 dynamic fanout 没有当前可通信叶子，所以只推进索引并写 undefined；
	 * 普通和静态并行任务则为每个子 Agent 生成稳定地址。
	 */
	let childTargetIndex = 0;
	const childIntercomTargets = childIntercomTarget ? steps.flatMap((step) => {
		if (!("parallel" in step) && step.importAsyncRoot) {
			childTargetIndex++;
			return [undefined];
		}
		if ("parallel" in step) {
			if (!Array.isArray(step.parallel)) {
				childTargetIndex++;
				return [undefined];
			}
			return step.parallel.map((task) => childIntercomTarget(task.agent, childTargetIndex++));
		}
		return [childIntercomTarget(step.agent, childTargetIndex++)];
	}) : undefined;

	/*
	 * 组装 SubagentRunConfig 并交给 spawnRunner。spawnRunner 会把配置写入临时 JSON，随后执行：
	 * `node jiti subagent-runner.ts <configPath>`，设置 detached=true，重定向日志并调用 unref()。
	 * 因此当前 Pi 进程可以继续工作或退出，后台 Runner 仍持有 chain 生命周期。
	 */
	let spawnResult: { pid?: number; error?: string } = {};
	try {
		spawnResult = spawnRunner(
			{
				id,
				steps,
				// 嵌套任务的结果留在任务树目录；顶层任务写入统一 RESULTS_DIR，便于 wait/status 查找。
				resultPath: inheritedNestedRoute ? nestedResultsPath(inheritedNestedRoute.rootRunId, id) : path.join(RESULTS_DIR, `${id}.json`),
				cwd: runnerCwd,
				placeholder: "{previous}",
				maxOutput,
				artifactsDir: artifactConfig.enabled ? artifactsDir : undefined,
				artifactConfig,
				share: shareEnabled,
				// 每个后台 run 使用独立 session 子目录，内部各 flat step 再创建自己的 session.jsonl。
				sessionDir: sessionRoot ? path.join(sessionRoot, `async-${id}`) : undefined,
				asyncDir,
				sessionId: ctx.currentSessionId,
				piPackageRoot,
				piArgv1: process.argv[1],
				worktreeSetupHook,
				worktreeSetupHookTimeoutMs,
				worktreeBaseDir,
				controlConfig,
				turnBudget: params.turnBudget,
				toolBudget: params.toolBudget,
				controlIntercomTarget,
				childIntercomTargets,
				resultMode,
				dynamicFanoutMaxItems: params.dynamicFanoutMaxItems,
				timeoutMs: params.timeoutMs,
				deadlineAt,
				globalConcurrencyLimit: params.globalConcurrencyLimit,
				workflowGraph,
				// 显式路由优先；否则沿用当前子 Agent 从环境变量继承的祖先路由。
				nestedRoute: nestedRoute ?? inheritedNestedRoute,
				nestedSelf: inheritedNestedRoute && nestedAddress ? {
					parentRunId: nestedAddress.parentRunId,
					parentStepIndex: nestedAddress.parentStepIndex,
					depth: nestedAddress.depth,
					path: nestedAddress.path,
				} : undefined,
			},
			id,
			runnerCwd,
		);
	} catch (error) {
		// 同步异常表示配置写入或 spawn 调用本身失败，此时没有可等待的后台 run。
		const message = error instanceof Error ? error.message : String(error);
		return formatAsyncStartError(resultMode, `Failed to start async ${resultMode} '${id}': ${message}`);
	}

	if (spawnResult.error) {
		// spawnRunner 用返回值报告可预期错误，例如 cwd 不存在、jiti 缺失或没有生成 pid。
		return formatAsyncStartError(resultMode, `Failed to start async ${resultMode} '${id}': ${spawnResult.error}`);
	}

	/*
	 * 拿到 pid 才算启动成功。接下来把逻辑 chain 展平成 Agent 列表和 parallelGroups，
	 * 这些数据不会驱动执行，只用于 nested projection、父会话事件、Async Widget 和 fleet/status 展示。
	 */
	if (spawnResult.pid) {
		const eventFirstStep = eventChain[0];
		const firstAgents = isParallelStep(eventFirstStep)
			? eventFirstStep.parallel.map((t) => t.agent)
			: isDynamicParallelStep(eventFirstStep)
				? [eventFirstStep.parallel.agent]
			: [(eventFirstStep as SequentialStep).agent];
		const parallelGroups: Array<{ start: number; count: number; stepIndex: number }> = [];
		const flatAgents: string[] = [];
		let flatStepStart = 0;
		for (let stepIndex = 0; stepIndex < eventChain.length; stepIndex++) {
			const step = eventChain[stepIndex]!;
			if (isParallelStep(step)) {
				parallelGroups.push({ start: flatStepStart, count: step.parallel.length, stepIndex });
				flatAgents.push(...step.parallel.map((task) => task.agent));
				flatStepStart += step.parallel.length;
			} else if (isDynamicParallelStep(step)) {
				parallelGroups.push({ start: flatStepStart, count: 1, stepIndex });
				flatAgents.push(step.parallel.agent);
				flatStepStart++;
			} else {
				flatAgents.push((step as SequentialStep).agent);
				flatStepStart++;
			}
		}
		/*
		 * 若这是嵌套后台任务，先向祖先的 nested event sink 写 started 事件。
		 * 事件包含 pid、asyncDir、父步骤地址和 intercom target，祖先因此可以展示、控制并递归追踪它。
		 */
		if (inheritedNestedRoute && nestedAddress) {
			const now = Date.now();
			try {
				writeNestedEvent(inheritedNestedRoute, {
					type: "subagent.nested.started",
					ts: now,
					parentRunId: nestedAddress.parentRunId,
					parentStepIndex: nestedAddress.parentStepIndex,
					child: {
						id,
						parentRunId: nestedAddress.parentRunId,
						parentStepIndex: nestedAddress.parentStepIndex,
						depth: nestedAddress.depth,
						path: nestedAddress.path,
						asyncDir,
						pid: spawnResult.pid,
						ownerIntercomTarget: process.env.PI_SUBAGENT_INTERCOM_SESSION_NAME,
						leafIntercomTarget: childIntercomTargets?.[0],
						intercomTarget: childIntercomTargets?.[0],
						ownerState: "live",
						mode: resultMode,
						state: "running",
						agent: firstAgents[0],
						agents: flatAgents,
						chainStepCount: eventChain.length,
						parallelGroups,
						...(params.timeoutMs !== undefined ? { timeoutMs: params.timeoutMs, deadlineAt } : {}),
						...(initialTurnBudget ? { turnBudget: initialTurnBudget } : {}),
						startedAt: now,
						lastUpdate: now,
					},
				});
			} catch (error) {
				console.error("Failed to emit nested async start event:", error);
			}
		}
		/*
		 * 同时在当前 Pi 会话发布 SUBAGENT_ASYNC_STARTED_EVENT。
		 * 本地 watcher 监听该事件后立即建立 Widget/状态跟踪；这里只通知“已启动”，
		 * 完成结果仍由后台文件、watcher 和 wait 工具在未来送回父 Agent。
		 */
		ctx.pi.events.emit(SUBAGENT_ASYNC_STARTED_EVENT, {
			lifecycleArtifactVersion: SUBAGENT_LIFECYCLE_ARTIFACT_VERSION,
			id,
			pid: spawnResult.pid,
			sessionId: ctx.currentSessionId,
			mode: resultMode,
			agent: firstAgents[0],
			agents: flatAgents,
			task: isParallelStep(eventFirstStep)
				? eventFirstStep.parallel[0]?.task?.slice(0, 50)
				: isDynamicParallelStep(eventFirstStep)
					? eventFirstStep.parallel.task?.slice(0, 50)
				: (eventFirstStep as SequentialStep).task?.slice(0, 50),
			chain: eventChain.map((s) =>
				isParallelStep(s) ? `[${s.parallel.map((t) => t.agent).join("+")}]` : isDynamicParallelStep(s) ? `expand:${s.parallel.agent}` : (s as SequentialStep).agent,
			),
			chainStepCount: eventChain.length,
			parallelGroups,
			workflowGraph,
			cwd: runnerCwd,
			asyncDir,
			...(params.timeoutMs !== undefined ? { timeoutMs: params.timeoutMs, deadlineAt } : {}),
			...(initialTurnBudget ? { turnBudget: initialTurnBudget } : {}),
			nestedRoute,
		});
	}

	// chainDesc 只用于给父 Agent 一眼可读的启动摘要，不参与后台执行。
	const chainDesc = chain
		.map((s) =>
			isParallelStep(s) ? `[${s.parallel.map((t) => t.agent).join("+")}]` : isDynamicParallelStep(s) ? `expand:${s.parallel.agent}` : (s as SequentialStep).agent,
		)
		.join(" -> ");

	/*
	 * 返回的是“启动回执”，不是 chain 结果。父 Agent 现在拿到 runId 后可以继续独立工作；
	 * 真正依赖结果时调用 wait，或用 status 做一次性检查。禁止在这里 await Runner，否则 async 会退化成前台模式。
	 */
	return {
		content: [{ type: "text", text: formatAsyncStartedMessage(`Async ${resultMode}: ${chainDesc} [${id}]`) }],
		details: { mode: resultMode, runId: id, results: [], asyncId: id, asyncDir, workflowGraph, ...(params.timeoutMs !== undefined ? { timeoutMs: params.timeoutMs, deadlineAt } : {}), ...(params.turnBudget ? { turnBudget: params.turnBudget } : {}), ...(params.toolBudget ? { toolBudget: params.toolBudget } : {}) },
	};
}

/**
 * Execute a single agent asynchronously
 */
export function executeAsyncSingle(
	id: string,
	params: AsyncSingleParams,
): AsyncExecutionResult {
	const {
		agent,
		agentConfig,
		ctx,
		cwd,
		maxOutput,
		artifactsDir,
		artifactConfig,
		shareEnabled,
		sessionRoot,
		sessionFile,
		maxSubagentDepth,
		worktreeSetupHook,
		worktreeSetupHookTimeoutMs,
		worktreeBaseDir,
		controlConfig,
		controlIntercomTarget,
		childIntercomTarget,
		nestedRoute,
	} = params;
	const task = params.task ?? "";
	const runnerCwd = resolveChildCwd(ctx.cwd, cwd);
	const skillNames = params.skills ?? agentConfig.skills ?? [];
	const availableModels = params.availableModels;
	const { resolved: resolvedSkills, missing: missingSkills } = resolveSkillsWithFallback(skillNames, runnerCwd, ctx.cwd);
	if (missingSkills.includes("pi-subagents")) return formatAsyncStartError("single", UNAVAILABLE_SUBAGENT_SKILL_ERROR);
	let systemPrompt = agentConfig.systemPrompt?.trim() ?? "";
	if (resolvedSkills.length > 0) {
		const injection = buildSkillInjection(resolvedSkills);
		systemPrompt = systemPrompt ? `${systemPrompt}\n\n${injection}` : injection;
	}
	const memoryInjection = buildAgentMemoryInjection(agentConfig, runnerCwd);
	if (memoryInjection) {
		systemPrompt = systemPrompt ? `${systemPrompt}\n\n${memoryInjection}` : memoryInjection;
	}

	const inheritedNestedRoute = resolveInheritedNestedRouteFromEnv();
	const nestedAddress = inheritedNestedRoute ? resolveNestedParentAddressFromEnv() : undefined;
	const asyncDir = inheritedNestedRoute
		? path.join(TEMP_ROOT_DIR, "nested-subagent-runs", inheritedNestedRoute.rootRunId, id)
		: path.join(ASYNC_DIR, id);
	try {
		fs.mkdirSync(asyncDir, { recursive: true });
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		return {
			content: [{ type: "text", text: `Failed to create async run directory '${asyncDir}': ${message}` }],
			isError: true,
			details: { mode: "single" as const, results: [] },
		};
	}

	const effectiveOutput = normalizeSingleOutputOverride(params.output, agentConfig.output);
	const outputPath = resolveSingleOutputPath(effectiveOutput, ctx.cwd, runnerCwd, params.outputBaseDir ?? (artifactsDir ? path.join(artifactsDir, "outputs", id) : undefined));
	systemPrompt = injectOutputPathSystemPrompt(systemPrompt, outputPath);
	const outputMode = params.outputMode ?? "inline";
	const validationError = validateFileOnlyOutputMode(outputMode, outputPath, `Async single run (${agent})`);
	if (validationError) return formatAsyncStartError("single", validationError);
	const taskWithOutputInstruction = injectSingleOutputInstruction(task, outputPath);
	const primaryModel = resolveSubagentModelOverride(
		params.modelOverride ?? agentConfig.model,
		ctx.currentModel,
		availableModels,
		ctx.currentModelProvider,
	);
	const effectiveThinking = params.thinkingOverride ?? agentConfig.thinking;
	const model = applyThinkingSuffix(primaryModel, effectiveThinking, params.thinkingOverride !== undefined);
	const toolBudgetInput = params.toolBudget ?? agentConfig.toolBudget ?? params.configToolBudget;
	const resolvedToolBudget = validateToolBudgetConfig(toolBudgetInput, params.toolBudget ? "toolBudget" : agentConfig.toolBudget ? "agent.toolBudget" : "config.toolBudget");
	if (resolvedToolBudget.error) return formatAsyncStartError("single", resolvedToolBudget.error);
	const deadlineAt = params.timeoutMs !== undefined ? Date.now() + params.timeoutMs : undefined;
	const initialTurnBudget = params.turnBudget ? initialTurnBudgetState(params.turnBudget) : undefined;
	let spawnResult: { pid?: number; error?: string } = {};
	try {
		spawnResult = spawnRunner(
			{
				id,
				steps: [
					{
						parentSessionId: ctx.parentSessionId ?? ctx.currentSessionId,
						agent,
						task: taskWithOutputInstruction,
						cwd: runnerCwd,
						model,
						thinking: resolveEffectiveThinking(model, effectiveThinking),
						modelCandidates: buildModelCandidates(primaryModel, agentConfig.fallbackModels, availableModels, ctx.currentModelProvider, { scope: ctx.modelScope }).map((candidate) =>
							applyThinkingSuffix(candidate, effectiveThinking, params.thinkingOverride !== undefined),
						),
						tools: agentConfig.tools,
						extensions: agentConfig.extensions,
						subagentOnlyExtensions: agentConfig.subagentOnlyExtensions,
						mcpDirectTools: agentConfig.mcpDirectTools,
						completionGuard: agentConfig.completionGuard,
						systemPrompt,
						systemPromptMode: agentConfig.systemPromptMode,
						inheritProjectContext: agentConfig.inheritProjectContext,
						inheritSkills: agentConfig.inheritSkills,
						skills: resolvedSkills.map((r) => r.name),
						outputPath,
						outputMode,
						sessionFile,
						maxSubagentDepth: resolveChildMaxSubagentDepth(maxSubagentDepth, agentConfig.maxSubagentDepth),
						effectiveAcceptance: resolveEffectiveAcceptance({
							explicit: params.acceptance,
							agentName: agent,
							task,
							mode: "single",
							async: true,
						}),
						...(resolvedToolBudget.budget ? { toolBudget: resolvedToolBudget.budget } : {}),
					},
				],
				resultPath: inheritedNestedRoute ? nestedResultsPath(inheritedNestedRoute.rootRunId, id) : path.join(RESULTS_DIR, `${id}.json`),
				cwd: runnerCwd,
				placeholder: "{previous}",
				maxOutput,
				artifactsDir: artifactConfig.enabled ? artifactsDir : undefined,
				artifactConfig,
				share: shareEnabled,
				sessionDir: sessionRoot ? path.join(sessionRoot, `async-${id}`) : undefined,
				asyncDir,
				sessionId: ctx.currentSessionId,
				piPackageRoot,
				piArgv1: process.argv[1],
				worktreeSetupHook,
				worktreeSetupHookTimeoutMs,
				worktreeBaseDir,
				controlConfig,
				timeoutMs: params.timeoutMs,
				deadlineAt,
				turnBudget: params.turnBudget,
				toolBudget: params.toolBudget,
				controlIntercomTarget,
				childIntercomTargets: childIntercomTarget ? [childIntercomTarget(agent, 0)] : undefined,
				resultMode: "single",
				nestedRoute: nestedRoute ?? inheritedNestedRoute,
				nestedSelf: inheritedNestedRoute && nestedAddress ? {
					parentRunId: nestedAddress.parentRunId,
					parentStepIndex: nestedAddress.parentStepIndex,
					depth: nestedAddress.depth,
					path: nestedAddress.path,
				} : undefined,
			},
			id,
			runnerCwd,
		);
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		return formatAsyncStartError("single", `Failed to start async run '${id}': ${message}`);
	}

	if (spawnResult.error) {
		return formatAsyncStartError("single", `Failed to start async run '${id}': ${spawnResult.error}`);
	}

	if (spawnResult.pid) {
		if (inheritedNestedRoute && nestedAddress) {
			const now = Date.now();
			try {
				writeNestedEvent(inheritedNestedRoute, {
					type: "subagent.nested.started",
					ts: now,
					parentRunId: nestedAddress.parentRunId,
					parentStepIndex: nestedAddress.parentStepIndex,
					child: {
						id,
						parentRunId: nestedAddress.parentRunId,
						parentStepIndex: nestedAddress.parentStepIndex,
						depth: nestedAddress.depth,
						path: nestedAddress.path,
						asyncDir,
						pid: spawnResult.pid,
						ownerIntercomTarget: process.env.PI_SUBAGENT_INTERCOM_SESSION_NAME,
						leafIntercomTarget: childIntercomTarget?.(agent, 0),
						intercomTarget: childIntercomTarget?.(agent, 0),
						ownerState: "live",
						mode: "single",
						state: "running",
						agent,
						agents: [agent],
						chainStepCount: 1,
						...(params.timeoutMs !== undefined ? { timeoutMs: params.timeoutMs, deadlineAt } : {}),
						...(initialTurnBudget ? { turnBudget: initialTurnBudget } : {}),
						startedAt: now,
						lastUpdate: now,
					},
				});
			} catch (error) {
				console.error("Failed to emit nested async start event:", error);
			}
		}
		ctx.pi.events.emit(SUBAGENT_ASYNC_STARTED_EVENT, {
			lifecycleArtifactVersion: SUBAGENT_LIFECYCLE_ARTIFACT_VERSION,
			id,
			pid: spawnResult.pid,
			sessionId: ctx.currentSessionId,
			mode: "single",
			agent,
			task: task?.slice(0, 50),
			cwd: runnerCwd,
			asyncDir,
			...(params.timeoutMs !== undefined ? { timeoutMs: params.timeoutMs, deadlineAt } : {}),
			...(initialTurnBudget ? { turnBudget: initialTurnBudget } : {}),
			nestedRoute,
		});
	}

	return {
		content: [{ type: "text", text: formatAsyncStartedMessage(`Async: ${agent} [${id}]`) }],
		details: { mode: "single", runId: id, results: [], asyncId: id, asyncDir, ...(params.timeoutMs !== undefined ? { timeoutMs: params.timeoutMs, deadlineAt } : {}), ...(params.turnBudget ? { turnBudget: params.turnBudget } : {}), ...(params.toolBudget ? { toolBudget: params.toolBudget } : {}) },
	};
}
