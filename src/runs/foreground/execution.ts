/**
 * Core execution logic for running subagents
 */

import { spawn } from "node:child_process";
import { existsSync, unlinkSync } from "node:fs";
import type { Message } from "@earendil-works/pi-ai";
import type { AgentConfig } from "../../agents/agents.ts";
import {
	ensureArtifactsDir,
	getArtifactPaths,
	writeArtifact,
	writeMetadata,
} from "../../shared/artifacts.ts";
import { createChildTranscriptWriter, type ChildTranscriptWriter } from "../../shared/child-transcript.ts";
import {
	type AgentProgress,
	type ArtifactPaths,
	type ControlEvent,
	type ModelAttempt,
	type RunSyncOptions,
	type SingleResult,
	type Usage,
	DEFAULT_MAX_OUTPUT,
	INTERCOM_DETACH_REQUEST_EVENT,
	INTERCOM_DETACH_RESPONSE_EVENT,
	type AcceptanceLedger,
	type ResolvedAcceptanceConfig,
	truncateOutput,
	getSubagentDepthEnv,
} from "../../shared/types.ts";
import {
	DEFAULT_CONTROL_CONFIG,
	buildControlEvent,
	claimControlNotification,
	deriveActivityState,
	shouldNotifyControlEvent,
} from "../shared/subagent-control.ts";
import {
	getFinalOutput,
	findLatestSessionFile,
	detectSubagentError,
	extractToolArgsPreview,
	extractTextFromContent,
} from "../../shared/utils.ts";
import { buildSkillInjection, resolveSkillsWithFallback } from "../../agents/skills.ts";
import { buildAgentMemoryInjection } from "../../agents/agent-memory.ts";
import { evaluateCompletionMutationGuard } from "../shared/completion-guard.ts";
import { getPiSpawnCommand } from "../shared/pi-spawn.ts";
import { createJsonlWriter } from "../../shared/jsonl-writer.ts";
import { attachPostExitStdioGuard, trySignalChild } from "../../shared/post-exit-stdio-guard.ts";
import { applyThinkingSuffix, buildPiArgs, cleanupTempDir } from "../shared/pi-args.ts";
import { readStructuredOutput } from "../shared/structured-output.ts";
import { captureSingleOutputSnapshot, formatSavedOutputReference, injectOutputPathSystemPrompt, resolveSingleOutput, validateFileOnlyOutputMode, type SingleOutputSnapshot } from "../shared/single-output.ts";
import {
	buildModelCandidates,
	formatModelAttemptNote,
	isRetryableModelFailure,
} from "../shared/model-fallback.ts";
import {
	createMutatingFailureState,
	didMutatingToolFail,
	isMutatingTool,
	nextLongRunningTrigger,
	recordMutatingFailure,
	resetMutatingFailureState,
	resolveCurrentPath,
	shouldEscalateMutatingFailures,
	summarizeRecentMutatingFailures,
} from "../shared/long-running-guard.ts";
import { acceptanceFailureMessage, evaluateAcceptance, formatAcceptancePrompt, resolveEffectiveAcceptance, stripAcceptanceReport } from "../shared/acceptance.ts";
import { appendTurnBudgetSystemPrompt, formatTurnBudgetOutput, initialTurnBudgetState, shouldAbortForTurnBudget, turnBudgetExceededMessage, turnBudgetSoftNote, turnBudgetState } from "../shared/turn-budget.ts";
import { initialToolBudgetState, toolBudgetState } from "../shared/tool-budget.ts";
import { resolveWatchdogConfig } from "../../watchdog/settings.ts";
import {
	acceptChildWatchdogEvent,
	childWatchdogIsActive,
	isChildWatchdogStatusEvent,
	resolveChildWatchdogConfig,
	type ChildWatchdogStateSnapshot,
} from "../../watchdog/child-status.ts";

const artifactOutputByResult = new WeakMap<SingleResult, string>();
const acceptanceOutputByResult = new WeakMap<SingleResult, string>();

function emptyUsage(): Usage {
	return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0, turns: 0 };
}

function sumUsage(target: Usage, source: Usage): void {
	target.input += source.input;
	target.output += source.output;
	target.cacheRead += source.cacheRead;
	target.cacheWrite += source.cacheWrite;
	target.cost += source.cost;
	target.turns += source.turns;
}

function formatTimeoutMessage(timeoutMs: number): string {
	return `Subagent timed out after ${timeoutMs}ms.`;
}

function resolveAttemptTimeout(options: RunSyncOptions): { timeoutMs: number; remainingMs: number; message: string } | undefined {
	if (options.timeoutMs === undefined) return undefined;
	const deadlineAt = options.deadlineAt ?? Date.now() + options.timeoutMs;
	return {
		timeoutMs: options.timeoutMs,
		remainingMs: Math.max(0, deadlineAt - Date.now()),
		message: formatTimeoutMessage(options.timeoutMs),
	};
}

function buildSkippedAcceptanceLedger(acceptance: ResolvedAcceptanceConfig, input: { id: string; message: string }): AcceptanceLedger {
	return {
		status: acceptance.level === "none" ? "not-required" : "rejected",
		explicit: acceptance.explicit,
		effectiveAcceptance: acceptance,
		inferredReason: acceptance.inferredReason,
		criteria: acceptance.criteria,
		runtimeChecks: acceptance.level === "none"
			? []
			: [{ id: input.id, status: "failed", message: input.message }],
		verifyRuns: [],
	};
}

function appendRecentOutput(progress: AgentProgress, lines: string[]): void {
	if (lines.length === 0) return;
	progress.recentOutput.push(...lines.filter((line) => line.trim()));
	if (progress.recentOutput.length > 50) {
		progress.recentOutput.splice(0, progress.recentOutput.length - 50);
	}
}

function stripAcceptanceReportsFromMessages(messages: Message[] | undefined): void {
	for (const message of messages ?? []) {
		if (message.role !== "assistant" || !Array.isArray(message.content)) continue;
		for (const part of message.content) {
			if (part.type === "text" && "text" in part && typeof part.text === "string") {
				part.text = stripAcceptanceReport(part.text);
			}
		}
	}
}

function snapshotProgress(progress: AgentProgress): AgentProgress {
	return {
		...progress,
		skills: progress.skills ? [...progress.skills] : undefined,
		recentTools: progress.recentTools.map((tool) => ({ ...tool })),
		recentOutput: [...progress.recentOutput],
	};
}

function snapshotResult(result: SingleResult, progress: AgentProgress): SingleResult {
	return {
		...result,
		messages: result.outputMode === "file-only" && result.savedOutputPath ? undefined : result.messages ? [...result.messages] : undefined,
		usage: { ...result.usage },
		skills: result.skills ? [...result.skills] : undefined,
		attemptedModels: result.attemptedModels ? [...result.attemptedModels] : undefined,
		modelAttempts: result.modelAttempts
			? result.modelAttempts.map((attempt) => ({
				...attempt,
				usage: attempt.usage ? { ...attempt.usage } : undefined,
			}))
			: undefined,
		controlEvents: result.controlEvents ? result.controlEvents.map((event) => ({ ...event })) : undefined,
		progress,
		progressSummary: result.progressSummary ? { ...result.progressSummary } : undefined,
		artifactPaths: result.artifactPaths ? { ...result.artifactPaths } : undefined,
		truncation: result.truncation ? { ...result.truncation } : undefined,
		outputReference: result.outputReference ? { ...result.outputReference } : undefined,
	};
}

async function runSingleAttempt(
	runtimeCwd: string,
	agent: AgentConfig,
	task: string,
	model: string | undefined,
	options: RunSyncOptions,
	shared: {
		sessionEnabled: boolean;
		systemPrompt: string;
		resolvedSkillNames?: string[];
		skillsWarning?: string;
		jsonlPath?: string;
		artifactPaths?: ArtifactPaths;
		transcriptWriter?: ChildTranscriptWriter;
		attemptNotes: string[];
		outputSnapshot?: SingleOutputSnapshot;
		originalTask?: string;
	},
): Promise<SingleResult> {
	const effectiveThinking = options.thinkingOverride ?? agent.thinking;
	const modelArg = applyThinkingSuffix(model, effectiveThinking, options.thinkingOverride !== undefined);
	const watchdogConfig = resolveWatchdogConfig(options.cwd ?? runtimeCwd);
	const childWatchdog = watchdogConfig.ok
		? resolveChildWatchdogConfig({
			config: watchdogConfig.config,
			agent: agent.name,
			runId: options.runId,
			childIndex: options.index ?? 0,
		})
		: undefined;
	const { args, env: sharedEnv, tempDir } = buildPiArgs({
		baseArgs: ["--mode", "json", "-p"],
		task,
		sessionEnabled: shared.sessionEnabled,
		sessionDir: options.sessionDir,
		sessionFile: options.sessionFile,
		model: modelArg,
		thinking: effectiveThinking,
		systemPromptMode: agent.systemPromptMode,
		inheritProjectContext: agent.inheritProjectContext,
		inheritSkills: agent.inheritSkills,
		requireReadTool: Boolean(shared.resolvedSkillNames?.length),
		tools: agent.tools,
		extensions: agent.extensions,
		subagentOnlyExtensions: agent.subagentOnlyExtensions,
		systemPrompt: appendTurnBudgetSystemPrompt(shared.systemPrompt, options.turnBudget),
		mcpDirectTools: agent.mcpDirectTools,
		cwd: options.cwd ?? runtimeCwd,
		promptFileStem: agent.name,
		intercomSessionName: options.intercomSessionName,
		orchestratorIntercomTarget: options.orchestratorIntercomTarget,
		runId: options.runId,
		childAgentName: agent.name,
		childIndex: options.index ?? 0,
		parentEventSink: options.nestedRoute?.eventSink,
		parentControlInbox: options.nestedRoute?.controlInbox,
		parentRootRunId: options.nestedRoute?.rootRunId,
		parentCapabilityToken: options.nestedRoute?.capabilityToken,
		parentSessionId: options.parentSessionId,
		structuredOutput: options.structuredOutput,
		toolBudget: options.toolBudget,
		childWatchdog,
	});

	const result: SingleResult = {
		agent: agent.name,
		task: shared.originalTask ?? task,
		exitCode: 0,
		messages: [],
		usage: emptyUsage(),
		model: modelArg,
		artifactPaths: shared.artifactPaths,
		transcriptPath: shared.transcriptWriter ? shared.artifactPaths?.transcriptPath : undefined,
		skills: shared.resolvedSkillNames,
		skillsWarning: shared.skillsWarning,
		...(options.turnBudget ? { turnBudget: initialTurnBudgetState(options.turnBudget) } : {}),
		...(options.toolBudget ? { toolBudget: initialToolBudgetState(options.toolBudget) } : {}),
	};
	const startTime = Date.now();
	if (options.structuredOutput) {
		try {
			if (existsSync(options.structuredOutput.outputPath)) unlinkSync(options.structuredOutput.outputPath);
		} catch {
			// Missing/stale structured-output files are handled after the child exits.
		}
	}
	const controlConfig = options.controlConfig ?? DEFAULT_CONTROL_CONFIG;
	let interruptedByControl = false;
	const allControlEvents: ControlEvent[] = [];
	let pendingControlEvents: ControlEvent[] = [];
	const emittedControlEventKeys = new Set<string>();
	const emitControlEvent = (event: ControlEvent) => {
		if (!shouldNotifyControlEvent(controlConfig, event)) return;
		if (!claimControlNotification(controlConfig, event, emittedControlEventKeys)) return;
		allControlEvents.push(event);
		pendingControlEvents.push(event);
		options.onControlEvent?.(event);
	};

	const progress: AgentProgress = {
		index: options.index ?? 0,
		agent: agent.name,
		status: "running",
		task,
		skills: shared.resolvedSkillNames,
		recentTools: [],
		recentOutput: [...shared.attemptNotes],
		toolCount: 0,
		tokens: 0,
		durationMs: 0,
		lastActivityAt: startTime,
	};
	result.progress = progress;
	const attemptTimeout = resolveAttemptTimeout(options);
	if (attemptTimeout?.remainingMs === 0) {
		result.exitCode = 1;
		result.timedOut = true;
		result.error = attemptTimeout.message;
		result.finalOutput = attemptTimeout.message;
		progress.status = "failed";
		progress.error = attemptTimeout.message;
		result.progressSummary = {
			toolCount: progress.toolCount,
			tokens: progress.tokens,
			durationMs: progress.durationMs,
		};
		return result;
	}
	const spawnEnv = { ...process.env, ...sharedEnv, ...getSubagentDepthEnv(options.maxSubagentDepth) };
	let observedMutationAttempt = false;

	const exitCode = await new Promise<number>((resolve) => {
		const spawnSpec = getPiSpawnCommand(args);
		const proc = spawn(spawnSpec.command, spawnSpec.args, {
			cwd: options.cwd ?? runtimeCwd,
			env: spawnEnv,
			stdio: ["ignore", "pipe", "pipe"],
			windowsHide: true,
		});
		const jsonlWriter = createJsonlWriter(shared.jsonlPath, proc.stdout);
		let buf = "";
		let processClosed = false;
		let settled = false;
		let detached = false;
		let intercomStarted = false;
		let assistantError: string | undefined;
		let removeAbortListener: (() => void) | undefined;
		let removeInterruptListener: (() => void) | undefined;
		let activityTimer: NodeJS.Timeout | undefined;
		let timeoutTimer: NodeJS.Timeout | undefined;
		let timeoutTerminationTimer: NodeJS.Timeout | undefined;
		let timeoutHardKillTimer: NodeJS.Timeout | undefined;
		let turnBudgetSoftReached = false;
		let turnBudgetTerminationTimer: NodeJS.Timeout | undefined;
		let turnBudgetHardKillTimer: NodeJS.Timeout | undefined;
		const clearTurnBudgetTimers = () => {
			if (turnBudgetTerminationTimer) {
				clearTimeout(turnBudgetTerminationTimer);
				turnBudgetTerminationTimer = undefined;
			}
			if (turnBudgetHardKillTimer) {
				clearTimeout(turnBudgetHardKillTimer);
				turnBudgetHardKillTimer = undefined;
			}
		};
		const clearTimeoutTimers = () => {
			if (timeoutTimer) {
				clearTimeout(timeoutTimer);
				timeoutTimer = undefined;
			}
			if (timeoutTerminationTimer) {
				clearTimeout(timeoutTerminationTimer);
				timeoutTerminationTimer = undefined;
			}
			if (timeoutHardKillTimer) {
				clearTimeout(timeoutHardKillTimer);
				timeoutHardKillTimer = undefined;
			}
		};

		const detachForIntercom = () => {
			detached = true;
			processClosed = true;
			result.detached = true;
			result.detachedReason = "intercom coordination";
			progress.status = "detached";
			progress.durationMs = Date.now() - startTime;
			result.progressSummary = {
				toolCount: progress.toolCount,
				tokens: progress.tokens,
				durationMs: progress.durationMs,
			};
			finish(-2);
		};

		// If the child emits a terminal assistant stop but never exits,
		// give it a short grace period to flush naturally, then clean it up.
		const FINAL_STOP_GRACE_MS = 1000;
		const HARD_KILL_MS = 3000;
		let childExited = false;
		let forcedTerminationSignal = false;
		let cleanTerminalAssistantStopReceived = false;
		let finalDrainTimer: NodeJS.Timeout | undefined;
		let finalHardKillTimer: NodeJS.Timeout | undefined;
		let watchdogTailTimer: NodeJS.Timeout | undefined;
		let childWatchdogState: ChildWatchdogStateSnapshot | undefined;
		const updateChildWatchdogState = (snapshot: ChildWatchdogStateSnapshot): void => {
			childWatchdogState = snapshot;
			result.watchdog = snapshot;
			progress.watchdog = snapshot;
		};
		const clearWatchdogTailTimer = () => {
			if (watchdogTailTimer) {
				clearTimeout(watchdogTailTimer);
				watchdogTailTimer = undefined;
			}
		};
		const clearFinalDrainTimers = () => {
			if (finalDrainTimer) {
				clearTimeout(finalDrainTimer);
				finalDrainTimer = undefined;
			}
			if (finalHardKillTimer) {
				clearTimeout(finalHardKillTimer);
				finalHardKillTimer = undefined;
			}
		};
		const startFinalDrain = () => {
			if (childWatchdogIsActive(childWatchdogState)) {
				armWatchdogTail();
				return;
			}
			if (childExited || finalDrainTimer || settled || processClosed || detached) return;
			finalDrainTimer = setTimeout(() => {
				if (settled || processClosed || detached) return;
				const termSent = trySignalChild(proc, "SIGTERM");
				if (!termSent) return;
				forcedTerminationSignal = true;
				if (!cleanTerminalAssistantStopReceived && !assistantError) {
					result.error = result.error ?? `Subagent process did not exit within ${FINAL_STOP_GRACE_MS}ms after its final message. Forcing termination.`;
				}
				finalHardKillTimer = setTimeout(() => {
					if (settled || processClosed || detached) return;
					forcedTerminationSignal = trySignalChild(proc, "SIGKILL") || forcedTerminationSignal;
				}, HARD_KILL_MS);
				finalHardKillTimer.unref?.();
			}, FINAL_STOP_GRACE_MS);
			finalDrainTimer.unref?.();
		};
		function armWatchdogTail(): void {
			if (!cleanTerminalAssistantStopReceived || watchdogTailTimer || settled || processClosed || detached) return;
			watchdogTailTimer = setTimeout(() => {
				watchdogTailTimer = undefined;
				updateChildWatchdogState({
					phase: "stale",
					seq: (childWatchdogState?.seq ?? 0) + 1,
					lastUpdate: Date.now(),
					followUpPending: false,
					reason: "child watchdog tail timeout",
					timedOut: true,
				});
				startFinalDrain();
				fireUpdate();
			}, childWatchdog?.watchdogTailTimeoutMs ?? 120_000);
			watchdogTailTimer.unref?.();
		}

		const unsubscribeIntercomDetach = options.intercomEvents?.on?.(INTERCOM_DETACH_REQUEST_EVENT, (payload) => {
			if (!options.allowIntercomDetach || detached || processClosed) return;
			if (!payload || typeof payload !== "object") return;
			const event = payload as { requestId?: unknown; runId?: unknown; agent?: unknown; childIndex?: unknown };
			const requestId = event.requestId;
			if (typeof requestId !== "string" || requestId.length === 0) return;
			const hasRoute = event.runId !== undefined || event.agent !== undefined || event.childIndex !== undefined;
			if (hasRoute) {
				if (typeof event.runId === "string" && event.runId !== options.runId) return;
				if (typeof event.agent === "string" && event.agent !== agent.name) return;
				if (typeof event.childIndex === "number" && event.childIndex !== (options.index ?? 0)) return;
			} else if (!intercomStarted) return;
			options.intercomEvents?.emit(INTERCOM_DETACH_RESPONSE_EVENT, { requestId, accepted: true, runId: options.runId, agent: agent.name, childIndex: options.index ?? 0 });
			detachForIntercom();
		});

		const finish = (code: number) => {
			if (settled) return;
			settled = true;
			clearFinalDrainTimers();
			clearWatchdogTailTimer();
			clearStdioGuard();
			clearTimeoutTimers();
			clearTurnBudgetTimers();
			if (activityTimer) {
				clearInterval(activityTimer);
				activityTimer = undefined;
			}
			unsubscribeIntercomDetach?.();
			removeAbortListener?.();
			removeInterruptListener?.();
			resolve(code);
		};

		const drainPendingControlEvents = (): ControlEvent[] | undefined => {
			if (pendingControlEvents.length === 0) return undefined;
			const events = pendingControlEvents;
			pendingControlEvents = [];
			return events;
		};

		let activeLongRunningNotified = false;
		let pendingToolResult: { tool: string; path?: string; mutates: boolean; startedAt?: number } | undefined;
		const mutatingFailures = createMutatingFailureState();
		const mutatingFailureWindowMs = 5 * 60_000;
		const currentToolDurationMs = (now: number) => progress.currentToolStartedAt ? Math.max(0, now - progress.currentToolStartedAt) : undefined;
		const emitNeedsAttention = (now: number, input: { message?: string; reason?: ControlEvent["reason"]; recentFailureSummary?: string; currentTool?: string; currentPath?: string; currentToolDurationMs?: number } = {}): boolean => {
			if (!controlConfig.enabled) return false;
			const previous = progress.activityState;
			progress.activityState = "needs_attention";
			const event = buildControlEvent({
				type: "needs_attention",
				from: previous,
				to: "needs_attention",
				runId: options.runId,
				agent: agent.name,
				index: options.index,
				ts: now,
				lastActivityAt: progress.lastActivityAt,
				message: input.message,
				reason: input.reason ?? "idle",
				turns: result.usage.turns,
				tokens: progress.tokens,
				toolCount: progress.toolCount,
				currentTool: input.currentTool ?? progress.currentTool,
				currentToolDurationMs: input.currentToolDurationMs ?? currentToolDurationMs(now),
				currentPath: input.currentPath ?? progress.currentPath,
				recentFailureSummary: input.recentFailureSummary,
			});
			emitControlEvent(event);
			return previous !== "needs_attention";
		};
		const emitActiveLongRunning = (now: number, reason: ControlEvent["reason"]): boolean => {
			if (!controlConfig.enabled || activeLongRunningNotified || progress.activityState === "needs_attention") return false;
			activeLongRunningNotified = true;
			const previous = progress.activityState;
			progress.activityState = "active_long_running";
			emitControlEvent(buildControlEvent({
				type: "active_long_running",
				from: previous,
				to: "active_long_running",
				runId: options.runId,
				agent: agent.name,
				index: options.index,
				ts: now,
				message: `${agent.name} is still active but long-running`,
				reason,
				turns: result.usage.turns,
				tokens: progress.tokens,
				toolCount: progress.toolCount,
				currentTool: progress.currentTool,
				currentToolDurationMs: currentToolDurationMs(now),
				currentPath: progress.currentPath,
				elapsedMs: now - startTime,
			}));
			return true;
		};
		const requestTurnBudgetAbort = (turnCount: number) => {
			const budget = options.turnBudget;
			if (!budget || result.timedOut || result.turnBudgetExceeded || interruptedByControl || processClosed || settled || detached) return;
			const message = turnBudgetExceededMessage(budget, turnCount);
			result.turnBudgetExceeded = true;
			result.wrapUpRequested = true;
			result.turnBudget = turnBudgetState(budget, turnCount, true);
			result.error = message;
			result.finalOutput = message;
			progress.status = "failed";
			progress.error = message;
			progress.durationMs = Date.now() - startTime;
			fireUpdate();
			trySignalChild(proc, "SIGINT");
			turnBudgetTerminationTimer = setTimeout(() => {
				if (processClosed || settled || detached || result.timedOut) return;
				trySignalChild(proc, "SIGTERM");
			}, 1000);
			turnBudgetTerminationTimer.unref?.();
			turnBudgetHardKillTimer = setTimeout(() => {
				if (processClosed || settled || detached || result.timedOut) return;
				trySignalChild(proc, "SIGKILL");
			}, 4000);
			turnBudgetHardKillTimer.unref?.();
		};

		const updateTurnBudget = (turnCount: number, terminalAssistantStop: boolean) => {
			const budget = options.turnBudget;
			if (!budget || result.timedOut || result.turnBudgetExceeded) return;
			if (turnCount < budget.maxTurns) {
				result.turnBudget = { ...budget, outcome: "within-budget", turnCount };
				return;
			}
			if (!turnBudgetSoftReached) {
				turnBudgetSoftReached = true;
				result.wrapUpRequested = true;
				appendRecentOutput(progress, [turnBudgetSoftNote(budget, turnCount)]);
			}
			result.turnBudget = turnBudgetState(budget, turnCount, false);
			if (shouldAbortForTurnBudget(budget, turnCount, terminalAssistantStop)) {
				requestTurnBudgetAbort(turnCount);
			}
		};

		const updateActivityState = (now: number): boolean => {
			if (!controlConfig.enabled) return false;
			const idleState = deriveActivityState({
				config: controlConfig,
				startedAt: startTime,
				lastActivityAt: progress.lastActivityAt,
				now,
			});
			if (idleState === "needs_attention") {
				return progress.activityState === "needs_attention" ? false : emitNeedsAttention(now);
			}
			const activeReason = nextLongRunningTrigger(controlConfig, {
				startedAt: startTime,
				now,
				turns: result.usage.turns,
				tokens: progress.tokens,
			});
			return activeReason ? emitActiveLongRunning(now, activeReason) : false;
		};


		const emitUpdateSnapshot = (text: string) => {
			if (!options.onUpdate || processClosed) return;
			const progressSnapshot = snapshotProgress(progress);
			const resultSnapshot = snapshotResult(result, progressSnapshot);
			const controlEvents = drainPendingControlEvents();
			options.onUpdate({
				content: [{ type: "text", text }],
				details: {
					mode: "single",
					results: [resultSnapshot],
					progress: [progressSnapshot],
					controlEvents,
				},
			});
		};

		const fireUpdate = () => {
			if (!options.onUpdate || processClosed) return;
			progress.durationMs = Date.now() - startTime;
			const output = (result.timedOut || result.turnBudgetExceeded) && result.finalOutput ? result.finalOutput : getFinalOutput(result.messages);
			emitUpdateSnapshot(output || "(running...)");
		};

		const processLine = (line: string) => {
			if (!line.trim()) return;
			jsonlWriter.writeLine(line);
			let evt: { type?: string; message?: Message; toolName?: string; args?: unknown };
			try {
				evt = JSON.parse(line) as { type?: string; message?: Message; toolName?: string; args?: unknown };
			} catch {
				shared.transcriptWriter?.writeStdoutLine(line);
				// Non-JSON stdout lines are expected; only structured events are parsed.
				return;
			}
			shared.transcriptWriter?.writeChildEvent(evt);

			if (isChildWatchdogStatusEvent(evt)) {
				if (!childWatchdog) return;
				const next = acceptChildWatchdogEvent({
					current: childWatchdogState,
					event: evt,
					runId: options.runId,
					agent: agent.name,
					childIndex: options.index ?? 0,
				});
				if (!next) return;
				updateChildWatchdogState(next);
				if (childWatchdogIsActive(next)) {
					clearFinalDrainTimers();
					armWatchdogTail();
				} else {
					clearWatchdogTailTimer();
					if (cleanTerminalAssistantStopReceived) startFinalDrain();
				}
				fireUpdate();
				return;
			}

			const now = Date.now();
			progress.durationMs = now - startTime;
			progress.lastActivityAt = now;
			updateActivityState(now);

			if (evt.type === "tool_execution_start") {
				const toolArgs = evt.args && typeof evt.args === "object" && !Array.isArray(evt.args)
					? evt.args as Record<string, unknown>
					: {};
				if (options.allowIntercomDetach && (evt.toolName === "intercom" || evt.toolName === "contact_supervisor")) {
					intercomStarted = true;
				}
				progress.toolCount++;
				if (options.toolBudget) {
					result.toolBudget = toolBudgetState(options.toolBudget, progress.toolCount);
				}
				progress.currentTool = evt.toolName;
				progress.currentToolArgs = extractToolArgsPreview(toolArgs);
				progress.currentToolStartedAt = now;
				progress.currentPath = resolveCurrentPath(evt.toolName, toolArgs);
				const mutates = isMutatingTool(evt.toolName, toolArgs);
				observedMutationAttempt = observedMutationAttempt || mutates;
				pendingToolResult = { tool: evt.toolName ?? "tool", path: progress.currentPath, mutates, startedAt: now };
				fireUpdate();
			}

			if (evt.type === "tool_execution_end") {
				if (progress.currentTool) {
					progress.recentTools.push({
						tool: progress.currentTool,
						args: progress.currentToolArgs || "",
						endMs: now,
					});
				}
				progress.currentTool = undefined;
				progress.currentToolArgs = undefined;
				progress.currentToolStartedAt = undefined;
				progress.currentPath = undefined;
				fireUpdate();
			}

			if (evt.type === "message_end" && evt.message) {
				result.messages.push(evt.message);
				if (evt.message.role === "assistant") {
					result.usage.turns++;
					progress.turnCount = result.usage.turns;
					const stopReason = (evt.message as { stopReason?: string }).stopReason;
					const hasToolCall = Array.isArray(evt.message.content)
						&& evt.message.content.some((part) => (part as { type?: string }).type === "toolCall");
					const terminalAssistantStop = stopReason === "stop" && !hasToolCall;
					updateTurnBudget(result.usage.turns, terminalAssistantStop);
					const u = evt.message.usage;
					if (u) {
						result.usage.input += u.input || 0;
						result.usage.output += u.output || 0;
						result.usage.cacheRead += u.cacheRead || 0;
						result.usage.cacheWrite += u.cacheWrite || 0;
						result.usage.cost += u.cost?.total || 0;
						progress.tokens = result.usage.input + result.usage.output;
					}
					if (!result.model && evt.message.model) result.model = evt.message.model;
					if (evt.message.errorMessage) assistantError = evt.message.errorMessage;
					const assistantText = extractTextFromContent(evt.message.content);
					appendRecentOutput(progress, assistantText.split("\n").slice(-10));
					// Final assistant message: start the exit drain window.
					if (terminalAssistantStop) {
						if (!evt.message.errorMessage && assistantText.trim()) assistantError = undefined;
						cleanTerminalAssistantStopReceived ||= !evt.message.errorMessage;
						startFinalDrain();
					}
				}
				updateActivityState(now);
				fireUpdate();
			}

			if (evt.type === "tool_result_end" && evt.message) {
				result.messages.push(evt.message);
				const resultText = extractTextFromContent(evt.message.content);
				if (options.toolBudget && pendingToolResult && resultText.includes("Tool budget hard limit reached")) {
					result.toolBudgetBlocked = true;
					result.toolBudget = toolBudgetState(options.toolBudget, progress.toolCount, pendingToolResult.tool);
				}
				appendRecentOutput(progress, resultText.split("\n").slice(-10));
				const toolSnapshot = pendingToolResult;
				pendingToolResult = undefined;
				if (toolSnapshot?.mutates && didMutatingToolFail(resultText)) {
					recordMutatingFailure(mutatingFailures, {
						tool: toolSnapshot.tool,
						path: toolSnapshot.path,
						error: resultText.split("\n").find((line) => line.trim())?.trim().slice(0, 180) ?? "mutating tool failed",
						ts: now,
					}, mutatingFailureWindowMs);
					if (shouldEscalateMutatingFailures(mutatingFailures, controlConfig.failedToolAttemptsBeforeAttention)) {
						emitNeedsAttention(now, {
							message: `${agent.name} needs attention after repeated mutating tool failures`,
							reason: "tool_failures",
							currentTool: toolSnapshot.tool,
							currentPath: toolSnapshot.path,
							currentToolDurationMs: toolSnapshot.startedAt ? Math.max(0, now - toolSnapshot.startedAt) : undefined,
							recentFailureSummary: summarizeRecentMutatingFailures(mutatingFailures),
						});
					}
				} else if (toolSnapshot?.mutates) {
					resetMutatingFailureState(mutatingFailures);
				}
				fireUpdate();
			}
		};

		if (controlConfig.enabled) {
			activityTimer = setInterval(() => {
				if (processClosed || settled || detached) return;
				const now = Date.now();
				if (updateActivityState(now)) {
					progress.durationMs = now - startTime;
					fireUpdate();
				}
			}, 1000);
			activityTimer.unref?.();
		}

		if (attemptTimeout) {
			timeoutTimer = setTimeout(() => {
				if (processClosed || settled || detached || interruptedByControl) return;
				result.timedOut = true;
				result.error = attemptTimeout.message;
				result.finalOutput = attemptTimeout.message;
				progress.status = "failed";
				progress.error = attemptTimeout.message;
				progress.durationMs = Date.now() - startTime;
				fireUpdate();
				trySignalChild(proc, "SIGINT");
				timeoutTerminationTimer = setTimeout(() => {
					if (processClosed || settled || detached) return;
					trySignalChild(proc, "SIGTERM");
				}, 1000);
				timeoutTerminationTimer.unref?.();
				timeoutHardKillTimer = setTimeout(() => {
					if (processClosed || settled || detached) return;
					trySignalChild(proc, "SIGKILL");
				}, 4000);
				timeoutHardKillTimer.unref?.();
			}, attemptTimeout.remainingMs);
			timeoutTimer.unref?.();
		}

		let stderrBuf = "";

		const clearStdioGuard = attachPostExitStdioGuard(proc, { idleMs: 2000, hardMs: 8000 });
		proc.stdout.on("data", (d) => {
			buf += d.toString();
			const lines = buf.split("\n");
			buf = lines.pop() || "";
			lines.forEach(processLine);
		});
		proc.stderr.on("data", (d) => {
			stderrBuf += d.toString();
		});
		proc.on("exit", () => {
			childExited = true;
			clearFinalDrainTimers();
		});
		proc.on("close", (code, signal) => {
			clearFinalDrainTimers();
			clearStdioGuard();
			void jsonlWriter.close().catch(() => {
				// JSONL artifact flush is best effort.
			});
			cleanupTempDir(tempDir);
			if (buf.trim()) processLine(buf);
			if (stderrBuf.trim()) shared.transcriptWriter?.writeStderrText(stderrBuf);
			let closeError = result.error ?? assistantError;
			const forcedDrainAfterFinalSuccess = forcedTerminationSignal && cleanTerminalAssistantStopReceived && !closeError;
			if (code !== 0 && stderrBuf.trim() && !closeError && !forcedDrainAfterFinalSuccess) {
				closeError = stderrBuf.trim();
			}
			const finalCode = forcedDrainAfterFinalSuccess ? 0 : forcedTerminationSignal || signal ? (code ?? 1) : (code ?? 0);
			if (detached) {
				const recoveredProgress = snapshotProgress(progress);
				const recoveredResult = snapshotResult(result, recoveredProgress);
				if (!recoveredResult.error && closeError) recoveredResult.error = closeError;
				recoveredResult.exitCode = recoveredResult.error && finalCode === 0 ? 1 : finalCode;
				recoveredProgress.status = recoveredResult.exitCode === 0 ? "completed" : "failed";
				recoveredProgress.durationMs = Date.now() - startTime;
				if (recoveredResult.error) recoveredProgress.error = recoveredResult.error;
				recoveredResult.progressSummary = {
					toolCount: recoveredProgress.toolCount,
					tokens: recoveredProgress.tokens,
					durationMs: recoveredProgress.durationMs,
				};
				let fullOutput = stripAcceptanceReport(getFinalOutput(recoveredResult.messages ?? []));
				fullOutput = fullOutput.trim() || recoveredResult.error || recoveredResult.finalOutput || "Detached child exited without final output.";
				recoveredResult.outputMode = options.outputMode ?? "inline";
				if (options.outputPath && recoveredResult.exitCode === 0) {
					const resolvedOutput = resolveSingleOutput(options.outputPath, fullOutput, shared.outputSnapshot);
					fullOutput = stripAcceptanceReport(resolvedOutput.fullOutput);
					recoveredResult.savedOutputPath = resolvedOutput.savedPath;
					recoveredResult.outputSaveError = resolvedOutput.saveError;
					if (resolvedOutput.savedPath) {
						recoveredResult.outputReference = formatSavedOutputReference(resolvedOutput.savedPath, fullOutput);
					} else {
						recoveredResult.exitCode = 1;
						recoveredResult.error = `Output file was not finalized after detached child exit: ${resolvedOutput.saveError ?? options.outputPath}`;
						recoveredProgress.status = "failed";
						recoveredProgress.error = recoveredResult.error;
					}
				}
				recoveredResult.finalOutput = options.outputMode === "file-only" && recoveredResult.savedOutputPath && recoveredResult.outputReference
					? recoveredResult.outputReference.message
					: fullOutput;
				if (recoveredResult.artifactPaths && options.artifactConfig?.enabled !== false && options.artifactConfig?.includeOutput !== false) {
					try {
						writeArtifact(recoveredResult.artifactPaths.outputPath, fullOutput);
					} catch {
						// Detached children may outlive test/temp cleanup; recovered status is best-effort.
					}
				}
				options.onDetachedExit?.(recoveredResult);
				finish(-2);
				return;
			}
			if (!result.error && closeError) result.error = closeError;
			processClosed = true;
			finish(finalCode);
		});
		proc.on("error", (error) => {
			clearFinalDrainTimers();
			clearStdioGuard();
			void jsonlWriter.close().catch(() => {
				// JSONL artifact flush is best effort.
			});
			cleanupTempDir(tempDir);
			if (stderrBuf.trim()) shared.transcriptWriter?.writeStderrText(stderrBuf);
			if (!result.error) {
				result.error = error instanceof Error ? error.message : String(error);
			}
			finish(1);
		});

		if (options.signal) {
			const kill = () => {
				if (processClosed || detached) return;
				proc.kill("SIGTERM");
				setTimeout(() => !proc.killed && proc.kill("SIGKILL"), 3000);
			};
			if (options.signal.aborted) kill();
			else {
				options.signal.addEventListener("abort", kill, { once: true });
				removeAbortListener = () => options.signal?.removeEventListener("abort", kill);
			}
		}

		if (options.interruptSignal) {
			const interrupt = () => {
				if (processClosed || detached || settled) return;
				if (result.timedOut) return;
				interruptedByControl = true;
				clearTimeoutTimers();
				progress.status = "running";
				progress.durationMs = Date.now() - startTime;
				result.interrupted = true;
				result.finalOutput = "Interrupted. Waiting for explicit next action.";
				progress.activityState = undefined;
				fireUpdate();
				trySignalChild(proc, "SIGINT");
				setTimeout(() => {
					if (settled || processClosed || detached) return;
					trySignalChild(proc, "SIGTERM");
				}, 1000).unref?.();
			};
			if (options.interruptSignal.aborted) interrupt();
			else {
				options.interruptSignal.addEventListener("abort", interrupt, { once: true });
				removeInterruptListener = () => options.interruptSignal?.removeEventListener("abort", interrupt);
			}
		}
	});
	result.exitCode = exitCode;
	if (interruptedByControl) {
		result.exitCode = 0;
		result.interrupted = true;
		result.error = undefined;
		result.finalOutput = result.finalOutput || "Interrupted. Waiting for explicit next action.";
		result.controlEvents = allControlEvents.length ? allControlEvents : undefined;
		progress.activityState = undefined;
		progress.durationMs = Date.now() - startTime;
		result.progressSummary = {
			toolCount: progress.toolCount,
			tokens: progress.tokens,
			durationMs: progress.durationMs,
		};
		return result;
	}
	if (result.detached) {
		result.exitCode = -2;
		result.finalOutput = "Detached for intercom coordination before task completion.";
		result.outputMode = options.outputMode ?? "inline";
		if (options.outputPath) {
			result.outputSaveError = "Output file was not finalized because the subagent detached for intercom coordination.";
		}
		return result;
	}

	if (result.error && result.exitCode === 0) {
		result.exitCode = 1;
	}
	if (result.exitCode === 0 && !result.error) {
		const errInfo = detectSubagentError(result.messages);
		if (errInfo.hasError) {
			result.exitCode = errInfo.exitCode ?? 1;
			result.error = errInfo.details
				? `${errInfo.errorType} failed (exit ${errInfo.exitCode}): ${errInfo.details}`
				: `${errInfo.errorType} failed with exit code ${errInfo.exitCode}`;
		}
	}
	if (result.exitCode === 0 && !result.error) {
		const finalText = getFinalOutput(result.messages);
		const missingStructuredOutput = options.structuredOutput
			? !existsSync(options.structuredOutput.outputPath)
			: false;
		if (!finalText?.trim() && (!options.structuredOutput || missingStructuredOutput)) {
			result.exitCode = 1;
			result.error = "Subagent produced no output (possible model cold-start or empty response).";
		}
	}
	if (options.structuredOutput && result.exitCode === 0 && !result.error) {
		const structured = readStructuredOutput({
			schema: options.structuredOutput.schema,
			schemaPath: options.structuredOutput.schemaPath,
			outputPath: options.structuredOutput.outputPath,
		});
		result.structuredOutputSchemaPath = options.structuredOutput.schemaPath;
		result.structuredOutputPath = options.structuredOutput.outputPath;
		if (structured.error) {
			result.exitCode = 1;
			result.error = structured.error;
		} else {
			result.structuredOutput = structured.value;
		}
	}

	progress.status = result.exitCode === 0 ? "completed" : "failed";
	progress.durationMs = Date.now() - startTime;
	if (result.error) {
		progress.error = result.error;
		if (progress.currentTool) {
			progress.failedTool = progress.currentTool;
		}
	}

	result.progressSummary = {
		toolCount: progress.toolCount,
		tokens: progress.tokens,
		durationMs: progress.durationMs,
	};

	const acceptanceOutput = getFinalOutput(result.messages);
	let fullOutput = stripAcceptanceReport(acceptanceOutput);
	if (result.timedOut) {
		const timeoutMessage = formatTimeoutMessage(options.timeoutMs ?? 0);
		fullOutput = fullOutput.trim()
			? `${timeoutMessage}\n\nPartial output before timeout:\n${fullOutput}`
			: timeoutMessage;
	} else if (result.turnBudgetExceeded && result.turnBudget) {
		fullOutput = formatTurnBudgetOutput(turnBudgetExceededMessage(result.turnBudget, result.turnBudget.turnCount), fullOutput);
	} else if (result.wrapUpRequested && result.turnBudget?.outcome === "wrap-up-requested") {
		const note = turnBudgetSoftNote(result.turnBudget, result.turnBudget.wrapUpRequestedAtTurn ?? result.turnBudget.turnCount);
		fullOutput = fullOutput.trim() ? `${note}\n\n${fullOutput}` : note;
	}
	const completionGuard = result.exitCode === 0 && !result.error && agent.completionGuard !== false
		? evaluateCompletionMutationGuard({
			agent: agent.name,
			task: shared.originalTask ?? task,
			messages: result.messages,
			tools: agent.tools,
			mcpDirectTools: agent.mcpDirectTools,
		})
		: undefined;
	if (completionGuard?.triggered && !observedMutationAttempt) {
		result.exitCode = 1;
		result.error = "Subagent completed without making edits for an implementation task.\nIt appears to have returned planning or scratchpad output instead of applying changes.";
		progress.status = "failed";
		progress.error = result.error;
		emitControlEvent(buildControlEvent({
			from: progress.activityState,
			to: "needs_attention",
			runId: options.runId ?? agent.name,
			agent: agent.name,
			index: options.index,
			ts: Date.now(),
			message: `${agent.name} completed without making edits for an implementation task`,
			reason: "completion_guard",
		}));
	}
		if (options.outputPath && result.exitCode === 0) {
			const resolvedOutput = resolveSingleOutput(options.outputPath, fullOutput, shared.outputSnapshot);
			fullOutput = stripAcceptanceReport(resolvedOutput.fullOutput);
			result.savedOutputPath = resolvedOutput.savedPath;
			result.outputSaveError = resolvedOutput.saveError;
			if (resolvedOutput.savedPath) {
				result.outputReference = formatSavedOutputReference(resolvedOutput.savedPath, fullOutput);
			}
	}
		artifactOutputByResult.set(result, fullOutput);
		acceptanceOutputByResult.set(result, acceptanceOutput);
	result.outputMode = options.outputMode ?? "inline";
	result.finalOutput = options.outputMode === "file-only" && result.savedOutputPath && result.outputReference
		? result.outputReference.message
		: fullOutput;
	result.controlEvents = allControlEvents.length ? allControlEvents : undefined;
	if (options.onUpdate) {
		const finalText = result.finalOutput || result.error || "(no output)";
		const progressSnapshot = snapshotProgress(progress);
		const resultSnapshot = snapshotResult(result, progressSnapshot);
		options.onUpdate({
			content: [{ type: "text", text: finalText }],
			details: {
				mode: "single",
				results: [resultSnapshot],
				progress: [progressSnapshot],
				controlEvents: allControlEvents.length ? allControlEvents : undefined,
			},
		});
	}
	return result;
}

/**
subagent-executor
    ↓
runSync()
    ├── 查找 Agent 配置
    ├── 注入 Skill / Memory / Acceptance
    ├── 选择模型和备用模型
    └── runSingleAttempt()
            ├── buildPiArgs()
            ├── spawn Pi 子进程
            ├── 解析 stdout JSONL
            ├── 汇总 Message / Tool / Usage
            └── 等待子进程退出
    ↓
验收、保存 Artifact、返回 SingleResult

可以把 runSync() 记成四层：
第一层：准备
Agent、Skill、Memory、Acceptance、Model
第二层：执行
runSingleAttempt() -> spawn Pi
第三层：容错
模型 fallback、超时、Session、输出恢复
第四层：收尾
Usage、Artifact、Acceptance、SingleResult
 */
export async function runSync(
	runtimeCwd: string, // 父 Agent 的工作目录
	/* 所有可用子 Agent 配置
	[
		{ name: "worker", systemPrompt: "...", tools: [...] },
		{ name: "reviewer", systemPrompt: "...", tools: [...] },
	] */
	agents: AgentConfig[], 
	// 本次需要调用的角色名称，例如：worker，reviewer，researcher，etc.
	agentName: string,
	task: string, // 交给子 Agent 的任务
	options: RunSyncOptions, // 模型、Session、超时、Artifact、输出模式等运行策略
): Promise<SingleResult> { // 一个子 Agent 的最终结果
	// 根据 agentName 查找 AgentConfig
	const agent = agents.find((a) => a.name === agentName);
	// 如果找不到，返回SingleResult类型的错误
	// 上层 Single、Parallel、Chain 都统一处理 SingleResult，返回错误结果比抛异常更容易聚合
	if (!agent) {
		return {
			agent: agentName,
			task,
			exitCode: 1,
			messages: [],
			usage: emptyUsage(),
			error: `Unknown agent: ${agentName}`,
		};
	}
	// 检查输出模式，例如用户配置 outputMode: "file-only"，那么必须存在 outputPath，否则结果只保存到文件
	// 但又没有文件地址，输出就会丢失。校验失败同样直接返回 SingleResult，不会启动子进程。
	const outputModeValidationError = validateFileOnlyOutputMode(options.outputMode, options.outputPath, `Single run (${agentName})`);
	if (outputModeValidationError) {
		return {
			agent: agentName,
			task,
			exitCode: 1,
			messages: [],
			usage: emptyUsage(),
			outputMode: options.outputMode,
			error: outputModeValidationError,
		};
	}

	/* 
	判断调用方是否显式开启了 share 选项，是否启用子 Agent 会话分享
	意思是：是否把子 Agent 的 Session 导出并上传，生成一个可分享链接
	开启后的流程
	例如父 Agent 调用：
	subagent({
		agent: "worker",
		task: "分析这个项目",
		async: true,
		share: true,
	});
	后台子 Agent 执行完成后，大致会经过：
	子 Agent 运行
		↓
	保存 session.jsonl
		↓
	将 Session 导出为 HTML
		↓
	检查 GitHub CLI gh 是否登录
		↓
	执行 gh gist create <session.html>
		↓
	获得 Gist ID
		↓
	生成网页查看链接 
	*/
	const shareEnabled = options.share === true;
	// 计算本次任务最终采用的验收规则
	const effectiveAcceptance = resolveEffectiveAcceptance({
		explicit: options.acceptance,
		agentName,
		task,
		mode: options.acceptanceContext?.mode ?? "single",
		async: options.acceptanceContext?.async,
		dynamic: options.acceptanceContext?.dynamic,
		dynamicGroup: options.acceptanceContext?.dynamicGroup,
	});
	// 然后将验收要求格式化成 Prompt
	const acceptancePrompt = formatAcceptancePrompt(effectiveAcceptance);
	// 将验收要求拼接到任务描述中
	// 因此子 Agent 实际收到的内容是：
	// - 原始任务
	// - 验收标准
	// - 必须输出的验收报告
	// - 需要执行的验证命令
	const taskWithAcceptance = acceptancePrompt ? `${task}\n${acceptancePrompt}` : task;
	// 满足任意条件就启用子 Agent Session：指定 sessionFile、指定 sessionDir、开启 share（因为要共享 Session，必须先有 Session 文件）
	// 启用后，子 Agent 的消息可以写入独立 JSONL Session，供恢复或继续执行
	const sessionEnabled = Boolean(options.sessionFile || options.sessionDir) || shareEnabled;
	// 优先级：本次调用 options.skills > AgentConfig.skills > 空数组
	const skillNames = options.skills ?? agent.skills ?? [];
	const skillCwd = options.cwd ?? runtimeCwd;
	const { resolved: resolvedSkills, missing: missingSkills } = resolveSkillsWithFallback(skillNames, skillCwd, runtimeCwd);
	// 用户明确要求 pi-subagents && pi-subagents 最终未能解析，返回错误，其他普通 Skill 缺失只会记录 warning
	// 因为这个 pi-subagents Skill 通常承载子 Agent 协作协议，缺失时继续执行可能导致行为完全不符合预期
	if (skillNames.some((skill) => skill.trim() === "pi-subagents") && missingSkills.includes("pi-subagents")) {
		return {
			agent: agentName,
			task,
			exitCode: 1,
			messages: [],
			usage: emptyUsage(),
			error: "Skills not found: pi-subagents",
		};
	}

	/* 
	最终 System Prompt 结构大致是：
	Agent 原始 System Prompt
			+
	Skill 内容
			+
	Persistent Agent Memory
			+
	输出文件协议
	之后这份 systemPrompt 会传给子 Pi 进程 

	这套记忆机制不是：
	自动从每轮对话提取记忆
	而是：
	运行前读取 MEMORY.md
		↓
	把记忆注入 System Prompt
		↓
	Agent 使用已有记忆执行任务
		↓
	Agent 自己判断是否产生了可复用知识
		↓
	调用文件工具更新 MEMORY.md
		↓
	下次运行再次读取
	所以它属于“文件持久化 + Prompt 注入 + Agent 主动维护”的长期记忆机制。它独立于 session.jsonl
	Session 保存一次会话的完整上下文，而 MEMORY.md 保存跨会话、跨运行复用的高价值角色知识
	*/

	// 组装 System Prompt
	let systemPrompt = agent.systemPrompt?.trim() || "";
	// 注入 Skill
	if (resolvedSkills.length > 0) {
		const skillInjection = buildSkillInjection(resolvedSkills);
		systemPrompt = systemPrompt ? `${systemPrompt}\n\n${skillInjection}` : skillInjection;
	}
	// 注入 Agent Memory，注入的是：角色 Memory 文件的内容
	const memoryInjection = buildAgentMemoryInjection(agent, skillCwd);
	if (memoryInjection) {
		systemPrompt = systemPrompt ? `${systemPrompt}\n\n${memoryInjection}` : memoryInjection;
	}
	// 注入输出文件要求
	systemPrompt = injectOutputPathSystemPrompt(systemPrompt, options.outputPath);


	// 构造候选模型，模型优先级大致是：本次 modelOverride → Agent 默认模型 → Agent fallbackModels
	// 还会过滤当前不可用模型，并考虑首选 Provider
	const candidates = buildModelCandidates(
		options.modelOverride ?? agent.model,
		agent.fallbackModels,
		options.availableModels,
		options.preferredModelProvider,
		{ scope: options.modelScope },
	);
	// 初始化统计容器，用于记录本次调用尝试过的模型，以及每次尝试的结果
	// 因为一次 runSync() 可能尝试多个模型，所以 Usage 和指标必须跨 Attempt 聚合
	const attemptedModels: string[] = [];
	const modelAttempts: ModelAttempt[] = [];
	const aggregateUsage = emptyUsage();
	const attemptNotes: string[] = [];
	let totalToolCount = 0;
	let totalDurationMs = 0;

	// 准备 Artifact，Artifact 是这次子 Agent 执行产生的运行文件
	// 假设：runId = run-abc，agentName = worker，index = 0
	// 可能创建类似目录：
	// artifacts/
	// └── run-abc/
	// 	└── worker-0/
	// 		├── input.md - 交给子 Agent 的任务
	// 		├── output.md - 子 Agent 最终输出
	// 		├── events.jsonl - 子 Pi stdout 输出的原始事件流
	// 		├── transcript.jsonl - 整理后的对话和执行记录
	// 		└── metadata.json - 模型、Token、耗时、退出码等
	let artifactPathsResult: ArtifactPaths | undefined;
	let jsonlPath: string | undefined;
	let transcriptWriter: ChildTranscriptWriter | undefined;
	// 如果启用了 Artifact
	if (options.artifactsDir && options.artifactConfig?.enabled !== false) {
		/* 
		生成 Artifact 路径， 这一步只是计算每个文件应该存在哪里
		得到的对象大致是：
		artifactPathsResult = {
			inputPath: ".../worker-0/input.md",
			outputPath: ".../worker-0/output.md",
			jsonlPath: ".../worker-0/events.jsonl",
 			transcriptPath: ".../worker-0/transcript.jsonl",
			metadataPath: ".../worker-0/metadata.json",
		};
		此时不一定已经创建这些文件，只是拿到了路径 
		*/
		artifactPathsResult = getArtifactPaths(options.artifactsDir, options.runId, agentName, options.index);
		// 创建 Artifact 目录，保证后面写文件时目录存在
		ensureArtifactsDir(options.artifactsDir);
		// 保存子 Agent 输入，往 input.md 写入任务描述，这里保存的是：原始任务 + Acceptance 验收要求
		// 提前保存输入的目的是：如果子 Agent 后面出现启动失败、模型报错、进程崩溃、超时或验收失败，开发者
		// 仍然可以打开 input.md，确认当时究竟给子 Agent 发送了什么任务。这里还没有执行子 Agent，只是在建立可观测性文件
		if (options.artifactConfig?.includeInput !== false) {
			writeArtifact(artifactPathsResult.inputPath, `# Task for ${agentName}\n\n${taskWithAcceptance}`);
		}
		/* 
		准备 JSONL 路径， 这里只是：把 events.jsonl 的路径保存到变量 jsonlPath 还没有开始写入
		stdout 会输出类似事件到 events.jsonl：
		{"type":"agent_start"}
		{"type":"message_start","message":{...}}
		{"type":"tool_execution_start","toolName":"read"}
		{"type":"tool_execution_end","toolName":"read"}
		{"type":"message_end","message":{...}}
		{"type":"agent_end"}

		因此完整关系是： 
		runSync() -> 只准备 jsonlPath
		runSingleAttempt() -> spawn 子 Pi -> 读取 proc.stdout -> 写入 events.jsonl 
		*/
		if (options.artifactConfig?.includeJsonl !== false) {
			jsonlPath = artifactPathsResult.jsonlPath;
		}
		// 创建 Transcript Writer，这里创建一个专门负责记录子 Agent 执行过程的 Writer
		// 它知道 Transcript 的：保存路径、当前是 foreground、Run ID、Agent 名称、子任务序号、工作目录
		if (options.artifactConfig?.includeTranscript !== false) {
			transcriptWriter = createChildTranscriptWriter({
				transcriptPath: artifactPathsResult.transcriptPath,
				source: "foreground",
				runId: options.runId,
				agent: agentName,
				childIndex: options.index,
				cwd: options.cwd ?? runtimeCwd,
			});
			/* 
			记录初始用户任务
			
			后面 runSingleAttempt() 解析子进程输出时，还会调用 
			- transcriptWriter.writeChildEvent(evt)
			- transcriptWriter.writeStdoutLine(line)
			- transcriptWriter.writeStderrText(stderr)
			
			于是 Transcript 会逐步包含：
			- 初始任务
			- Assistant 消息
			- Tool Call
			- Tool Result
			- 错误输出
			- 最终回答
			*/
			transcriptWriter.writeInitialUserMessage(taskWithAcceptance);
		}
	}

	let lastResult: SingleResult | undefined;
	// 模型尝试循环，没有显式候选模型时传入 undefined，让 Pi 使用当前默认模型
	// 这里即使没有传 candidates，也会进入 for 循环，因为 candidates 是 [undefined]
	const modelsToTry = candidates.length > 0 ? candidates : [undefined];
	// 遍历候选模型，尝试执行子 Agent
	for (let i = 0; i < modelsToTry.length; i++) {
		const candidate = modelsToTry[i];
		// 记录输出文件执行前快照，这是为了执行结束后判断文件是否真的被创建或修改，而不是误把旧文件当作本次输出
		const outputSnapshot = captureSingleOutputSnapshot(options.outputPath);
		// 调用核心执行函数：这里才会：构造 Pi CLI 参数 → spawn 子进程 → 解析 JSONL → 等待退出
		const result = await runSingleAttempt(runtimeCwd, agent, taskWithAcceptance, candidate, options, {
			sessionEnabled,
			systemPrompt,
			resolvedSkillNames: resolvedSkills.length > 0 ? resolvedSkills.map((skill) => skill.name) : undefined,
			skillsWarning: missingSkills.length > 0 ? `Skills not found: ${missingSkills.join(", ")}` : undefined,
			jsonlPath,
			artifactPaths: artifactPathsResult,
			transcriptWriter,
			attemptNotes,
			outputSnapshot,
			originalTask: task,
		});
		// 聚合每次 Attempt，保存最近一次结果
		lastResult = result;
		// 记录实际使用模型
		if (result.model) attemptedModels.push(result.model);
		else if (candidate) attemptedModels.push(candidate);
		// 累加 Usage
		sumUsage(aggregateUsage, result.usage);
		// 累加子 Agent 实际执行的工具调用次数和总耗时
		totalToolCount += result.progressSummary?.toolCount ?? 0;
		totalDurationMs += result.progressSummary?.durationMs ?? 0;
		// 判断本次 Attempt 是否成功
		const attemptSucceeded = result.exitCode === 0 && !result.error;
		// 构造 ModelAttempt 对象，用于记录本次尝试的模型、成功与否、退出码、错误信息、Usage
		const attempt: ModelAttempt = {
			model: result.model ?? candidate ?? agent.model ?? "default",
			success: attemptSucceeded,
			exitCode: result.exitCode,
			error: result.error,
			usage: { ...result.usage },
		};
		// 记录本次尝试的模型、成功与否、退出码、错误信息、Usage
		modelAttempts.push(attempt);
		// 如果子 Agent 因中断、超时、超出预算而退出，则直接跳出循环，不再尝试其他模型，因为这些不是模型 Provider 故障
		if (result.detached || result.timedOut || result.turnBudgetExceeded) {
			break;
		}
		// 如果本次 Attempt 成功，则直接跳出循环，因为已经找到了成功的模型
		if (attemptSucceeded) {
			break;
		}
		// 如果本次 Attempt 失败，则判断是否可以重试，如果不能重试，则直接跳出循环，因为已经尝试了所有模型
		if (!isRetryableModelFailure(result.error) || i === modelsToTry.length - 1) {
			break;
		}
		// 如果本次 Attempt 失败，则记录本次尝试的模型、成功与否、退出码、错误信息、Usage
		attemptNotes.push(formatModelAttemptNote(attempt, modelsToTry[i + 1]));
	}

	// 理论上循环至少执行一次，但仍提供兜底结果
	const result = lastResult ?? {
		agent: agentName,
		task,
		exitCode: 1,
		messages: [],
		usage: emptyUsage(),
		error: "Subagent did not produce a result.",
	} satisfies SingleResult;

	// 将跨 Attempt 的聚合结果覆盖进去
	result.usage = aggregateUsage;
	result.attemptedModels = attemptedModels.length > 0 ? attemptedModels : undefined;
	result.modelAttempts = modelAttempts.length > 0 ? modelAttempts : undefined;
	// 计算本次调用的总工具调用次数、总 Token 数、总耗时
	result.progressSummary = {
		toolCount: totalToolCount,
		tokens: aggregateUsage.input + aggregateUsage.output,
		durationMs: totalDurationMs,
	};
	// 将本次调用的尝试笔记合并到最近输出中
	if (attemptNotes.length > 0 && result.progress) {
		result.progress.recentOutput = [...attemptNotes, ...result.progress.recentOutput];
		if (result.progress.recentOutput.length > 50) {
			result.progress.recentOutput.splice(50);
		}
	}

	// 记录 Transcript 路径
	if (transcriptWriter) result.transcriptPath = artifactPathsResult?.transcriptPath;
	// 记录 Transcript 错误
	if (transcriptWriter?.getError()) result.transcriptError = transcriptWriter.getError();

	// 记录 Artifact 路径和内容
	if (artifactPathsResult && options.artifactConfig?.enabled !== false) {
		// 记录 Artifact 路径
		result.artifactPaths = artifactPathsResult;
		// 记录 Output 内容
		if (options.artifactConfig?.includeOutput !== false) {
			// 写入 Output 内容
			writeArtifact(artifactPathsResult.outputPath, artifactOutputByResult.get(result) ?? result.finalOutput ?? "");
		}
		// 记录 Metadata 内容
		if (options.artifactConfig?.includeMetadata !== false) {
			writeMetadata(artifactPathsResult.metadataPath, {
				runId: options.runId,
				agent: agentName,
				task,
				exitCode: result.exitCode,
				usage: result.usage,
				model: result.model,
				attemptedModels: result.attemptedModels,
				modelAttempts: result.modelAttempts,
				durationMs: result.progressSummary?.durationMs,
				toolCount: result.progressSummary?.toolCount,
				error: result.error,
				...(transcriptWriter ? { transcriptPath: artifactPathsResult.transcriptPath } : {}),
				transcriptError: result.transcriptError,
				skills: result.skills,
				skillsWarning: result.skillsWarning,
				timestamp: Date.now(),
			});
		}

		// 记录 Output 截断信息
		if (options.maxOutput) {
			const config = { ...DEFAULT_MAX_OUTPUT, ...options.maxOutput };
			// 返回给父 Agent 的内容会被截断，但完整输出仍可以保存在 Artifact 文件里。这样避免子 Agent 结果占满父 Agent Context
			// 开头的 maxOutput 行进入父 Agent Context，Artifact 作为可查询的外部记忆。当前实现已经提供了 Artifact 路径，但是否主动读取仍由父 Agent判断，所以输出协议和父 Agent提示词同样重要
			const truncationResult = truncateOutput(result.finalOutput ?? "", config, artifactPathsResult.outputPath);
			if (truncationResult.truncated) result.truncation = truncationResult;
		}
	} else if (options.maxOutput) {
		const config = { ...DEFAULT_MAX_OUTPUT, ...options.maxOutput };
		const truncationResult = truncateOutput(result.finalOutput ?? "", config);
		if (truncationResult.truncated) result.truncation = truncationResult;
	}

	// 记录 Session 文件路径，如果调用方提供了明确 sessionFile，existsSync 是 Node.js 的 fs.existsSync 方法，同步检查文件是否存在
	// 或者子 Agent 产生了消息，也要记录 Session 文件路径
	// 如果文件不存在，也没有消息，说明子 Agent 可能还没真正启动就失败了，因此不把这个路径写进结果。
	if (options.sessionFile && (existsSync(options.sessionFile) || result.messages?.length)) {
		result.sessionFile = options.sessionFile;
	} else if (shareEnabled && options.sessionDir) { // 如果使用共享 Session 目录，找到子 Agent 实际创建的 JSONL 文件并记录
		const sessionFile = findLatestSessionFile(options.sessionDir);
		if (sessionFile) result.sessionFile = sessionFile;
	}

	// 某些状态不能验收，例如：中断、超时、超出预算，这些情况生成 skipped/rejected ledger
	// 正常情况调用 evaluateAcceptance() 进行验收，它可能检查：
	// - Agent 是否输出验收报告
	// - 必要结果字段是否存在
	// - 测试/构建命令是否通过
	// - 自定义 Verify Command 是否成功
	result.acceptance = result.detached
		? buildSkippedAcceptanceLedger(effectiveAcceptance, { id: "detached", message: "Acceptance was not evaluated because the subagent detached for intercom coordination before task completion." })
		: result.timedOut
			? buildSkippedAcceptanceLedger(effectiveAcceptance, { id: "timeout", message: "Acceptance was not evaluated because the subagent timed out." })
			: result.turnBudgetExceeded
			? buildSkippedAcceptanceLedger(effectiveAcceptance, { id: "turn-budget", message: "Acceptance was not evaluated because the subagent exceeded its turn budget." })
			: await evaluateAcceptance({
			acceptance: effectiveAcceptance,
			output: acceptanceOutputByResult.get(result) ?? result.finalOutput ?? "",
			cwd: options.cwd ?? runtimeCwd,
		});
	// 如果验收失败，则构造失败原因消息
	const acceptanceFailure = acceptanceFailureMessage(result.acceptance);
	// 然后从对外消息中移除内部验收报告
	stripAcceptanceReportsFromMessages(result.messages);
	// 如果是显式 Acceptance，并且验收失败，也就是说：子 Agent 自己认为成功 ≠ 父 Agent 最终判定成功，必须通过确定性验收。
	if (acceptanceFailure && result.acceptance.explicit && result.exitCode === 0 && !result.detached && !result.interrupted && !result.timedOut) {
		result.exitCode = 1;
		result.error = result.error ? `${result.error}\n${acceptanceFailure}` : acceptanceFailure;
		if (result.progress) {
			result.progress.status = "failed";
			result.progress.error = result.error;
		}
	}

	// 返回结果
	// 上层可能是：
	// runSinglePath()：直接包装成 Tool Result。
	// Parallel：与其他 SingleResult 聚合。
	// Chain：作为下一步骤的输入。
	return result;
}
