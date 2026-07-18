export interface RunnerSubagentStep {
	/** Session id of the direct parent session for permission-system ask forwarding. */
	parentSessionId?: string;
	agent: string;
	task: string;
	importAsyncRoot?: {
		runId: string;
		asyncDir: string;
		resultPath: string;
		index: number;
	};
	phase?: string;
	label?: string;
	outputName?: string;
	structured?: boolean;
	cwd?: string;
	model?: string;
	thinking?: string;
	modelCandidates?: string[];
	tools?: string[];
	extensions?: string[];
	subagentOnlyExtensions?: string[];
	mcpDirectTools?: string[];
	completionGuard?: boolean;
	systemPrompt?: string | null;
	systemPromptMode?: "append" | "replace";
	inheritProjectContext: boolean;
	inheritSkills: boolean;
	skills?: string[];
	outputPath?: string;
	outputMode?: "inline" | "file-only";
	sessionFile?: string;
	maxSubagentDepth?: number;
	structuredOutput?: {
		schema: import("../../shared/types.ts").JsonSchemaObject;
		schemaPath: string;
		outputPath: string;
	};
	structuredOutputSchema?: import("../../shared/types.ts").JsonSchemaObject;
	effectiveAcceptance?: import("../../shared/types.ts").ResolvedAcceptanceConfig;
	toolBudget?: import("../../shared/types.ts").ResolvedToolBudget;
}

export interface ParallelStepGroup {
	parallel: RunnerSubagentStep[];
	concurrency?: number;
	failFast?: boolean;
	worktree?: boolean;
}

export interface DynamicRunnerGroup {
	expand: import("../../shared/settings.ts").DynamicExpandSpec;
	parallel: RunnerSubagentStep;
	collect: import("../../shared/settings.ts").DynamicCollectSpec;
	concurrency?: number;
	failFast?: boolean;
	phase?: string;
	label?: string;
	sessionFiles?: (string | undefined)[];
	thinkingOverrides?: (string | undefined)[];
	effectiveAcceptance?: import("../../shared/types.ts").ResolvedAcceptanceConfig;
}

export type RunnerStep = RunnerSubagentStep | ParallelStepGroup | DynamicRunnerGroup;

export function isParallelGroup(step: RunnerStep): step is ParallelStepGroup {
	return "parallel" in step && Array.isArray(step.parallel);
}

export function isDynamicRunnerGroup(step: RunnerStep): step is DynamicRunnerGroup {
	return "expand" in step && "collect" in step && "parallel" in step && !Array.isArray((step as { parallel?: unknown }).parallel);
}

export function flattenSteps(steps: RunnerStep[]): RunnerSubagentStep[] {
	const flat: RunnerSubagentStep[] = [];
	for (const step of steps) {
		if (isParallelGroup(step)) {
			for (const task of step.parallel) flat.push(task);
		} else if (isDynamicRunnerGroup(step)) {
			continue;
		} else {
			flat.push(step);
		}
	}
	return flat;
}

export const DEFAULT_GLOBAL_CONCURRENCY_LIMIT = 20;

/**
 * 一个基于 Promise 实现的信号量（Semaphore），用于在单次运行中，跨多个 mapConcurrent 调用限制并发访问
 * 它强制执行一个全局上限，控制同时执行的子 Agent 任务总数，而不受每个步骤自身并发限制的影响
 */
export class Semaphore {
	// 当前可用许可数（初始为 limit），大于 0 表示可以立即执行；等于 0 表示已满，新任务需要等待
	private available: number;
	// 等待队列（存储 resolve 函数），当有任务被阻塞时，它的 resolve 会被存入队列，等待释放时被调用
	private readonly queue: Array<() => void> = [];

	constructor(limit: number) {
		// 初始化可用许可数，确保至少为 1
		this.available = Math.max(1, Math.floor(limit) || 1);
	}

	// 获取一个许可，如果当前有可用许可，则立即返回；否则将当前任务阻塞，等待许可释放
	acquire(): Promise<void> {
		if (this.available > 0) {
			this.available--;
			return Promise.resolve();
		}
		// 如果当前没有可用许可，则创建一个 Promise，并将其 resolve 函数存入等待队列
		return new Promise<void>((resolve) => {
			this.queue.push(resolve);
		});
	}

	// 释放一个许可，如果等待队列中有任务，则唤醒第一个等待者；否则增加可用许可数
	release(): void {
		// 从等待队列中取出第一个等待者，并调用其 resolve 函数
		const next = this.queue.shift();
		if (next) {
			next();
		} else {
			this.available++;
		}
	}
}

export async function mapConcurrent<T, R>(
	items: T[],
	limit: number,
	fn: (item: T, i: number) => Promise<R>,
	globalSemaphore?: Semaphore,
): Promise<R[]> {
	const safeLimit = Math.max(1, Math.floor(limit) || 1);
	const results: R[] = new Array(items.length);
	let next = 0;

	async function worker(_workerIndex: number): Promise<void> {
		while (next < items.length) {
			const i = next++;
			if (globalSemaphore) {
				await globalSemaphore.acquire();
				try {
					results[i] = await fn(items[i], i);
				} finally {
					globalSemaphore.release();
				}
			} else {
				results[i] = await fn(items[i], i);
			}
		}
	}

	await Promise.all(
		Array.from({ length: Math.min(safeLimit, items.length) }, (_, wi) => worker(wi)),
	);
	return results;
}

export interface ParallelTaskResult {
	agent: string;
	taskIndex?: number;
	output: string;
	exitCode: number | null;
	error?: string;
	timedOut?: boolean;
	model?: string;
	attemptedModels?: string[];
	outputTargetPath?: string;
	outputTargetExists?: boolean;
}

export function aggregateParallelOutputs(
	results: ParallelTaskResult[],
	headerFormat: (index: number, agent: string) => string = (i, agent) =>
		`=== Parallel Task ${i + 1} (${agent}) ===`,
): string {
	return results
		.map((r, i) => {
			const header = headerFormat(r.taskIndex ?? i, r.agent);
			const hasOutput = Boolean(r.output?.trim());
			const status =
				r.timedOut
					? `TIMED OUT${r.error ? `: ${r.error}` : ""}`
					: r.exitCode === -1
					? "SKIPPED"
					: r.exitCode !== 0 && r.exitCode !== null
						? `FAILED (exit code ${r.exitCode})${r.error ? `: ${r.error}` : ""}`
						: r.error
							? `WARNING: ${r.error}`
							: !hasOutput && r.outputTargetPath && r.outputTargetExists === false
								? `EMPTY OUTPUT (expected output file missing: ${r.outputTargetPath})`
								: !hasOutput && !r.outputTargetPath
									? "EMPTY OUTPUT (no textual response returned)"
							: "";
			const body = status ? (hasOutput ? `${status}\n${r.output}` : status) : r.output;
			return `${header}\n${body}`;
		})
		.join("\n\n");
}

export const MAX_PARALLEL_CONCURRENCY = 4;
