import assert from "node:assert/strict";
import test from "node:test";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	TASK_NOTIFICATION_TYPE,
	TaskCoordinator,
	registerTaskCoordinator,
	type TaskCoordinatorOptions,
	type TaskNotificationUpdate,
	type TaskSource,
} from "../src/index.js";

class Bus {
	private readonly listeners = new Map<string, Set<(data: unknown) => void>>();

	emit(channel: string, data: unknown): void {
		for (const listener of this.listeners.get(channel) ?? []) listener(data);
	}

	on(channel: string, listener: (data: unknown) => void): () => void {
		const listeners = this.listeners.get(channel) ?? new Set();
		listeners.add(listener);
		this.listeners.set(channel, listeners);
		return () => listeners.delete(listener);
	}
}

interface HarnessExtension {
	coordinator: TaskCoordinator;
	handlers: Map<string, Array<(event: any, ctx: ExtensionContext) => unknown>>;
	renderers: Map<string, (message: any, options: any, theme: any) => any>;
}

interface Harness {
	extensions: HarnessExtension[];
	messages: Array<{ owner: TaskSource; message: any; options: any }>;
	widgetCalls: number;
	ctx: ExtensionContext;
	start(index: number): void;
	setIdle(value: boolean): void;
	dispatch(type: string, event: unknown): Promise<void>;
	flush(): void;
	close(): void;
}

function update(source: TaskSource, suffix: string, event: "ready" | "terminal" = "terminal"): TaskNotificationUpdate {
	return {
		eventId: `${source}:instance-${suffix}:${event}`,
		taskKey: `${source}:instance-${suffix}`,
		source,
		taskId: `${source.slice(0, 2)}_${suffix}`,
		event,
		status: event === "ready" ? "ready" : "completed",
		durationMs: 1_700,
		summary: `${source} task ${suffix}`,
		output: `line one\nline two`,
		ok: true,
	};
}

async function waitUntil(predicate: () => boolean, timeoutMs = 1_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!predicate() && Date.now() < deadline) {
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
	assert.equal(predicate(), true, "condition was not reached before timeout");
}

function harness(
	sources: TaskSource[],
	send?: (owner: TaskSource, message: any, options: any) => void,
	autoStart = true,
	coordinatorOptions: TaskCoordinatorOptions = {},
): Harness {
	const bus = new Bus();
	const messages: Harness["messages"] = [];
	let widgetCalls = 0;
	const extensions: HarnessExtension[] = [];
	const contexts: ExtensionContext[] = [];
	let idle = true;
	const ctx = {
		hasUI: true,
		mode: "tui",
		isIdle: () => idle,
		ui: {
			setWidget: () => {},
		},
	} as unknown as ExtensionContext;

	for (const source of sources) {
		const handlers = new Map<string, Array<(event: any, ctx: ExtensionContext) => unknown>>();
		const renderers = new Map<string, (message: any, options: any, theme: any) => any>();
		const pi = {
			events: {
				emit: (channel: string, data: unknown) => bus.emit(channel, data),
				on: (channel: string, listener: (data: unknown) => void) => bus.on(channel, listener),
			},
			on: (type: string, handler: (event: any, context: ExtensionContext) => unknown) => {
				const values = handlers.get(type) ?? [];
				values.push(handler);
				handlers.set(type, values);
			},
			registerMessageRenderer: (type: string, renderer: (message: any, options: any, theme: any) => any) => {
				renderers.set(type, renderer);
			},
			sendMessage: (message: any, options: any) => {
				if (send) send(source, message, options);
				messages.push({ owner: source, message, options });
			},
		} as unknown as ExtensionAPI;
		const coordinator = registerTaskCoordinator(pi, source, {
			coalesceMs: 60_000,
			heartbeatMs: 0,
			participantStaleMs: 60_000,
			...coordinatorOptions,
		});
		extensions.push({ coordinator, handlers, renderers });
	}

	for (const extension of extensions) {
		const source = extension.coordinator.source;
		contexts.push({
			...ctx,
			ui: {
				setWidget: () => { widgetCalls++; },
			},
		} as unknown as ExtensionContext);
	}
	const start = (index: number) => extensions[index]!.coordinator.startSession(contexts[index]!, "session-one");
	if (autoStart) for (let index = 0; index < extensions.length; index++) start(index);

	return {
		extensions,
		messages,
		get widgetCalls() { return widgetCalls; },
		ctx,
		start,
		setIdle(value) {
			idle = value;
		},
		async dispatch(type, event) {
			for (const extension of extensions) {
				for (const handler of extension.handlers.get(type) ?? []) await handler(event, ctx);
			}
		},
		flush() {
			for (const extension of extensions) extension.coordinator.flushNow();
		},
		close() {
			for (const extension of extensions) extension.coordinator.closeSession();
		},
	};
}

const combinations: TaskSource[][] = [
	["python"],
	["pwsh"],
	["subagent"],
	["python", "pwsh"],
	["python", "subagent"],
	["pwsh", "subagent"],
	["python", "pwsh", "subagent"],
];

for (const sources of combinations) {
	test(`aggregates one message for ${sources.join("+")}`, async () => {
		const state = harness(sources);
		try {
			let submitted = 0;
			for (const [index, extension] of state.extensions.entries()) {
				extension.coordinator.offer(update(extension.coordinator.source, String(index)), {
					onSubmitted: () => { submitted++; },
				});
			}
			state.flush();
			await new Promise((resolve) => setImmediate(resolve));
			assert.equal(state.messages.length, 1);
			assert.equal(state.messages[0]!.message.details.tasks.length, sources.length);
			assert.equal(state.messages[0]!.message.customType, TASK_NOTIFICATION_TYPE);
			assert.equal(state.messages[0]!.options.triggerTurn, true);
			assert.equal(submitted, sources.length);
		} finally {
			state.close();
		}
	});
}

test("a successful explicit presentation removes only that pending task", () => {
	const state = harness(["python", "pwsh"]);
	try {
		let withdrawn = 0;
		const python = state.extensions[0]!.coordinator;
		const pwsh = state.extensions[1]!.coordinator;
		const py = update("python", "one");
		python.offer(py, { onWithdrawn: (reason) => {
			assert.equal(reason, "presented");
			withdrawn++;
		} });
		pwsh.offer(update("pwsh", "two"));
		python.withdrawTask(py.taskKey, ["terminal"], "presented");
		state.flush();
		assert.equal(withdrawn, 1);
		assert.equal(state.messages.length, 1);
		assert.deepEqual(state.messages[0]!.message.details.tasks.map((task: TaskNotificationUpdate) => task.source), ["pwsh"]);
	} finally {
		state.close();
	}
});

test("withdrawing every viewed result produces no notification", () => {
	const state = harness(["python", "pwsh", "subagent"]);
	try {
		for (const [index, extension] of state.extensions.entries()) {
			const value = update(extension.coordinator.source, String(index));
			extension.coordinator.offer(value);
			extension.coordinator.withdrawTask(value.taskKey, ["terminal"], "presented");
		}
		state.flush();
		assert.equal(state.messages.length, 0);
	} finally {
		state.close();
	}
});

test("a late higher-priority participant recovers offers and holds through probe replay without setting a widget", () => {
	const state = harness(["pwsh", "python"], undefined, false);
	try {
		state.start(0);
		const pwsh = state.extensions[0]!.coordinator;
		const task = update("pwsh", "early");
		const release = pwsh.holdTask(task.taskKey);
		pwsh.offer(task);
		state.start(1);
		state.flush();
		assert.equal(state.messages.length, 0);
		assert.equal(state.widgetCalls, 0);
		release();
		state.flush();
		assert.equal(state.messages.length, 1);
		assert.equal(state.messages[0]!.owner, "python");
		assert.equal(state.messages[0]!.message.details.tasks[0].taskId, task.taskId);
	} finally {
		state.close();
	}
});

test("terminal supersedes a ready event that has not been submitted", () => {
	const state = harness(["pwsh"]);
	try {
		const coordinator = state.extensions[0]!.coordinator;
		const ready = update("pwsh", "transition", "ready");
		let reason: string | undefined;
		coordinator.offer(ready, { onWithdrawn: (value) => { reason = value; } });
		coordinator.offer(update("pwsh", "transition", "terminal"));
		state.flush();
		assert.equal(reason, "superseded");
		assert.equal(state.messages.length, 1);
		assert.deepEqual(state.messages[0]!.message.details.tasks.map((task: TaskNotificationUpdate) => task.event), ["terminal"]);
	} finally {
		state.close();
	}
});

test("task holds prevent the query race without blocking another plugin", () => {
	const state = harness(["python", "pwsh"]);
	try {
		const python = state.extensions[0]!.coordinator;
		const pwsh = state.extensions[1]!.coordinator;
		const py = update("python", "held");
		const release = python.holdTask(py.taskKey);
		python.offer(py);
		pwsh.offer(update("pwsh", "free"));
		state.flush();
		assert.equal(state.messages.length, 1);
		assert.deepEqual(state.messages[0]!.message.details.tasks.map((task: TaskNotificationUpdate) => task.source), ["pwsh"]);
		release();
		state.flush();
		assert.equal(state.messages.length, 2);
		assert.deepEqual(state.messages[1]!.message.details.tasks.map((task: TaskNotificationUpdate) => task.source), ["python"]);
	} finally {
		state.close();
	}
});

test("active turns keep notifications cancellable until tool results have been presented", async () => {
	const state = harness(["pwsh"]);
	try {
		state.setIdle(false);
		const coordinator = state.extensions[0]!.coordinator;
		const task = update("pwsh", "wait-race");
		coordinator.offer(task);
		state.flush();
		assert.equal(state.messages.length, 0);

		coordinator.withdrawTask(task.taskKey, ["terminal"], "presented");
		await state.dispatch("turn_end", { type: "turn_end" });
		assert.equal(state.messages.length, 0);
	} finally {
		state.close();
	}
});

test("turn_end delivers an unclaimed active-turn notification before the next model call", async () => {
	const state = harness(["pwsh"]);
	try {
		state.setIdle(false);
		state.extensions[0]!.coordinator.offer(update("pwsh", "turn-boundary"));
		state.flush();
		assert.equal(state.messages.length, 0);

		await state.dispatch("turn_end", { type: "turn_end" });
		assert.equal(state.messages.length, 1);
		assert.equal(state.messages[0]!.message.details.tasks[0].taskId, "pw_turn-boundary");
	} finally {
		state.close();
	}
});

test("agent_settled flushes an active notification when no turn boundary consumes it", async () => {
	const state = harness(["subagent"]);
	try {
		state.setIdle(false);
		state.extensions[0]!.coordinator.offer(update("subagent", "settled"));
		state.flush();
		assert.equal(state.messages.length, 0);

		state.setIdle(true);
		await state.dispatch("agent_settled", { type: "agent_settled" });
		assert.equal(state.messages.length, 1);
		assert.equal(state.messages[0]!.message.details.tasks[0].taskId, "su_settled");
	} finally {
		state.close();
	}
});

test("agent_settled keeps work pending if an earlier extension starts another run", async () => {
	const state = harness(["python"]);
	try {
		state.setIdle(false);
		state.extensions[0]!.coordinator.offer(update("python", "settled-order"));
		state.flush();
		assert.equal(state.messages.length, 0);

		state.setIdle(true);
		state.extensions[0]!.handlers.get("agent_settled")!.unshift(() => state.setIdle(false));
		await state.dispatch("agent_settled", { type: "agent_settled" });
		assert.equal(state.messages.length, 0);

		await state.dispatch("turn_end", { type: "turn_end" });
		assert.equal(state.messages.length, 1);
	} finally {
		state.close();
	}
});

test("a coalescing timer that fires while active recovers at agent_settled", async () => {
	const state = harness(["python"], undefined, true, { coalesceMs: 5 });
	try {
		state.setIdle(false);
		state.extensions[0]!.coordinator.offer(update("python", "busy-timer"));
		await new Promise((resolve) => setTimeout(resolve, 15));
		assert.equal(state.messages.length, 0);

		state.setIdle(true);
		await state.dispatch("agent_settled", { type: "agent_settled" });
		assert.equal(state.messages.length, 1);
	} finally {
		state.close();
	}
});

test("turn boundaries preserve delivery retry backoff", async () => {
	let attempts = 0;
	const state = harness(["pwsh"], () => {
		attempts++;
		throw new Error("queue unavailable");
	}, true, { deliveryRetryMs: 20, maxDeliveryAttempts: 2 });
	try {
		state.setIdle(false);
		state.extensions[0]!.coordinator.offer(update("pwsh", "boundary-retry"));
		await assert.rejects(state.dispatch("turn_end", { type: "turn_end" }), /queue unavailable/);
		assert.equal(attempts, 1);

		await state.dispatch("turn_end", { type: "turn_end" });
		assert.equal(attempts, 1);

		await new Promise((resolve) => setTimeout(resolve, 25));
		await assert.rejects(state.dispatch("turn_end", { type: "turn_end" }), /queue unavailable/);
		assert.equal(attempts, 2);
	} finally {
		state.close();
	}
});

test("stale participants cannot leave tasks permanently held", () => {
	const state = harness(["python", "pwsh"], undefined, true, { participantStaleMs: 10 });
	try {
		const python = state.extensions[0]!.coordinator;
		const pwsh = state.extensions[1]!.coordinator;
		const task = update("pwsh", "abandoned-hold");
		pwsh.holdTask(task.taskKey);
		pwsh.offer(task);

		const participants = (python as any).participants as Map<string, { seenAt: number }>;
		participants.get(pwsh.participantId)!.seenAt = Date.now() - 100;
		state.flush();

		assert.equal(state.messages.length, 1);
		assert.equal(state.messages[0]!.message.details.tasks[0].taskId, task.taskId);
	} finally {
		state.close();
	}
});

test("message_end acknowledges every source in an aggregated delivery", async () => {
	const state = harness(["python", "pwsh", "subagent"]);
	try {
		const delivered: TaskSource[] = [];
		for (const [index, extension] of state.extensions.entries()) {
			const source = extension.coordinator.source;
			extension.coordinator.offer(update(source, String(index)), {
				onDelivered: () => { delivered.push(source); },
			});
		}
		state.flush();
		const sent = state.messages[0]!.message;
		await state.dispatch("message_end", {
			type: "message_end",
			message: {
				role: "custom",
				customType: sent.customType,
				details: sent.details,
				content: sent.content,
				display: true,
				timestamp: Date.now(),
			},
		});
		await new Promise((resolve) => setImmediate(resolve));
		assert.deepEqual(delivered.sort(), ["pwsh", "python", "subagent"]);
	} finally {
		state.close();
	}
});

test("accepted updates retry boundedly when message_end never arrives", async () => {
	const state = harness(["python"], undefined, true, {
		deliveryRetryMs: 5,
		maxDeliveryAttempts: 2,
	});
	try {
		let withdrawal: string | undefined;
		state.extensions[0]!.coordinator.offer(update("python", "retry"), {
			onWithdrawn: (reason) => { withdrawal = reason; },
		});
		state.flush();
		assert.equal(state.messages.length, 1);
		await state.dispatch("agent_settled", { type: "agent_settled" });
		await waitUntil(() => {
			state.flush();
			return state.messages.length === 2;
		});
		assert.equal(state.messages.length, 2);
		await state.dispatch("agent_settled", { type: "agent_settled" });
		await waitUntil(() => withdrawal === "retry-exhausted");
		state.flush();
		assert.equal(state.messages.length, 2);
		assert.equal(withdrawal, "retry-exhausted");
	} finally {
		state.close();
	}
});

test("delivery acknowledgement does not depend on a fresh leader lease", async () => {
	const state = harness(["python"], undefined, true, { participantStaleMs: 1 });
	try {
		let delivered = 0;
		state.extensions[0]!.coordinator.offer(update("python", "stale-ack"), {
			onDelivered: () => { delivered++; },
		});
		state.flush();
		const sent = state.messages[0]!.message;
		await new Promise((resolve) => setTimeout(resolve, 5));
		await state.dispatch("message_end", {
			type: "message_end",
			message: { role: "custom", customType: sent.customType, details: sent.details },
		});
		await new Promise((resolve) => setImmediate(resolve));
		assert.equal(delivered, 1);
	} finally {
		state.close();
	}
});

test("source callback failures remain isolated from coordination", async () => {
	const state = harness(["python", "pwsh"]);
	try {
		const first = update("python", "throw");
		const second = update("pwsh", "reject");
		state.extensions[0]!.coordinator.offer(first, { onSubmitted: () => { throw new Error("callback throw"); } });
		state.extensions[1]!.coordinator.offer(second, { onSubmitted: async () => { throw new Error("callback reject"); } });
		assert.doesNotThrow(() => state.flush());
		await new Promise((resolve) => setImmediate(resolve));
		assert.equal(state.messages.length, 1);
		assert.equal(state.messages[0]!.message.details.tasks.length, 2);
	} finally {
		state.close();
	}
});

test("delivery persistence is serialized after submission and retried on callback failure", async () => {
	const state = harness(["python"], undefined, true, { deliveryRetryMs: 5 });
	try {
		const order: string[] = [];
		let deliveredAttempts = 0;
		state.extensions[0]!.coordinator.offer(update("python", "ordered"), {
			onSubmitted: async () => {
				await new Promise((resolve) => setTimeout(resolve, 5));
				order.push("submitted");
			},
			onDelivered: () => {
				deliveredAttempts++;
				assert.deepEqual(order, ["submitted"]);
				if (deliveredAttempts === 1) throw new Error("transient marker failure");
				order.push("delivered");
			},
		});
		state.flush();
		const sent = state.messages[0]!.message;
		await state.dispatch("message_end", {
			type: "message_end",
			message: { role: "custom", customType: sent.customType, details: sent.details },
		});
		await waitUntil(() => deliveredAttempts === 2);
		assert.equal(deliveredAttempts, 2);
		assert.deepEqual(order, ["submitted", "delivered"]);
	} finally {
		state.close();
	}
});

test("a synchronous enqueue failure rolls the whole batch back with bounded retry", async () => {
	let attempts = 0;
	const state = harness(["python", "pwsh"], () => {
		attempts++;
		if (attempts === 1) throw new Error("injected failure");
	}, true, { deliveryRetryMs: 5 });
	try {
		for (const [index, extension] of state.extensions.entries()) {
			extension.coordinator.offer(update(extension.coordinator.source, String(index)));
		}
		assert.throws(() => state.flush(), /injected failure/);
		await waitUntil(() => {
			state.flush();
			return attempts === 2;
		});
		assert.equal(attempts, 2);
		assert.equal(state.messages.length, 1);
		assert.equal(state.messages[0]!.message.details.tasks.length, 2);
	} finally {
		state.close();
	}
});

test("repeated synchronous enqueue failure stops at max attempts and releases the source claim", async () => {
	let attempts = 0;
	const state = harness(["python"], () => {
		attempts++;
		throw new Error("persistent failure");
	}, true, { deliveryRetryMs: 5, maxDeliveryAttempts: 2 });
	try {
		let withdrawal: string | undefined;
		state.extensions[0]!.coordinator.offer(update("python", "bounded"), {
			onWithdrawn: (reason) => { withdrawal = reason; },
		});
		assert.throws(() => state.flush(), /persistent failure/);
		await waitUntil(() => attempts === 2 && withdrawal === "retry-exhausted");
		assert.doesNotThrow(() => state.flush());
		assert.equal(attempts, 2);
		assert.equal(withdrawal, "retry-exhausted");
	} finally {
		state.close();
	}
});

test("shared notification rendering contains no spacer rows", () => {
	const state = harness(["python"]);
	try {
		const renderer = state.extensions[0]!.renderers.get(TASK_NOTIFICATION_TYPE)!;
		const value = update("python", "render");
		const component = renderer(
			{ details: { deliveryId: "d", eventIds: [value.eventId], tasks: [value] } },
			{ expanded: false },
			{ fg: (_tone: string, text: string) => text },
		);
		assert.deepEqual(component.render(120).map((line: string) => line.trimEnd()), [
			"● python py_render completed · 1.7s",
			"  python task render",
			"  line one",
			"  line two",
		]);
	} finally {
		state.close();
	}
});
