# pi-task-coordinator

Shared, low-noise background-task notification aggregation and delivery for the `@4fu` Pi extensions.

This is an internal runtime library rather than a Pi extension. `pi-python`, `pi-pwsh`, and `pi-subagent` install it transitively and cooperate through Pi's extension event bus. Any one plugin works by itself; any installed combination elects one notification coordinator and batches task updates for delivery.

Task execution and durable state remain owned by the originating plugin. This package owns notification aggregation and delivery only, including presentation withdrawal and leader election. The shared task TUI and `/tasks` command live in `@4fu/pi-tasks`.

Task plugins should be upgraded together for this presentation split.

## Requirements

- Pi `0.84.1` or newer
- Node.js `22.19.0` or newer

## License

MIT
