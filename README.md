# pi-task-coordinator

Shared, low-noise background-task notifications for the `@4fu` Pi extensions.

This is an internal runtime library rather than a Pi extension. `pi-python`, `pi-pwsh`, and `pi-subagent` install it transitively and cooperate through Pi's extension event bus. Any one plugin works by itself; any installed combination elects one notification coordinator, batches task updates, and renders one active-task widget.

Task execution and durable state remain owned by the originating plugin. The coordinator only handles short-lived aggregation, presentation withdrawal, leader election, and shared TUI rendering.

## Requirements

- Pi `0.84.1` or newer
- Node.js `22.19.0` or newer

## License

MIT
