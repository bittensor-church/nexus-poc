# Nexus

Opinionated event-driven actor framework for building Bittensor subnets. Python, strict typing, thread-based
concurrency, and message-based IPC.

## Knowledge base

Before any task, read `docs/nexus.md`. It is the grounding document for all decision
making and work within the Nexus project.

The knowledge base is tailored both towards the developers and users of the Nexus framework.

## Tech

- Python 3.14
- uv as package manager (no pip)
- pytest, basedpyright
- ruff for linting and formatting
- litestar for HTTP endpoints (https://docs.litestar.dev/latest/)
- pylon for subtensor communication (https://github.com/bittensor-church/bittensor-pylon) - no `bittensor` dependency

## QA tools

These tools are configured to comply with project conventions.
Full-project QA MUST be run from the repository root through nox.
Focused package checks may be run with `uv run` from that package directory.
ALWAYS use these instead of manual checks.

```sh
uvx nox -vs lint              # root lint, formatting check, type check, docs/shell checks
uvx nox -vs test-3.14         # root tests
uvx nox -vs cat-images-lint   # cat-images demo lint, formatting check, type check
uvx nox -vs cat-images-test   # cat-images demo tests
```

Always use `uv` or `uvx`, never bare `python` or `pip`.

## Coding guidelines

- All imports MUST go at the top of a file. No inline imports.
- Public consumers MUST import Nexus interfaces from `nexus.v1`. Implementation modules live under
  `nexus._internal`; do not import from legacy public paths such as `nexus.actors`, `nexus.core`, `nexus.utils`, or
  `nexus.nexus_validator`.
- Wire each source with at most one primary and explicit taps. The primary keeps the current context; every tap gets
  an independent child context. Use taps-only wiring for broadcasts and subnet-clock consumers.
- Use `.then(primary, taps=[...])`, `.then(taps=[...])`, or named `Targets(...)`. Do not use legacy `.then(a, b)` or
  iterable named-route values.
- Prefer short and concise code. Avoid deeply nested code.
- Code MUST be well-typed. Prefer typed structures over dictionaries.
- Be strict with domain types: avoid bare `str`/`int`/`float` for domain values when a stronger type exists.
- For durations use `datetime.timedelta` (not raw numeric seconds) unless there is a strong reason not to.
- Introduce `NewType` or small typed wrappers for semantically distinct values (paths, ports, IDs, timeouts) to prevent argument mix-ups and make APIs self-explanatory.
- Use short and descriptive variable, function, and class names.
- Comments MUST NOT reiterate what the code does. If the code is descriptive enough - skip comments.
- DO write comments that explain non-obvious code, gotchas.
- Do NOT write overly defensive code, NEVER use hasattr/getattr to inspect objects that are well typed.
- Do NOT double-check for things that are already handled by the project's QA tools - use the tools instead.
- Do NOT leave dead code in the codebase.
- You are allowed to restructure code. Prefer this over introducing workarounds.
- Do NOT use assertions in production code for runtime invariants. Raise a specific exception instead.
- `InternalFrameworkException` is a common default for invariant breaches, but choose context-specific exceptions when appropriate (for example `InternalStateCorruptionException`, `FlowMisconfiguredException`, `ActorMisconfiguredException`, `SubnetMisconfiguredException`).

## Documentation guidelines

- You MUST proactively update documentation for whatever you are working on.
- ALWAYS keep the project's README, AGENTS.md, code comments, docstrings, docs, knowledge base to date.
- ALWAYS create and keep up-to-date high-level documentation for "public" classes, functions, and modules.
