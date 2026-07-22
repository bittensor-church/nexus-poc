# Nexus

Opinionated event-driven actor framework for building Bittensor subnets. Thread-based concurrency, message-based flows.

## Prerequisites

- Python 3.14+
- [uv](https://docs.astral.sh/uv/)

## Setup

```sh
git clone https://github.com/bittensor-church/nexus-poc.git
cd nexus-poc
uv sync --all-groups
```

## Development

Full-project QA is run from the repository root through nox.

```sh
# Root lint, formatting check, type check, docs/shell checks
uvx nox -vs lint

# Root tests
uvx nox -vs test-3.14

# cat-images demo lint, formatting check, type check
uvx nox -vs cat-images-lint

# cat-images demo tests
uvx nox -vs cat-images-test
```

## Validator wiring

`NexusValidator` discovers runtime components from `connect(...)` calls. Each source has at most one primary target and
any number of taps:

```py
self.connect(results, result_handler)  # The primary keeps the current context.
self.connect(results, result_handler, taps=[audit_log, metrics])
self.connect(subnet_clock.source, taps=[task.block_beat, weight_trigger.block_beat])
```

The primary receives the existing parent context. Each tap receives a distinct child context containing a snapshot of
the parent payload and user data. A taps-only connection broadcasts only to those children; it does not dispatch the
parent context.

The same roles are available in the flow DSL:

```py
from nexus.v1 import Flow, Targets

flow = (
    Flow.from_connectable(request_source)
    .then(worker, taps=[request_audit])
    .then(ok=response_sink, error=Targets(taps=[error_log, error_metrics]))
)
```

Plain positional and named targets are primaries. Use `Targets(primary=..., taps=[...])` for a named source that needs
both roles. Legacy `.then(a, b)` and iterable named-route values are invalid; use the explicit `taps=` forms instead.

## Public API

Import Nexus public interfaces from the versioned API package:

```py
from nexus.v1 import Flow, NexusTask, NexusValidator, Source, Targets
```

Implementation modules live under `nexus._internal` and are not public API. Do not import from legacy paths such as
`nexus.actors`, `nexus.core`, `nexus.utils`, or `nexus.nexus_validator`.

## Project structure

```
src/nexus/
├── v1/                # Public versioned API facade
└── _internal/         # Implementation modules
    ├── core/
    │   ├── dsl/       # Flow DSL: nodes, pipes, flow definitions
    │   └── runtime/   # Event bus, actors, context store, serialization
    ├── actors/        # Built-in actors (retry, REST, task-result splitting, S3, etc.)
    ├── examples/      # Examples of actor usage
    └── utils/         # Shared utilities, including OpenRouter chat-completion helpers
demos/cat-images/      # Demo subnet package and Docker/localnet tooling
tests/                 # pytest suite (includes NexusTask wiring scaffolds in tests/nexus_task_test_setup.py)
```
