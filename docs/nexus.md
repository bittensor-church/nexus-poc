# Nexus

Opinionated framework for building Bittensor subnet validators. Replaces bittensor SDK for validator
development.

A validator built with Nexus is a directed graph of actors (later called a pipeline), passing typed
messages between each other. The pipeline's shape is declared up front and the Nexus runtime executes it.
Multiple pipelines coexisting within the same process are OK.

The developer's work:

**explore** — learn about available nodes, actors, the Nexus Task
**design** — using Nexus's building blocks, plan out the shape of your validator
**build** — build the design, declaratively connecting actors into pipelines, filling in gaps
**verify** — double-check whether your solution doesn't violaty any rules below

## Core Concepts

### Node and Actor

A Node is a setup-time declaration of an actor: it describes its configuration and builds the actor. An Actor
is the runtime counterpart — a thread that does the actual work. The actor holds a reference back to its node
to read config. Nodes and actors tend to come in pairs.

Nexus ships with prebuilt actors for common validator concerns — chain synchronization, miner communication,
task routing, weight setting, result persistence, HTTP entry points, generic retries, external service
integrations, and more. There are many generic actor patterns, meant to be extended and used when there is no
specific actor that would fit a use case.

#### Custom actors

Always discover available actors by browsing `nexus.v1` and reading their docstrings. In all cases, try to
reuse what is already available. When nothing fits your use case, choose a base actor/node that are closest to
what you need.

Preference rules:

1. Use predefined actor compositions (see e.g. NexusTask)
2. Use existing actors
3. Compose a solution out of available pieces
4. Extend a built-in actor pattern
5. Fully custom actor — last resort

### Source, Sink, Pipe

Actors have named sources (outputs) and sinks (inputs), similar to network ports. A Source or Sink is just a
typed identifier. Messages are paired with their source/sink to keep track of where they came from and where
they're going. Actors receive messages on sinks and emit messages from sources.

Pipe is a connection between a source of one actor and a sink of another, forming the pipeline. You define
the connections between sources and sinks declaratively, while the Nexus runtime creates and uses pipes to
route messages at runtime.

A source may feed multiple sinks when those downstream branches are independent pieces of work. Do not use
fan-out for internal cleanup or compensation paths that must happen before a public outcome is emitted. In those
cases, serialize the cleanup through one actor and emit the final success or error only after the actor has restored
its per-context state.

### Context

Every message entering a pipeline gets its own context. As the message is transformed and passed between
actors, it carries the same context throughout the linear part of the flow.

Contexts include an arbitrary data bag that actors can use to store persistent per-flow information. Contexts
survive restarts — they are persisted and reloaded.

Each context has a single linear lifecycle. When a source fans out to multiple sinks, Nexus creates one child
context per downstream branch. Each child records a snapshot of its parent and starts with the parent's current
payload and user data.

Conversely, when there's a gather point, a new context with multiple parents should be created. Multi-parent
contexts do not implicitly merge payloads or user data. Instead, they expose ordered parent snapshots through
`context.copy_parent_context_snapshots()`; the gather actor should build any aggregate payload or user data explicitly.

### Nexus Task

A generic template for a unit of work with built-in and configurable retry, timeout, and result storage.
Underneath, composed out of common actors.

**As a rule of thumb, if something normally goes in a task queue, it is a candidate for a Nexus Task.** (think a Celery task)

Pluggable components:

- "router" picks a target that will execute the task. The target type is arbitrary: it could be a miner,
  any neuron, a remote API, or local in-process execution (embedded executor).
- "communicator" passes the task to the routed target and receives the result, implementing a communication protocol
  or doing the work itself (embedded communicator).
- "payload creator" maps the task to a payload for the executor.
- "executor result converter" maps the executor's raw result for further processing.

Routers an communicators are pluggable and can be used to implement custom strategies and connectors.
Payload mappers (both at input and output) are also pluggable and are used to shape the messages going into and out of
the Nexus Task.

#### Result Store

Nexus-provided storage solution for Nexus Task results. All Nexus Tasks results are automatically stored
in a global result store shared between all tasks.

- The store is persistent – survives restarts
- Results are keyed by task name
- Results can be queried via the store

## Common Patterns

Recipes to be followed for implementing common validator concerns.
Feel free to adapt them to the specific needs of a subnet, mix and match multiple patterns.
Everything in here is composable, and these are just good starting points.

### Epoch-driven weight setting

Synchronizes weighing and weight setting with subnet's epoch boundaries.

Recipe:

- Chain synchronization actor emits timing signals, "beats" - new block, epochs boundary.
- Set weights beat actor gates weight-setting attempts until the configured epoch offset, checks pylon's
  unstable weights status endpoint for the current block, and emits a SetWeightsBeat only when weights still need
  to be set.
- Weight setter actor responds to SetWeightsBeat, calls a developer-provided weighing function, passes in weighing
  bundle
- Weighing function gets the epoch and task result store from bundle, queries store for relevant task results
- Aggregates scores into weights, returns them to let the actor handle setting on chain

### Miner Nexus Task

Implements miner task routing and miner contract with pluggable routing, retry strategies and persistent task
storage under the hood.

Recipe:

- Miner is the executor, using neuron communicator
- Neuron router provides fresh axon info; developer-provided callback function acts as miner selection strategy
- "payload_creator" prepares the task so it conforms to miner contract
- "executor_result_converter" converts raw miner response for later processing and/or loopback to initiator
- task, result and converted result are all stored in task result store for later consumption by other pipelines

Benefits:

- Routing, retries and persistent task result storage handled out of the box

### Validation Nexus Task

Expresses validation as a Nexus Task

Recipe:

- Pipeline triggered by a successful miner task
- For in-process validation cases: simple validation function extracted, passed into embedded communicator
- Routing and payload mappings are short-cuircuited if necessary
- Validation result (score, pass/fail, any other metadata) is stored as regular nexus task result in the store
- Validation task results used for weighing (instead of using miner scores directly)

When only a subset of results is to be validated: a sampler sitting in front applies sampling and batching strategies

Benefits:

- Moves heavy, long or expensive validation out of other code paths
- Precomputed validation results are safely stored and can be easily retrieved from task store later

## Critical Requirements for Nexus-based Validators

Nexus has a set of important invariants: it is critical to meet them at all times in order to make best use of the
guarantees offered by the framework.

### Never sidestep the actor runtime

**All code running in the validator process must be tied to some actor's thread.**

Allowed:

- Literal actor loop code
- Actor's message handlers
- Callbacks / strategies passed into the actor's node, called from within the actor
- Composite blocks wrapping complex pipelines (see e.g. NexusTask)
- As a last resort, threads started and managed by the actor code itself

Disallowed:

- any thread, process, async loop not tied to any specific actor

### Never store iportant state in-memory

**A validator process must be restartable without losing any work**

Nexus provides two mechanisms to keep state:

- contexts
- result store

Never use variables, objects, fields, properties, globals, contextvars, or anything similar to store
information that should not be lost on restart:

- miners' work results
- validation results
- scores queued for weight setting
- intermediate aggregates

In most cases, you should use Nexus Tasks for the task and and result stores for its result.

### Always connect logging listeners to all error sources

Inspect every actor you use for the sources it exposes.

You have to connect a logging listener to all error sources: Nexus does not log actor error outputs out of the box!

### Naming matters

Node and actor IDs/names are used for identification across persistence, tracing, and routing. Pick
descriptive, stable, unique names.

### Only import from versioned public modules

**Every Nexus symbol must be imported from a versioned module: `nexus.v1`, or any newer version. Never import
from `nexus._internal`.**

`nexus._internal` holds implementation modules whose layout, names, and signatures may change at any time
between versions. The versioned modules (`nexus.v1`, and future `nexus.vN`) re-export the public surface and
are the only import path with a stability guarantee — `nexus.v1` will not change in a backwards-incompatible
way.

## Source Discovery

This document describes Nexus conceptually. Specific actors, nodes, and APIs must be discovered from the
public API surface and docstrings — what is described here is not exhaustive.

`nexus.v1` is the complete public index: every supported actor, node, type, and helper is re-exported from
there. Browse `nexus.v1` to enumerate what is available, then read the docstrings of the symbols relevant to
the task at hand — they are the authoritative source of information about specific components. The
sink/source interface, message typing, configuration, and usage notes for each actor/node pair live on the
node class (the actor classes themselves are typically undocumented).
