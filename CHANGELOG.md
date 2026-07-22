# Changelog

## Unreleased

### Breaking changes

- Replaced ambiguous per-source sink sets with explicit `Targets(primary=..., taps=...)` in the `nexus.v1` API.
  A primary keeps the existing context; every declared tap, including a sole tap, receives a distinct child context.
  Taps-only wiring does not dispatch the parent context.
- `Flow.then(a, b)` and iterable named routes such as `then(ok=[a, b])` are no longer accepted. Migrate them to
  `then(a, taps=[b])`, `then(taps=[a, b])`, or `then(ok=Targets(primary=a, taps=[b]))` according to the intended roles.
- `Pipes[source]` now returns `Targets[Sink]` instead of a raw sink set. Read `.primary` and `.taps` explicitly.
- `Context.copy_parent_context_snapshots()` now returns an
  `ImmutableMap[ContextId, ParentContextSnapshot]` instead of a tuple. Select snapshots by parent context ID rather
  than position; mapping iteration order is unspecified. Duplicate parent IDs produce one relationship and snapshot.

### Changed

- `NexusValidator.connect` now accepts `connect(source, primary=None, *, taps=...)`. Existing two-argument calls remain
  primary connections. Subnet-clock consumers, including Nexus Tasks and the cat-images weight-setting trigger, are
  connected as taps.
- Target declarations are merged centrally: duplicate taps and repeated identical primaries are idempotent, while
  conflicting primaries or a sink assigned both roles raise `FlowMisconfiguredException`.
- All tap child contexts and parent snapshots are created before dispatch begins, so primary processing cannot change
  what sibling taps inherit from the emission.
