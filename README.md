# Shared synchronization engine

Implementation of [Seigiard/opds-generator#49](https://github.com/Seigiard/opds-generator/issues/49).

Package identity: `@seigiard/sync-engine`.
Repository identity: `Seigiard/sync-engine`.

The library owns synchronization mechanisms. Applications own domain processing and publication requirements.
Bun on Linux/Docker and Effect 4 are the supported execution boundary.

## First release: 0.1.0

`runInitialPass({ sourcePath, outputPath, declare, handle })` returns an Effect.
It scans regular source paths and directories, with relative path identity,
size and modification time. Source symlinks are excluded. Source/output trees
must be disjoint. Applications declare an `InitialPlan`: initial work and a
final publication Effect. Each handler returns required cascade work. The
engine drains that work before final publication. Applications keep domain
classification, extraction, rendering, path projection and publication rules.

The initial pass stops on a read or handler failure. It does not clear outputs.
`ScanFailed` and `OutputOwnershipFailed` distinguish engine failures from the
application's error channel. Handlers own their interruptible preparation and
safe publication phases. All native filesystem Promise crossings are owned.

`acquireOutputTree(outputPath)` supplies the same lease to legacy consumers
during migration. It returns an async release function. Effect consumers use
`Effect.acquireRelease` in a scope; Promise consumers must release it after
joining their work. Linux `flock` (`util-linux`) holds a persistent lock inode
at `.sync-engine.lock`. A handshake confirms ownership. Release closes stdin
and joins the holder. Process death closes the pipe and releases the kernel
lock. Contention waits one second before failing. Keep the inode in the
dedicated output area while owners may use it. Canonical-path aliases use the
same lock. Separate applications must use separate, non-overlapping output roots.

The exact `effect@4.0.1` peer keeps one runtime identity. The tarball ships
TypeScript source for Bun; it contains neither node_modules nor bundled Effect.
This local release is not an npm publication. Watchers, freshness, resync,
optional-work readiness, concurrency and reconciliation are later slices.

## Consumer shape check

- OPDS: required book/folder cascades precede root publication.
- TTRPG: a full listing admits tree classification and index work; optional
  parallel images need a later scheduling/readiness extension.
- OPML: required RSS cascades precede final OPML. Its private cache projection
  remains application logic.

## Verify and pack

```sh
COMPOSE_PROJECT_NAME=opds49-50 docker compose -f docker-compose.test.yml run --build --rm engine-test
COMPOSE_PROJECT_NAME=opds49-50 docker compose -f docker-compose.test.yml down
bun pm pack
```

OPDS's `docs/agents/shared-sync-engine.md` records the integration reproduction
command and temporary lifecycle selection seam. Copy a versioned packed
artifact to that consumer; direct checkout imports are not the release boundary.
