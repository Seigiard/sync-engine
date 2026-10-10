# AGENTS.md

This file provides guidance to coding agents working with code in this repository. `CLAUDE.md` is a symlink to it.

`@seigiard/sync-engine` owns synchronization mechanisms: scan, required work, freshness, live passes, output ownership. Consuming applications (OPDS, TTRPG, OPML) own domain processing and publication. The package ships its TypeScript source to Bun consumers, so `src/` is the release.

`README.md` is the behavior contract for the public API. A change to observable behavior updates the README in the same change.

## Commands

The output lease spawns Linux `flock` (util-linux), which macOS lacks. Without it, `bun test` stops at the `test/flock-guard.ts` preload and prints the Docker commands. Run the suite in Docker; CI (`.github/workflows/test.yml`) runs the same command on every PR and push to `main`:

```sh
docker compose -f docker-compose.test.yml run --rm --build engine-test                      # bun run check
docker compose -f docker-compose.test.yml run --rm engine-test bun test test/work.test.ts    # one file
docker compose -f docker-compose.test.yml run --rm engine-test bun test test/live.test.ts -t "<name>"
```

Compose bind-mounts `src`, `test`, `scripts`, `tsconfig.json` and `bunfig.toml`; pass `--build` after a change to `package.json` or `bun.lock`.

Host-safe, after `bun install`: `bun run lint`, `bun run typecheck`. The model check is the one test free of `flock`, and the calibration baseline runs it on the host: `bun scripts/calibrate-live-model.ts --only none`.

Manual checks:

- `bun test/shared-mount-check.ts` runs on the host and drives containers that share one volume, to prove the lease holds across containers. It uses the compose image `${COMPOSE_PROJECT_NAME}-engine-test` (default project `opds49-52`); build the image under the same project name first.
- `bun scripts/verify-pack.ts` verifies the packed tarball from a clean checkout. Run it before a release.

## Architecture

- `src/index.ts` is the public entry. `openSynchronization` composes the internal opening with its initial freshness commit, while `runInitialPass` scopes it. It builds on `ownership.ts` (lease, state area), `work.ts` (scheduler) and `freshness.ts` (retained results), and re-exports `source.ts` (observation, output cleanup).
- `src/internal.ts` owns the type-checked two-phase opening: validation, lease, scan, declaration, minimum/required work and publication, followed by a synchronization handle whose completion only drains later work and an explicit freshness commit. `src/initial-pass.ts` owns the shared opening contracts and source scanner; public and live callers compose the commit operation.
- `effect` is an exact peer (`4.0.1`) so consumers share one runtime identity. Keep the dev and peer versions equal.

### Live session: pure reducer + interpreter

The live API is split in two, and the split is load-bearing:

- `src/live-machine.ts` holds the whole session as one immutable value and a pure `step(state, event) → { state, commands, admission }`, free of Effect and I/O. Retry, reopen, follow-up, closing-window and `ready` rules live here; a change to session rules lands here.
- `src/live.ts` is the interpreter. It executes commands, owns fibers, scopes and Deferreds, and feeds outcomes back as events. `startLiveSynchronizationWithHooks` exposes test seams for timing windows.

`test/live-machine.test.ts` is a bounded-exhaustive model check over every event sequence up to `LIVE_MODEL_DEPTH` (default 10, about 80 s). Violations name an invariant id, e.g. `[P1]`. It also asserts coverage goals, so a depth below 10 goes red on `missingGoals` while the reducer is sound.

`scripts/calibrate-live-model.ts` applies named mutations to the reducer; each must turn the model check red on its expected invariant. Mutations anchor on exact reducer text. After editing `src/live-machine.ts`, run `bun scripts/calibrate-live-model.ts --dry-run` and re-anchor every mutation it reports.

## Agent skills

### Issue tracker

Issues live in GitHub Issues for `Seigiard/sync-engine` (`gh` CLI). See `docs/agents/issue-tracker.md`.

### Triage labels

The five default labels: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one root `GLOSSARY.md` and `docs/adr/`. See `docs/agents/domain.md`.
