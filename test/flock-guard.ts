// Preloaded by bunfig.toml. Without flock every lease-opening test fails on its own, burying the cause under timeouts.
if (Bun.which("flock") === null) {
  console.error(`flock (util-linux) is not on PATH, and the output lease needs it. Run the tests in Docker:

  docker compose -f docker-compose.test.yml run --rm --build engine-test
  docker compose -f docker-compose.test.yml run --rm --build engine-test bun test test/work.test.ts -t "<name>"
`);
  process.exit(1);
}
