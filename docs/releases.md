# Releases

Use this procedure to configure npm publication or release a version. The package ships source directly, so publication needs neither a build nor installed project dependencies. Keep `devDependencies` in the manifest: consumers do not install them when they install this package as a dependency.

## One-time npm setup

First merge `.github/workflows/publish.yml` into `main`. Then, as a package maintainer:

1. Enable 2FA on the npm account used to approve releases.
2. Open [the package settings](https://www.npmjs.com/package/@seigiard/sync-engine/access), then **Trusted publishing** → **Add trusted publisher** → **GitHub Actions**.
3. Enter these case-sensitive values:

   | Field | Value |
   | --- | --- |
   | Organization or user | `Seigiard` |
   | Repository | `sync-engine` |
   | Workflow filename | `publish.yml` |
   | Environment name | Leave empty |
   | Allowed actions | Allow `npm stage publish`; disable direct `npm publish` |

4. Restrict release-tag creation and changes to release maintainers through GitHub tag rulesets.
5. After the first successful staged submission confirms the connection, set **Publishing access** to **Require two-factor authentication and disallow tokens**. Revoke obsolete npm write tokens.

These settings require maintainer account access; the workflow does not create them. It uses OIDC without an npm secret or GitHub environment. **Setup is verified when a candidate appears in npm's Staged Packages through this workflow.** Saving the publisher form alone does not validate the connection.

## Release a version

1. Update `package.json` to the intended stable version and merge it into `main` with the release changes.
2. From that commit, create and push the matching tag, for example `v0.5.6`. The workflow requires the version to match the tag and the commit to belong to `main`.

   ```sh
   git tag v0.5.6
   git push origin v0.5.6
   ```

3. Wait for both jobs in the `publish` workflow to succeed. The workflow summary records the verified archive inventory, commit and hashes.
4. In npm's **Staged Packages**, inspect the candidate against the intended commit and archive hash. Approve the matching candidate with 2FA; reject an incorrect candidate. Staging alone does not make the version available to consumers.
5. Check the published version with `npm view @seigiard/sync-engine@0.5.6 version dist.integrity --json`, using the version you released. **Release is complete when the version is available and its integrity matches the verified archive.**

For content inspection, `npm stage list`, `npm stage view`, and `npm stage download` use your interactive npm account. See the [staged publishing instructions](https://docs.npmjs.com/staged-publishing/) for commands and runtime requirements. Runtime versions and Action SHAs are pinned in `.github/workflows/publish.yml`.

## Local archive verification

From a clean checkout, with Node 24+, npm, git, and tar available:

```sh
node scripts/verify-pack.ts
```

No `npm install` or `bun install` is needed. The existing `bun run verify:pack` command also works, with npm available on PATH. The verifier packs with `--ignore-scripts`, rejects extra or missing files, checks source bytes against the checkout, checks package identity and the Effect peer, and prints the archive path and hashes. Publication uses the verified `.tgz`, not a second implicit pack.

## Why publication is separate

The `check` job verifies the archive and runs the full Docker check with read-only repository permissions. Only the separate `publish` job has `id-token: write`. It checks out the same commit in a fresh runner and repacks it without installing project dependencies, restoring dependency caches, or running lifecycle scripts. It submits that verified archive, not files produced by the test job.

This separation keeps dependency execution away from publication credentials. GitHub Actions are pinned to full commit SHAs, and checkout does not persist Git credentials. GitHub-hosted runners supply the OIDC identity; npm attaches provenance for this public repository and package.

These controls reduce exposure during publication. Provenance identifies the producing workflow and commit; it does not prove the source is harmless. Stage-only permission keeps the final release decision with the maintainer.

References:

- [npm Trusted Publishing](https://docs.npmjs.com/trusted-publishers/)
- [npm staged publishing](https://docs.npmjs.com/staged-publishing/)
- [Andrey Sitnik: The secure way to release an npm package in 2026](https://evilmartians.com/chronicles/the-secure-way-to-release-an-npm-package)
