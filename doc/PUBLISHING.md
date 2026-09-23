# Publishing a new version to NuGet

This is a fork of [elsa-workflows/elsa-core](https://github.com/elsa-workflows/elsa-core)
(`origin` = `alkampfergit/elsa-core`, `upstream` = `elsa-workflows/elsa-core`). We do **not**
publish to nuget.org. Instead, tagging a commit triggers a GitHub Actions workflow that builds
the Designer, packs the .NET libraries, and pushes the resulting NuGet packages to a private
**Azure Artifacts** feed.

The workflow that does this is
[`.github/workflows/publish-maintenance-to-azure-artifacts.yml`](../.github/workflows/publish-maintenance-to-azure-artifacts.yml).

## How it works

1. The workflow triggers on any pushed tag matching `v*`.
2. It checks out the tagged commit and confirms it is an ancestor of one of the release branches
   listed in the `RELEASE_BRANCHES` env var of the workflow (see [Release branches](#release-branches)).
   **Tags created on any other branch will fail the release** — merge your changes into a release
   branch and push it to `origin` first.
3. It derives the package version from the tag name (see [Tag format](#tag-format) below).
4. It builds the Designer (`npm`), the ASP.NET bindings, then runs
   `dotnet build` / `dotnet pack` with `/p:Version=<version>` for the whole solution.
5. It uploads the resulting `.nupkg`/`.snupkg` files as a build artifact, then pushes them to the
   `azure-artifacts` NuGet source using `dotnet nuget push --skip-duplicate`.

## Release branches

Three spinoff lines are maintained:

| Branch | Based on | What it changes | Tag example | NuGet version |
|---|---|---|---|---|
| `feature/spinoff-jarvis` | Elsa `2.16.1` | Full fork: net10, cleanup, library upgrades | `v2.16.1.0005` | `2.16.1.5` |
| `spinoff-12-jarvis` | Elsa `2.12.0` | Full fork ported onto 2.12.0 (net10, cleanup, library upgrades) | `v2.12.100.1` | `2.12.100.1` |
| `spinoff-12-jarvis-fixreferences` | Elsa `2.12.0` | Only AutoMapper → MagicMapper (plus these publish files) | `v2.12.0.0003` | `2.12.0.3` |

`spinoff-12-jarvis` published `2.12.0.2` before moving to the `2.12.100.*` range, which is the
first range of the full fork on 2.12.0. The `2.12.0.*` range from `2.12.0.3` on belongs to
`spinoff-12-jarvis-fixreferences`.

All lines publish to the same Azure Artifacts feed under the same package IDs, so always tag with
the version range belonging to the branch — otherwise the lines become indistinguishable to
consumers.

### `spinoff-12-jarvis-fixreferences`: MagicMapper on net8.0

This branch is official Elsa 2.12.0 with AutoMapper replaced by
[MagicMapper](https://www.nuget.org/packages/MagicMapper), a drop-in fork that ships an
`AutoMapper.dll` with the same assembly identity (`PublicKeyToken=be96cd2c38ef1005`).
MagicMapper only targets `net8.0`, so the two projects that reference AutoMapper directly
(`Elsa.Core`, `Elsa.Webhooks.Persistence.YesSql`) gain a `net8.0` target that uses MagicMapper,
while their original targets keep AutoMapper 12. Applications on net8.0 or later therefore get
no AutoMapper package at all; applications on older frameworks are unchanged. The branch must be pushed to `origin` before the tag,
because the workflow verifies the tag against `origin/<branch>`.

The workflow file is read from the tagged commit, so each branch must carry a copy of the
workflow whose `RELEASE_BRANCHES` includes it.

## Tag format

The tag name (minus the leading `v`) must match one of two shapes:

- **Maintenance build**: `MAJOR.MINOR.PATCH.REVISION`, e.g. `v2.16.1.0004`.
  This becomes NuGet package version `2.16.1.4` and Designer npm version
  `2.16.1-maintenance.4`.
- **Plain semver (optionally with prerelease)**: `MAJOR.MINOR.PATCH[-prerelease]`, e.g.
  `v2.16.2` or `v2.16.2-custom.1`. Both the NuGet package and the Designer package use this
  version verbatim.

Any tag that doesn't match one of these is rejected by the workflow before anything is built.

## Releasing a new version

1. Make sure your changes are merged into the release branch (`feature/spinoff-jarvis` or
   `spinoff-12-jarvis`) and that branch is pushed to `origin`.
2. Decide the next version number, following the [tag format](#tag-format) above. Check existing
   tags with:

   ```bash
   git tag --list "v*" --sort=-v:refname
   ```

3. Tag the commit on the release branch and push the tag:

   ```bash
   # 2.16 line (feature/spinoff-jarvis)
   git tag v2.16.1.0005
   git push origin v2.16.1.0005

   # 2.12 line (spinoff-12-jarvis)
   git tag v2.12.0.0001
   git push origin v2.12.0.0001
   ```

4. Watch the **Publish Maintenance Packages to Azure Artifacts** run under the *Actions* tab. It
   builds, packs, and pushes automatically — no further manual steps.
5. Once the run succeeds, the packages are available on the Azure Artifacts feed configured for
   this repository (see [Prerequisites](#prerequisites) — the feed URL is not stored in the repo).

## Prerequisites (one-time repository setup)

The publish job needs two things configured in the GitHub repository settings
(*Settings → Secrets and variables → Actions*), since none of this is committed to the repo:

- **Repository variable** `AZURE_ARTIFACTS_FEED_URL` — the Azure Artifacts NuGet v3 feed URL to
  publish to.
- **Repository secret** `AZURE_DEVOPS_TOKEN` — a PAT with *Packaging (Read & write)* permission on
  that Azure DevOps organization/feed.

The workflow writes a throwaway `nuget.config` under `$RUNNER_TEMP` at run time and adds the feed
as a source named `azure-artifacts` using those two values — your machine's/repo's committed
[`Nuget.Config`](../Nuget.Config) is untouched and still only points at nuget.org.

## Publishing a build locally (without CI)

Useful for testing a package before tagging, or if you need to push a one-off build by hand.
`dotnet nuget push` needs a source and a way to authenticate:

```bash
# Build & pack with an explicit version
dotnet build --configuration Release /p:Version=2.16.1.5
dotnet pack --configuration Release /p:Version=2.16.1.5 /p:PackageOutputPath=./out

# Add the Azure Artifacts feed as a source (once), using a PAT as the password
dotnet nuget add source "<AZURE_ARTIFACTS_FEED_URL>" \
  --name azure-artifacts \
  --username anything \
  --password "<your PAT>" \
  --store-password-in-clear-text \
  --configfile ./local.nuget.config

# Push
dotnet nuget push "out/*.nupkg" \
  --source azure-artifacts \
  --api-key AZ \
  --skip-duplicate \
  --configfile ./local.nuget.config
```

Keep `local.nuget.config` (or whatever you name it) out of source control — it contains your PAT
in clear text.

## Legacy upstream workflow

[`publish-latest-elsa.yml`](../.github/workflows/publish-latest-elsa.yml) is the original
upstream workflow that publishes to feedz.io and nuget.org on pushes to `2.x`/`rc/*` and on
GitHub Releases. It is **not** used by this fork's release process and targets secrets
(`FEEDZ_API_KEY`, `NUGET_API_KEY`, etc.) that aren't configured here — ignore it unless you're
specifically reviving publishing to the public feeds.
