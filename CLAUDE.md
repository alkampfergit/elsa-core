# CLAUDE.md

Guidance for Claude Code (and other agents) working in this repository.

## What this repo is

This is **alkampfergit/elsa-core**, a fork of the (now effectively abandoned at
the v2.x line) [elsa-workflows/elsa-core](https://github.com/elsa-workflows/elsa-core)
workflow engine. Upstream has moved on to a v3.x rewrite with a different
architecture; this fork exists to **keep the v2.12.x line alive and patched**
for one real consumer: **Jarvis.Automation** (`Jarvis.Automation` repo, built
by Proximo). Most work here is driven by "what does Jarvis.Automation need
fixed/upgraded," not by upstream feature parity.

Remotes:
- `origin` → `https://github.com/alkampfergit/elsa-core` (this fork, where we push)
- `upstream` → `https://github.com/elsa-workflows/elsa-core.git` (the original project, read-only reference)

## Branch model — read this before picking a branch to work on

**`master` is not where Jarvis fixes land.** It's kept around mostly for
legacy/upstream-style CI (see "Release pipelines" below) and moves slowly.
Checking `master`'s `Rebus.ServiceProvider` version against a maintenance
branch's version during this conversation confirmed it's genuinely behind —
don't assume a fix merged somewhere is present on `master`.

The actual maintenance work happens on a **long-lived, never-merged-to-master
feature branch**, with this lineage (oldest → newest):

```
feature/spinoff-jarvis  →  spinoff-12-jarvis  →  spinoff-12-jarvis-fixreferences  (current)
```

Each of these is/was "the" active Jarvis maintenance branch at some point.
They are **not meant to be merged into `master`** — a user explicitly
corrected this assumption mid-session ("this branch will never end, we
should only commit with a tag so the tag will be published"). Treat any of
these branches as a long-lived patch branch: commit fixes directly to the
current one (`spinoff-12-jarvis-fixreferences` as of this writing), and cut
releases from it via tags (see below) — don't propose merging it into
`master` as "finishing the work."

All three branch names are hardcoded in the `RELEASE_BRANCHES` env var of
`.github/workflows/publish-maintenance-to-azure-artifacts.yml` — that's the
authoritative list of branches a release tag is allowed to point into. If a
new maintenance branch generation is ever started (e.g.
`spinoff-13-jarvis-...`), that workflow's `RELEASE_BRANCHES` list must be
updated too, or tagging it will fail validation.

## Versioning and tagging

Package version is normally driven by `common.props`' `<Version>2.12</Version>`,
but every release pipeline overrides it at build/pack time via `/p:Version=X`,
where `X` comes from a **git tag**, not from a file.

Two tag shapes are supported (see `publish-maintenance-to-azure-artifacts.yml`,
"Extract package version" step):

- **Maintenance format**: `vMAJOR.MINOR.PATCH.REVISION`, e.g. `v2.12.0.0005`.
  This is the one actually used for Jarvis releases. `REVISION` is a
  zero-padded 4-digit counter (`0001`, `0002`, ...) that increments per
  release on top of the same `MAJOR.MINOR.PATCH` base.
- **Plain semver**: `vMAJOR.MINOR.PATCH[-prerelease]`, e.g. `v2.15.2` or
  `v2.15.2-custom.1`.

Existing maintenance lines as of this writing: `v2.12.0.0001`..`v2.12.0.0005`
and `v2.16.1.0001`..`v2.16.1.0004` (two independent bases, each with its own
revision counter — check `git tag -l 'v*' --sort=v:refname` before picking
the next number for a given base).

### ⚠️ Leading-zero gotcha — this bit us once already

NuGet **normalizes away leading zeros** in numeric version components. A
tag `v2.12.0.0005` produces a package whose actual, resolvable version
string is **`2.12.0.5`**, not `2.12.0.0005`. The workflow does NOT reformat
this for you in the tag-push path — it packs with the literal
`${GITHUB_REF_NAME#v}` string, and NuGet's own normalization is what turns
`0005` into `5` when the `.nupkg` is created. Confirmed empirically this
session: packing produced `Elsa.Server.Hangfire.2.12.0.5.nupkg`, not
`...2.12.0.0005.nupkg`.

**Consequence:** when you bump a consumer's pinned Elsa version (e.g.
Jarvis.Automation's `src/Directory.Packages.props`), use the
**normalized** form (`2.12.0.5`), not the tag's zero-padded form
(`2.12.0.0005`). Getting this wrong produces `NU1603`/`NU1102` warnings
where NuGet silently falls back to the nearest *different* version it can
find (e.g. an unrelated `2.12.100` line) instead of erroring loudly.

### Known gotcha — feed propagation delay after a fresh push

Right after `v2.12.0.0005` was published, GitHub Actions logs confirmed
**all 80 packages were built, packed, and successfully pushed** to the
Azure Artifacts feed (every push logged "Your package was pushed",
packed/pushed package lists matched 1:1) — yet Jarvis.Automation's restore
still reported every single `Elsa.*` package as "not found at 2.12.0.5" via
`NU1603`, falling back to unrelated versions (`2.12.100`, an older line).

**Confirmed root cause:** Azure Artifacts' NuGet v3 registration endpoint
propagates a brand-new package version with a real delay (longer than
nuget.org's), and/or caches a "this version doesn't exist" response from
before the push. Re-running `dotnet nuget locals all --clear` followed by a
clean restore some time later resolved every `Elsa.*` package to `2.12.0.5`
with zero warnings — confirming the packages were never actually missing,
just not yet visible through the feed's (and the local HTTP cache's) view.

**If you hit this again:** don't chase package-source-mapping or
credentials issues first. Clear local NuGet caches
(`dotnet nuget locals all --clear`) and retry — if it still fails after
that, wait and retry again before assuming something is actually broken.
Only after ruling out propagation lag should you double check
`Directory.Packages.props` has the normalized version string (see the
leading-zero gotcha above).

## Release pipelines — there are two, for different purposes

### 1. `publish-maintenance-to-azure-artifacts.yml` — **the one Jarvis uses**

- **Triggers:** push of a `v*` tag, **or** manual `workflow_dispatch` (added
  this session specifically so a release doesn't require creating a tag
  first — you can launch it by hand from the Actions tab, giving it a
  `version` input in the same format tags use).
- **Guard:** validates the tagged/dispatched commit is an ancestor of one of
  `RELEASE_BRANCHES` (see above). Fails loudly if not — this is what stops
  someone from accidentally publishing a release off a random branch.
- **Builds:** the Designer (npm), the Designer's ASP.NET bindings, then
  `dotnet build`/`dotnet pack` the whole solution with `/p:Version=<tag>`.
- **Publishes to:** a private **Azure Artifacts** feed named `Proximo` in
  the `prxm` Azure DevOps organization (`AZURE_ARTIFACTS_FEED_URL` repo
  variable: `https://pkgs.dev.azure.com/prxm/_packaging/Proximo/nuget/v3/index.json`).
  This is the feed Jarvis.Automation's `NuGet.Config` (`ProximoAzDo` source,
  mapped to the `Elsa*` package pattern) restores from.

To cut a release: commit to `spinoff-12-jarvis-fixreferences` (or whatever
the current maintenance branch is), then either push a `vMAJOR.MINOR.PATCH.NNNN`
tag pointing at that commit, or run the workflow manually with a `version`
input. Either way, wait for all three jobs (`Validate Tag and Branch` →
`Build and Pack Maintenance Packages` → `Publish to Azure Artifacts`) to go
green before telling a consumer repo to bump its reference.

### 2. `publish-latest-elsa.yml` — legacy/upstream-style, `master`-only

Runs on push/PR to `master`, manual dispatch, or a published GitHub
Release. Publishes npm (Designer) and NuGet packages to **feedz.io**
(preview, on every `master` push) or **npmjs.org**/**nuget.org** (on a
GitHub Release). This predates the Jarvis fork relationship and isn't part
of the Jarvis release flow — don't use it to answer "how do I ship a fix,"
use pipeline #1.

There are also three Docker-image workflows
(`publish-latest-{dashboard,server,dashboard-and-server}-docker.yml`),
all `master`-only and manually dispatchable, pushing images to GitHub
Packages. Also unrelated to the Jarvis release flow.

## Practical checklist for "ship a fix to Jarvis.Automation"

1. Make the fix on `spinoff-12-jarvis-fixreferences` (confirm this is still
   the current branch — check `RELEASE_BRANCHES` in the workflow file if
   unsure, and check with the user if a newer generation branch exists).
2. Build + test locally (`dotnet build Elsa.sln`, relevant `dotnet test`
   projects) before tagging anything — the release pipeline does run tests
   as part of build, but catching problems locally first is faster.
3. Commit and push to the branch.
4. Pick the next revision number for the relevant `MAJOR.MINOR.PATCH` base
   (`git tag -l 'vMAJOR.MINOR.PATCH.*' --sort=v:refname`), zero-padded to 4
   digits.
5. Either:
   - `git tag vMAJOR.MINOR.PATCH.NNNN <commit>` then
     `git push origin vMAJOR.MINOR.PATCH.NNNN` (use `--force` only if you
     are deliberately moving an existing tag, and say so), or
   - trigger `publish-maintenance-to-azure-artifacts.yml` manually via
     `gh workflow run` / the Actions UI with the equivalent `version` input.
6. Watch the run (`gh run view <id> --repo alkampfergit/elsa-core
   --json status,conclusion,jobs`) until all jobs succeed.
7. Tell the consumer repo's maintainer the **normalized** package version
   (strip leading zeros from the revision) to pin in `Directory.Packages.props`.

## Rebus — Elsa's internal message bus

This fork's `Elsa.Core`/`Elsa.Abstractions` use [Rebus](https://github.com/rebus-org/Rebus)
internally for workflow dispatch (`ICommandSender`, `IEventPublisher`,
`IServiceBusFactory`, `QueuingWorkflowDispatcher` and friends) and as the
transport abstraction under `Elsa.Activities.RabbitMq`,
`Elsa.Activities.AzureServiceBus`, `Elsa.Activities.Kafka`. A set of
**dedicated, duplicate Rebus-branded packages** (`Elsa.Rebus.RabbitMq`,
`Elsa.Rebus.AzureServiceBus`, `Elsa.Activities.Rebus`, `Elsa.Caching.Rebus`,
and their sample projects) were deliberately removed in this fork — don't
reintroduce them; their functionality overlapped with the activity-specific
transports that remain.

The in-memory Rebus bus (the default, used by every `AddElsa(...)` call
unless a real transport is configured) has real integration test coverage
under `test/integration/Elsa.Core.IntegrationTests/Messaging/` and
`.../RabbitMq/` — exercising real send/publish/dispatch round trips, not
mocks, specifically because a Rebus package upgrade in this fork once broke
runtime behavior (`UseInMemoryTransport`'s `registerSubscriptionStorage`
default changed, and `ICommandSender.DeferAsync` was silently dropping its
delay) that a plain `dotnet build` success did not catch. **If you bump any
`Rebus*` package version here, run those test projects before assuming it's
safe** — a clean build is not sufficient evidence.
