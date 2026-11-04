---
name: cut-maintenance-release
description: Tag and publish a Jarvis.Automation maintenance release of this Elsa fork via the Azure Artifacts pipeline. Use when asked to "cut a release", "publish a new Elsa version", "tag a release for Jarvis", or when a fix needs to reach Jarvis.Automation.
---

# Cut a maintenance release

This repo (alkampfergit/elsa-core) ships fixes to Jarvis.Automation via a
private Azure Artifacts feed, published by
`.github/workflows/publish-maintenance-to-azure-artifacts.yml`. Read
`CLAUDE.md`'s "Branch model", "Versioning and tagging", and "Release
pipelines" sections first if you haven't already — this skill assumes that
context.

## Preconditions

- The fix must already be committed and pushed to the **current** maintenance
  branch. As of this writing that's `spinoff-12-jarvis-fixreferences`; confirm
  by checking the `RELEASE_BRANCHES` env var in
  `.github/workflows/publish-maintenance-to-azure-artifacts.yml` — the
  **last** entry is the current one, earlier entries are prior generations
  kept for compatibility with old tags.
- Never propose merging this branch into `master`. It's intentionally
  long-lived; releases are cut by tagging commits on it directly.

## Steps

1. **Confirm the branch and that it's pushed:**
   ```
   git branch --show-current
   git status
   git log origin/<branch>..HEAD   # should be empty — push first if not
   ```

2. **Pick the next revision number.** Releases use
   `vMAJOR.MINOR.PATCH.REVISION` (4-digit zero-padded revision), continuing
   the counter for that `MAJOR.MINOR.PATCH` base:
   ```
   git fetch --tags
   git tag -l 'vMAJOR.MINOR.PATCH.*' --sort=v:refname
   ```
   Use the next integer after the highest existing revision. If this is a
   new base version with no prior tags, ask the user what base to use
   rather than guessing — don't invent a new `MAJOR.MINOR.PATCH`.

3. **Tag the commit** (the tip of the maintenance branch, or wherever the
   fix landed — must be an ancestor of the branch for the workflow's guard
   to pass):
   ```
   git tag vMAJOR.MINOR.PATCH.NNNN <commit-sha>
   git push origin vMAJOR.MINOR.PATCH.NNNN
   ```
   If moving an *existing* tag to a newer commit (e.g. you added one more
   fix after tagging), use `git tag -f` and `git push --force origin
   <tag>` — but only when the user explicitly asked for the tag to move,
   not as a default.

   Alternatively, trigger the workflow by hand without a tag:
   ```
   gh workflow run publish-maintenance-to-azure-artifacts.yml \
     --repo alkampfergit/elsa-core \
     --ref <maintenance-branch> \
     -f version=MAJOR.MINOR.PATCH.NNNN
   ```

4. **Watch the run** until all three jobs succeed:
   ```
   gh run list --workflow=publish-maintenance-to-azure-artifacts.yml --repo alkampfergit/elsa-core --limit 1
   gh run view <run-id> --repo alkampfergit/elsa-core --json status,conclusion,jobs
   ```
   Jobs, in order: `Validate Tag and Branch` → `Build and Pack Maintenance
   Packages` → `Publish to Azure Artifacts`. If `Validate Tag and Branch`
   fails, the commit isn't on an allowed branch — don't retry the same tag,
   fix the branch situation first.

5. **Report the normalized version to use downstream.** NuGet strips
   leading zeros from the revision: tag `v2.12.0.0005` → package version
   `2.12.0.5`. Tell the user this normalized string — it's what goes into
   a consumer's `Directory.Packages.props`, not the zero-padded tag name.

6. **Don't assume the consumer repo updates itself.** Getting the package
   onto the feed is only half the job — the consuming repo
   (Jarvis.Automation) needs its pinned version bumped separately and a
   restore run to confirm resolution. If NuGet falls back to an unrelated
   version instead of erroring (`NU1603`), see CLAUDE.md's "Open question"
   note under Versioning before assuming it's a feed propagation delay.
