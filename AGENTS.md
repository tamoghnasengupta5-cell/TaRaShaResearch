# Owner release policy

This policy applies to TaRaShaResearch, TaRaSha Discover, and the upstream
TaRaShaData.ai service. It records the owner's instruction of 2026-10-05.

## Local work and publishing to main

- Develop, review, and test changes locally.
- Do not create commits or push changes unless the owner explicitly asks.
- When the owner requests publishing, commit and push the requested changes to
  `main` after reviewing and testing their scope. Approval for one application
  does not authorize publishing changes to the other applications.
- `main` holds code awaiting release. A commit, push, merge, successful check,
  or request to publish to `main` is not approval to deploy to production.

## Production release

- Require separate, explicit owner approval before deploying from `main` to
  Azure Production. Approval must identify the application(s) and reviewed
  commit(s). If the approved code changes, obtain approval for the new version.
- Deploy only the approved, immutable commit from `main`, not uncommitted local
  files, a development branch, or whatever happens to be newest later.
- Do not configure deployment on push, merge, schedule, or completion of CI.
  Production deployment workflows must use `workflow_dispatch` only, enforce
  `refs/heads/main`, require the full `approved_commit` SHA to match the run,
  and check out the workflow run's immutable `github.sha`.
- Automatic build, lint, and test checks may run without deploying.
- Do not deploy Research, Discover, TaRaShaData, their serving adapters, or
  production database/schema/snapshot changes as an implied part of another
  application's release. Release each dependency only within explicit approval.
- Azure tier changes also require their own owner authorization.

## Release configuration

- Research and Discover share the TaRaShaResearch Git repository in different
  worktrees. The published `main` tree contains both applications. TaRaShaData.ai
  is a separate repository. Review selected changes when promoting between
  worktrees; do not replace `main` with a whole older checkout or overwrite
  unrelated work.
- Research and Discover Azure deployment workflows in this source tree are
  manual-only and enforce `main` plus the full approved commit SHA.
- Discover's GitHub Azure deployment workflow remains disabled remotely as the
  production hold. Verify the manual-only workflow on remote `main` before
  re-enabling it for a separately approved production release.
- Research's restored deployment workflow supports manual releases only. Its
  presence on `main` is not production release approval.
- TaRaShaData.ai's GitHub workflow performs verification only; no Azure
  deployment workflow is installed. Keep its production release separate and
  manual. Do not treat its CI checks as deployment authorization.
- Publishing the release controls or application changes to `main` does not
  authorize deploying applications, database migrations, or data snapshots.
