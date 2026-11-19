Research public preview
=======================

The public Research application runs at https://www.research.tarasha.biz/.
It uses a verified SQLite snapshot of the current localhost Research database.
The snapshot and its manifest are private deployment artifacts, outside Git.
No PostgreSQL credentials, Keychain items, account/session tables or source
connection strings belong in the application package or Azure configuration.

Local preparation
-----------------

`scripts/export_research_preview.py` resolves the selected localhost checkout's
normal database configuration in memory. For PostgreSQL, it exports Research's
code-owned financial tables in one read-only, repeatable-read transaction.
It reconciles every table's count and canonical numeric-value digest, checks SQLite
integrity and foreign keys, and writes a private manifest beside the snapshot.
It refuses an existing destination or a destination inside this checkout.
The source database is never initialized, migrated, seeded or written.

The owner approved recalculating existing formulas before publication.
`scripts/recompute_research_preview.py` copies that verified export to another
private artifact, attempts all 11 existing company formulas for every source
company, and records progress, errors, additions, changed figures and new table
digests. Each formula retains its existing missing-input behavior. It requires
all source/assumption tables outside the 14 derived cache tables to remain
unchanged, checks integrity and relationships, and writes a new snapshot hash.
The original verified export remains unchanged. The recalculated artifact and
its new hash must be reviewed separately before production publication.

Preview runtime
---------------

Set `TARASHA_RESEARCH_SNAPSHOT=data/research-preview.db`. The snapshot is
packaged inside the application and resolves relative to the extracted app
root. Start with `bash deployment/start-research-preview.sh`; the startup
script enforces `TARASHA_READ_ONLY=1`. A missing snapshot fails closed and
cannot fall back to a writer connection or an empty SQLite database.
The SQLite URI uses `mode=ro&immutable=1`; connections also set `query_only`.
Startup skips all migrations and seeding. Persistent financial uploads,
administration, settings and saved buckets/dashboards cannot be changed.
Source financial records and assumptions are preserved; calculation helpers
that normally refresh stored results use the offline recalculated published
values. Nonpersistent
filters, ticker selection uploads, calculations and downloads remain available.

Gateway and Azure
-----------------

The gateway routes the Research public hostname to the free F1 application's
default Azure hostname, preserving the root request path and browser Origin.
It must forward WebSocket upgrades and Sec-WebSocket-Protocol, Cookie and
Set-Cookie. Research cookies must remain host-only. Startup explicitly allows
https://www.research.tarasha.biz while keeping CORS and XSRF protection enabled.
Research requires no paid database. Use normal Azure Python build automation:
Oryx stores a combined compressed application/dependency archive in wwwroot,
then extracts the runtime into `/tmp` and sets `APP_PATH`. The startup script
changes to its extracted application root before resolving the relative data
path. Read-only mode never seeds or copies this snapshot to `/home/app.db`.
Do not enable `WEBSITE_RUN_FROM_PACKAGE` for this Python release. See the
[Azure Python configuration documentation](https://learn.microsoft.com/en-us/azure/app-service/configure-language-python#customize-build-automation),
[Oryx build template](https://github.com/microsoft/Oryx/blob/main/src/BuildScriptGenerator/BaseBashBuildScript.sh.tpl),
and [Python startup generator](https://github.com/microsoft/Oryx/blob/main/src/startupscriptgenerator/src/python/scriptgenerator.go).

The compressed archive and retained deployment files must fit F1 storage;
expanded runtime files in `/tmp` are accounted separately. A snapshot is copied into a release package only after
its manifest digest is verified. Never copy the whole shared repository,
localhost virtual environment, consumer directory or a stale app.db.

Reproducible source/data packaging
--------------------------------

The original root `requirements.txt` remains the local development entry point.
`deployment/requirements-research.lock` pins the 60 verified runtime dependency
versions for Azure. `scripts/package_research_preview.py` copies that lock into
the artifact-root `requirements.txt`, includes only tracked root Python modules,
assets, startup/lock files and the verified data artifact, and records the full
source commit, snapshot SHA-256 and every packaged file hash. Consumer, tests,
local environments, caches and secret configuration files are excluded.

Release mode refuses a dirty checkout, a source commit different from HEAD,
a commit not present on the locally verified origin/main history, or a snapshot
hash/manifest mismatch. Fetch and verify main before using that mode. `--draft`
is solely for local review: its manifest explicitly sets `deployable=false` and
records the source file fingerprint rather than pretending uncommitted changes
belong to the base commit. The source/data ZIP is input for Oryx build automation;
the Linux dependency probe archive is a storage measurement, not a runnable
release artifact.

Observed storage model for review
--------------------------------

The owner selected a fresh Linux Free F1 Research service: resource group
`TaRaShaResearchPreview`, plan `TaRaSha-Research-Free`, app `tarasha-research`.
The previous Research service and its databases/backups are preserved. The
gateway uses the new app's actual Azure hostname after provisioning; the public
Research hostname remains unchanged.

The combined local archive probe is 300,788,646 bytes. On the fresh plan this
leaves 772,953,178 bytes before initial platform files and a reserve. Allowing
20 MiB for the two source-built packages and builder metadata leaves
751,981,658 bytes; conservatively retaining the 115,270,182-byte upload ZIP
as well leaves 636,711,476 bytes. These are measured-payload planning figures,
not a substitute for checking the exact locked cloud build and temporary
build/runtime capacity before public cutover.

Historical evidence from the preserved app confirms the standard pipeline.
Its read-only Azure inventory observed 809,269,248 bytes used of 1,073,741,824,
including the existing 237,387,759-byte Oryx output archive. All other retained
files total 571,881,489 bytes. The local combined GNU-tar/default-zstd probe is
300,788,646 bytes, including the reviewed 254,513,152-byte database, source/assets
and matching Linux dependency bytecode. It excludes two small source-built
packages and builder/tool metadata, so a reserve and the actual build size check
remain necessary.

Retaining both old and new output archives exceeds the observed quota by
36,316,070 bytes. Normal replacement would leave 201,071,689 bytes; retaining a
roughly 115.3 MB source/data upload ZIP as well leaves roughly 85.8 MB before
the reserve. The last two successful deployment logs confirm upload/extraction
under `/tmp/zipdeploy`, intermediate build content under `/tmp/<uid>`, and direct
Oryx output to `/home/site/wwwroot`. The official template writes the same
`output.tar.zst` destination pathname. This historical replacement calculation
does not govern the selected fresh target. No legacy databases or backups have
been deleted to make these measurements fit. Verify the new app's exact build,
initial quota usage and temporary-runtime capacity before public cutover.

Release boundary
----------------

These files prepare a local, reviewable preview. They do not deploy Research,
change Discover, upload a snapshot or authorize a production release.
The owner release policy in AGENTS.md still applies to the immutable source
commit and separately reviewed snapshot artifact.
