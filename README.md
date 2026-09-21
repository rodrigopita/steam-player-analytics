# Steam Player Analytics

A small analytics platform that builds its own dataset by polling Steam's public APIs. Airflow fetches player counts every hour and the catalog every day, lands the raw responses in S3, loads a local Postgres warehouse, and dbt models one business process into a star schema. A Quarto dashboard reads the marts; the published copy is at https://rodrigopita.github.io/steam-player-analytics/.

I built it to keep my Airflow and dbt practice current outside work. It is small enough to run on a laptop and real enough to fail the way production pipelines fail, and when it fails an hour of data is gone for good. The scope and the trade-offs are in [docs/scope.md](docs/scope.md).

## Run it

You need Docker, the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli), [uv](https://docs.astral.sh/uv/), [Terraform](https://developer.hashicorp.com/terraform/install), [Quarto](https://quarto.org/docs/get-started/), an AWS account with the CLI configured, and a free [Steam Web API key](https://steamcommunity.com/dev/apikey).

1. Clone and install the Python tooling. uv fetches Python 3.14 if you do not have it.
   ```bash
   git clone git@github.com:rodrigopita/steam-player-analytics.git
   cd steam-player-analytics
   uv sync --all-groups
   ```
2. Create the raw zone. One bucket and one append-only IAM user, in your account.
   ```bash
   cd terraform
   cp terraform.tfvars.example terraform.tfvars  # set bucket_name and owner
   terraform init && terraform apply
   terraform output -raw airflow_conn_aws_default  # paste into .env below
   cd ..
   ```
3. Configure Airflow. Copy the example and fill in the Steam key, the bucket name and the connection from step 2. The warehouse line stays as it is.
   ```bash
   cp .env.example .env
   ```
4. Start Airflow and the warehouse.
   ```bash
   astro dev start
   ```
   Airflow is at http://localhost:8080, login `admin` / `admin`. The warehouse is Postgres on `localhost:5433`.
5. Bootstrap and fill the universe. In the Airflow UI, trigger `warehouse_bootstrap` once; it creates the `raw` schema. Then unpause and trigger `steam_catalog_refresh`; it builds the tracked universe and fetches metadata, about two minutes. Finally unpause `steam_player_snapshots_hourly`. It runs at every top of the hour; trigger it once if you do not want to wait.
6. Optional: start with three weeks of history. Your instance begins empty. The latest [release](https://github.com/rodrigopita/steam-player-analytics/releases) carries a dump of the raw schema; restoring it is idempotent, so running it twice changes nothing.
   ```bash
   gunzip -c raw-YYYY-MM-DD.sql.gz | PGPASSWORD=steamdw-local psql -h localhost -p 5433 -U warehouse -d steam -v ON_ERROR_STOP=1 -q
   ```
7. Build the marts and open the dashboard.
   ```bash
   source .venv/bin/activate
   (cd dbt && dbt build)
   (cd dashboard && quarto preview)
   ```
   The dashboard footer tells you how fresh what you are looking at is.

To publish your own copy, `quarto publish gh-pages` from `dashboard/` renders against your warehouse and pushes the site to a `gh-pages` branch of your fork.

## Why this architecture

<picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/architecture-dark.svg">
    <img alt="Airflow DAGs land Steam responses in S3, load a local Postgres raw schema, dbt builds staging and marts, a Quarto dashboard reads the marts and publishes to GitHub Pages" src="docs/architecture-light.svg">
</picture>

- **Three DAGs.** Hourly snapshots and the daily catalog are different flows. A snapshot that fails is lost; a catalog day that fails is covered by the next run. Separate DAGs mean a slow metadata refresh cannot delay a snapshot, and each gets its own audit row. The third, `warehouse_bootstrap`, creates the raw schema once, by hand.
- **Raw JSON in S3 before anything parses it.** Steam cannot be asked about the past, so the response body is the only irreplaceable thing in the system, and it is the one part that does not live on the laptop. Everything below it can be rebuilt from the bundles.
- **Append-only by policy.** The pipeline's IAM user can put, get and list; no statement grants delete. Bucket versioning keeps any overwritten version.
- **Airflow writes `raw`, dbt writes everything else.** Loads use `ON CONFLICT DO NOTHING` on the natural key, so rerunning an hour inserts nothing. Staging views rename, deduplicate and derive; marts are tables.
- **One business process.** Everything measured is a game's concurrent players at an hour. No second ingestion was added to make the schema look bigger.
  - `fct_player_counts` is that fact at its native grain.
  - `fct_player_counts_daily` re-grains it to the day as a dense snapshot: one row per game per day from the game's first day, with unobserved days kept as rows so a window function sees the gap instead of skipping it.
  - `fct_player_trends_daily` adds day-over-day change, seven-day average and days since peak at that grain. They share a grain, so they are columns on one table and the dashboard does one join, not three.
  - `fct_snapshot_hours` is not about games. It is the spine of hours the pipeline should have run, which is why it is a view and not a table.
- **dbt in this repository.** The sources are coupled to what Airflow lands; a schema change is one commit touching both sides.
- **The universe is a rule, not a list.** Every game that has appeared in Steam's most-played top 100 since 2026-08-26, plus a curated seed. Add, never remove; deactivate only on evidence stronger than a missing metadata call. When the chart shifted on 2026-09-16 the rule admitted 32 games in one night and refused the same six non-games it refuses every day.

## Why these technologies

- **Airflow 3 on the Astro CLI.** One command starts a local Airflow with its own metadata database, and a compose override adds the warehouse container next to it. Airflow 3.3.1, Astro Runtime 3.3-5. The image installs `requirements.txt`, so the uv environment is for tooling and never for a runtime dependency.
- **S3 for the raw zone.** Its API is the one every tool speaks. It is the project's only cloud component, because the raw zone is the one part that has to outlive the laptop.
- **PostgreSQL for the warehouse.** A relational system that runs in a container and shows its work: constraints, upserts, indexes, query plans. At 24 MB the dataset costs nothing to keep local. The models stay portable except where Postgres behavior is the point, such as `ON CONFLICT` on the loads and arrays in the audit tables. Postgres 16.
- **dbt Core.** Twelve models, 43 tests, freshness declared on four raw tables. Staging as views, marts as tables. A decision about a column lives in that column's yaml description. dbt-core 1.12.
- **Terraform, adopted late.** The bucket and the IAM user were built in the console on day one and imported three weeks later. The first plan changed one tag, `managed-by`; the second plan changed nothing. Terraform 1.16, AWS provider 6, local state because there is one operator.
- **Quarto for the dashboard.** It renders a static page from the laptop warehouse and pushes it to GitHub Pages in one command. That is the whole requirement, no server and no database exposed to the internet. Charts are Plotly; SQL decides every number and Python only draws. Quarto 1.10, Plotly 7.
- **uv and ruff.** uv pins Python 3.14 and the lockfile. ruff runs with its defaults, which grew in 0.16 and reshaped a few dict literals in the chart module.
- **One rule behind the list.** No package until it pays for itself, and no helper for two same-shaped call sites until a third appears. What was weighed and turned down is in Alternatives considered and rejected.

## How it fails, how quality is measured, how it watches itself

The trade-off taht shapes all three: Steam only reports the present. A rerun can reload an hour; it cannot observe one that was missed. Uptime is part of the dataset's quality, and the pipeline is built to say so rather than hide it.

### Failure modes

- **A game delisted from sale vanished from the universe.** Rocket League left Steam's app list when it stopped being sold; the app-list gate excluded it silently. Seed games now bypass the gate; curation vouches for them, the chart needs evidence.
- **A region-locked game was declared dead.** MahjongSoul had 14,000 players and no US storefront entry, so a missing metadata call deactivated it. Chart presence now outranks missing metadata: a game is deactivated only when it is off the chart and without metadata.
- **One game with no public stats sank the whole hour.** Steam returns 404 for Deadlock. With the default trigger rule, one failed mapped task cancelled the bundle for the other 94 games. The 404 is now data, a row with status `unobservable`, and the bundle task runs on `all_done`.
- **Manual test runs folded into scheduled hours.** The grain test failed on its first run: 36 violations from three cp1 hours where a manual run landed minutes after the tick. Staging keeps the observation closest to the hour; raw keeps every row.
- **The laptop was off.** Nine hours on 2026-09-09, six on 2026-09-14, and more since; 21 unrecorded hours out of 623. Nothing can be done about them after the fact, and the hour grid on the dashboard shows each one.
- **The run at restart fails.** With `catchup=False`, Airflow fires the latest missed interval as soon as it is back, before the warehouse container resolves. Five hours ran short this way, each an audit row with nothing behint it.
- **Steam was unreachable for an hour.** On 2026-09-16 at 13:00 UTC every one of the 145 fetches failed and the bundle landed with empty lists. That hour broke Deadlock's 24-hour unobservable streak, the warning test fired at the next build, and the rule was tightened: an hour that observed nothing is not evidence about anything.
- **A password change broke a run.** The warehouse password was rotated while Airflow was up; the 03:00 run failed until the restart. The old password was the word `warehouse`, which Airflow's secret masker redacted everywhere in the logs, including the module name.
- **The chart moved.** On 2026-09-16 the universe rule admitted 32 games in one night. The gate that refuses non-games refused the same six it refuses daily.
- **A deliberately killed load.** `load_raw_counts` was killed mid-run on 2026-09-10 13:00 and the run cleared twice: 105 rows and one audit row before and after, zero inserted on the rerun. The write-up is in [docs/2026-09-10-cp4-failure-demonstration.md](docs/2026-09-10-cp4-failure-demonstration.md).

### Data quality

- **43 dbt tests.** Non-negative counts, UTC timestamps, one observation per game per hour, accepted values for every status, and singular tests for the leaderboard's row contract and the hour spine's density. `dbt build` is green or nothing ships.
- **Deduplication in staging, never in raw.** Raw is append-only in the warehouse too; a duplicate observation is resolved by a rule in a view not by a delete.
- **Partial days are kept, with their hour count.** A day cut by an outage stays in the daily fact with `observed_hours` beside its average; the dashboard shows the count on hover. Dropping partial days erased a game that joined three days before an outage.
- **A sanity check against SteamDB.** Same top ten, same top two; day-over-day direction agreed on 5 of 6 pairs; every daily peak within 3% below SteamDB's, never above, which is what hourly sampling should do.
- **Freshness is declared on the raw tables.** Two hours for the hourly tables, 26 for the daily ones. Mart freshness is build freshness, and the dashboard footer says both.

### Observability

- **One audit row per run, per DAG.** `raw.snapshot_runs` and `raw.catalog_runs`, keyed on the run id. A cleared rerun refreshes its row, except the catalog's transition counts, which keep the first non-null value because a rerun finds the transitions already made.
- **An hour spine that names every hour.** `fct_snapshot_hours` runs from the first observation to now and gives each hour a status: complete, ran short, unrecorded, or unaudited for the 291 hours before the audit existed. It is a view, so the answer is live.
- **`unrecorded`, not `never_ran`.** The audit writer runs inside Airflow; when the warehouse is down it cannot write. No audit row and no facts means the warehouse heard nothing, and the status name claims no more than that.
- **A flag for games Steam never reports, and a canary for it.** `dim_game.is_unobservable` is true after 24 consecutive evidence hours in the unobservable list; a warning test flags any game in the latest hour's list that the flag has not caugh up with.
- **Latency per call.** Median and maximum per hour in the audit row. Steam answers in about 340 ms; the worst seen is 87 seconds.
- **The dashboard's pipeline page.** The hour grid, games observed per hour, latency on a log axis, and a footer on every page with the render time, the marts' last hour and the pipeline's last hour.

## What breaks as volume grows

Measured on 2026-09-17, with 147 games and 23 days of history.

| Quantity                              | Value                                                                              |
| ------------------------------------- | ---------------------------------------------------------------------------------- |
| Hourly run, median and p90            | 37 s and 63 s over 224 audited runs                                                |
| Hourly run at 106 games and at 147    | 36 s and 43 s                                                                      |
| Airflow's concurrency                 | 16 tasks per DAG, 32 overall; `expand` refuses more than 1,024 items               |
| One Steam call, median and worst seen | 338 ms and 87 s                                                                    |
| Catalog refresh, a normal day         | about 2 minutes for 147 metadata calls through a 4-slot pool                       |
| S3, total                             | 530 MB, of which 429 MB is the daily app-list snapshot and 9 MB the hourly bundles |
| Warehouse                             | 24 MB, 52,000 fact rows                                                            |
| `dbt build`, 55 nodes                 | 1.4 s                                                                              |

In the order they would bite:

- **The universe passes 1,024 games.** Airflow will not map a task over more items than `max_map_length`. A configuration ceiling, not a performance one, and the first hard stop. Not observed; read from the setting.
- **Staging views re-read raw on every query.** Every mart query goes through views over the raw tables, and `fct_snapshot_hours` counts staging rows per hour each time the dashboard renders. Fine at 52,000 rows. A thousand games for a year is 8.7 million, and each render becomes a table scan. Inferred; the remedy is materialized or incremental staging, deliberately not built yet.
- **Full-refresh marts.** `dbt build` rebuilds every table from scratch. 1.4 seconds today, minutes at millions of rows. Inferred; dbt's incremental models are the standard answer and are not needed at this size.
- **The catalog's metadata pass.** 147 games through four slots is two minutes, measured. Steam's storefront allows roughly 200 calls per five minutes, so a thousand games is a 25-minute refresh at best. Slope measured, ceiling inferred.
- **The app-list snapshot.** Twenty megabytes a day, forever, about 7 GB a year; 81% of the bucket today. Cheap to store and the only unbounded file, and it is parsed in worked memory every day. Measured.
- **What does not break.** The hourly run barely moves with the universe, because 16 fetches run at once and each takes a third of a second; the run is scheduler overhead. Hourly bundles are 25 KB, the warehouse is 24 MB, and the published dashboard's size is the plotting library, not the data.

## What would change in production

Most of what changes is where things run, not what they are. The model, the tests, the audit rows and the universe rule carry over unchanged.

- **Airflow off the laptop.** A managed deployment, Astronomer being the natural one for a project already on Astro. Every unrecorded hour so far has the same cause, a laptop that was off or asleep; a scheduler that is always on removes the largest failure mode by removing the laptop.
- **Secrets out of `.env`.** Connections and the Steam key live in the deployment's secrets backend, and the pipeline identity becomes an IAM role instead of a user whose key sits in a Terraform state file.
- **dbt orchestrated by Airflow.** A build after each hourly load, so the marts move with the pipeline and the two dates in the dashboard footer converge. Deliberately unscoped here so that the difference stays visible.
- **Alerts on the hour statuses.** A ran-short or unrecorded hour pages someone within the hour instead of waiting for a query or a glance at the grid.
- **Terraform state in a remote backend with locking.** This should be true the moment a second operator or a CI job applies.
- **Storage lifecycle.** The daily app-list snapshots move to a colder tier after a month; the hourly bundles stay hot, because they are the irreplaceable part.
- **A live dashboard instead of a static render.** With a warehouse reachable from the internet, the current Evidence line or a served Quarto replaces the local render, the publish command and the release dump.

The warehouse itself forks two ways, determined by the data:

- **RDS Postgres.** At 24 MB today and gigabytes at a thousand games for years, a managed Postgres keeps every model as it is, including the `ON CONFLICT` loads and the array columns, at a cost in the tens of dollars a month. This is the honest choice for the data this project has.
- **A columnar warehouse.** Redshift, BigQuery, Snowflake or Databricks if the universe grows toward the whole Steam catalog or the fact table gains a second grain. The dbt models port with dialect changes; the loads change shape, from row upserts to bulk merges; and the design's one Postgres-specific habit, arrays in the audit tables, becomes a struct or a child table. The staging-views-over-raw problem from the previous section disappears, because scanning is what these engines do.
