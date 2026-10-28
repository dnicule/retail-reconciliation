Retail Product Reconciliation

A retailer uses a PostgreSQL-backed sales application for employee quotations and a MongoDB-backed online catalog. When an approved product change reaches only one database, staff and customers see different information and teams must correct it manually. This locally runnable proof of concept makes incomplete deliveries recoverable and exposes disagreements that need a business decision.

The solution assumes an existing Update API receives approved, complete product snapshots from one revision authority. The included API represents that integration point: it records each update durably before attempting both destinations in parallel, while a separate worker repairs incomplete approved updates and preserves newer data.

## Table of contents

- [Table of contents](#table-of-contents)
- [Solution at a glance](#solution-at-a-glance)
- [Architecture](#architecture)
- [Customer context and scope](#customer-context-and-scope)
- [Business Value](#business-value)
- [Trade-offs and limitations](#trade-offs-and-limitations)
- [Known limitations](#known-limitations)
- [Quick start](#quick-start)
  - [Prerequisites](#prerequisites)
  - [First-time setup](#first-time-setup)
  - [DEMO](#demo)
    - [Prerequisites](#prerequisites-1)
    - [Demo 1 - Successful product creation in both systems](#demo-1---successful-product-creation-in-both-systems)
    - [Demo 2 - Repeated request](#demo-2---repeated-request)
    - [Demo 3 - Partial failure, automatic retries, recovery](#demo-3---partial-failure-automatic-retries-recovery)
    - [Demo 4 - Reconciliation conflict](#demo-4---reconciliation-conflict)
    - [Demo 5 - Business decision resolving conflict](#demo-5---business-decision-resolving-conflict)
- [Observability](#observability)
- [Use of AI](#use-of-ai)

## Solution at a glance

The implemented path accepts approved updates, attempts both writes, automatically recovers eligible incomplete work, and exposes durable status through logs and read-only API routes.

| Available today                                                    | What it provides                                                                                                                             |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| [Update API](update-api/README.md)                                 | Durable, idempotent admission; initial parallel delivery                                                                                     |
| [Reconciliation worker](reconciliation-worker/README.md)           | Continuous update-job processing, bounded retries, conditional repairs, conflicts, and recovery after lease expiry                           |
| [Database tools](dbInit/README.md)                                 | Explicit initialization/fixture commands, stable API requests, synthetic missing-copy jobs, and a reversible PostgreSQL product-write fault. |
| [Local databases](DOCKER.md#network-and-existing-database-startup) | PostgreSQL 18 (`myapp`) and MongoDB 8 (`retail_catalog`), with existing structures and records.                                              |
| [Observability stack](observability/README.md)                     | Docker log collection through Alloy and Loki, viewed in Grafana.                                                                             |

## Architecture

![Retail reconciliation architecture: approved product snapshots enter the API, which commits a durable job before parallel writes; the worker repairs eligible updates; Alloy, Loki and Grafana collect and expose logs.](docs/images/retail-product-reconciliation-light.svg)

Green paths show admission and initial delivery; gray paths show worker recovery and log ingestion; dashed purple paths show Docker logs. The API commits the immutable update before either product write. The worker processes eligible update jobs, preserves newer revisions, and verifies repairs.

</details>

## Customer context and scope

The customer flow handles approved product updates, records them before delivery, and automatically repairs incomplete deliveries. It covers retained update jobs; general drift detection and a dedicated reconciliation dashboard remain future targets.

Happy path:

![Happy Path SVG](docs/images/approved-product-updates-from-approval-to-business-value-light.svg)

1. Receive the approved update. The source supplies an event ID, product ID, revision, and complete shared content: name, description, and category. The API validates the request and recognizes identical replays.

2. Record before writing. The API commits an immutable update job in PostgreSQL before attempting either destination. This preserves the approved intent for recovery.

3. Start parallel delivery. After the job commit is confirmed, the API independently attempts 3a, the employee quotation database, and 3b, the online catalog. Revision guards preserve newer data and destination-specific fields.

4. Recover incomplete work. The worker discovers eligible jobs through notifications and periodic scans. It reads both destinations, selects the highest valid agreeing revision, and repairs PostgreSQL (4a) or MongoDB (4b) as needed. It verifies the result before completion; unresolved conflicts or exhausted retries remain blocked.

5. Support the business goal. Successful delivery and recovery bring employee-facing information (5a) and customer-facing information (5b) into agreement, reducing manual corrections.

6. Expose progress and problems. Structured logs record processing outcomes. Read-only API diagnostics expose job status and current product observations.

7. Show future scope. A dedicated reconciliation dashboard and a separate process for discovering drift outside update jobs are planned extensions.

Green arrows identify parallel initial writes. Dashed arrows show outcomes, visibility, and future targets.

## Business Value

Fewer manual corrections and consistent product information after successful recovery, with explicit evidence when automation cannot decide safely. Coverage is limited to **approved update jobs**.

Scanning all products, process audit rows, or revisit completed jobs to discover later drift are out of scope. A separate future integrity process would own general drift discovery (version 2).

## Trade-offs and limitations

| Decision                                            | Benefit                                                                 | Cost or limitation                                                                |
| --------------------------------------------------- | ----------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| PostgreSQL jobs table                               | Durable admission, scheduling and claims using existing infrastructure. | PostgreSQL availability gates admission and coordination.                         |
| Independent writes and eventual recovery            | One destination can progress during failure of the other.               | Temporary divergence; no cross-database atomicity or rollback.                    |
| One revision authority and immutable full snapshots | Clear ordering, safe replay and missing-copy reconstruction.            | Upstream must enforce approval and revision semantics                             |
| One active job per worker                           | Bounded execution and understandable failures.                          | Limited throughput per worker; multi-worker performance has not been benchmarked. |
| Eight attempts, then a blocked state                | Bounds repeated failures and exposes intervention needs.                | Restoring a dependency cannot resume an exhausted cycle without external requeue. |
| Retain all jobs in the PoC                          | Preserves deduplication and approved recovery history.                  | Storage grows; no automatic archival or separate attempt-history table.           |
| Separate API and worker                             | Fast initial delivery and independent background recovery               | Two services to deploy, configure and maintain                                    |

Scope excludes full sales/catalog/product-management application. Intentional deletion would need versioned tombstones; otherwise missing copies may be reconstructed.

## Known limitations

- **Approved updates only:** the worker does not discover unrelated drift, process audit rows or revisit completed jobs.
- **Temporary inconsistency:** PostgreSQL and MongoDB writes are independent; no cross-database transaction or rollback exists.
- **PostgreSQL dependency:** outages prevent new update admission and worker job claims.
- **Limited shared fields:** synchronization covers only `name`, `description` and `category`.
- **No intentional deletion support:** missing products may be reconstructed; approved deletion would require versioned tombstones.
- **Local-field recovery limits:** reconstructing a deleted product cannot recover its destination-specific fields.
- **No intervention interface:** conflicts and attention jobs remain blocked until an external owner applies the requeue contract.
- **Unmeasured performance:** throughput, recovery latency and multi-worker performance have not been benchmarked.
- **No job archival:** retained jobs grow indefinitely; no separate attempt-history table exists.
- **Demo deployment:** production authentication, authorization, TLS and operational hardening are outside the delivered scope.
- **No permanent consistency guarantee:** completion confirms an observation; later writes can introduce divergence.

## Quick start

### Prerequisites

- Start Docker and open a terminal in the copied project folder.

- You need Docker with Linux containers, Compose **2.30+**, and `curl`. The setup script does not need local Node, npm, or nvm.

- Run from the root dir `sh ./setup.sh --help` to display its usage.

### First-time setup

1. **Start infrastructure**

```sh

sh ./setup.sh --infrastructure

```

NOTE: If it creates `observability/.env`, set `HOST_ID` and `GRAFANA_ADMIN_PASSWORD`, then run the same command again. Wait for **Infrastructure is ready**.

2. **Prepare fresh databases separately.**

Run these one at a time, from the project root, only when the application schemas do not exist:

```sh

sh database/initialize-postgres.sh

```

```sh

docker exec -i mongodb-mongo-1 mongosh --quiet --norc \

'mongodb://127.0.0.1:27017/retail_catalog?serverSelectionTimeoutMS=5000' \

--file /dev/stdin < database/initialize-mongo.js

```

Wait for each command to succeed before continuing. These use the existing containers and create schemas without sample data or local Node/npm. Skip this step on normal reruns. See [database preparation](database/README.md) if a step fails.

3. **Start the API and worker.**

```sh

sh ./setup.sh

```

If asked, fill in the database credentials in `update-api/.env.docker` and `reconciliation-worker/.env.docker`, then rerun. Keep the provided Docker hostnames `postgres` and `mongo`.

4. **Check everything.**

```sh

sh ./setup.sh --check

```

This checks the services without starting or changing them.

### DEMO

#### Prerequisites

- curl installed

- RANDOM and uuidgen available (should be available on MacOS) - can be relaced with manually random generated id and number

#### Demo 1 - Successful product creation in both systems

Below curl request creates a new product by calling the Update-API (jobId and productId are randomly generated)

```sh

curl --request POST \

--url 'http://127.0.0.1:43187/approved-updates' \

--header 'Content-Type: application/json' \

--write-out '\nHTTP status: %{http_code}\n' \

--data "{

\"eventId\": \"$(uuidgen)\",

\"productId\": \"SKU-DEMO-${RANDOM}\",

\"revision\": 1,

\"product\": {

\"name\": \"Demo Product\",

\"description\": \"Demo Description\",

\"category\": \"demo\"

}

}"

```

HTTP **200** confirms a completed job

`status`: `completed` - confirms the update was done.

`revision`: 1 - shows the product revision

Save `jobId` and `productId` for further demo scenarios

```JSON

{"jobId":"dd097a9b-7b1d-4656-bcb4-98feeba7d8f7","productId":"SKU-DEMO-5537","revision":1,"status":"completed","duplicate":false,"statusFresh":true,"statusAsOf":"2026-10-28T15:18:13.678Z"}

HTTP status: 200

```

#### Demo 2 - Repeated request

Use the same curl but replace jobId and productId with previously saved values

```sh

curl --request POST \

--url 'http://127.0.0.1:43187/approved-updates' \

--header 'Content-Type: application/json' \

--write-out '\nHTTP status: %{http_code}\n' \

--data "{

\"eventId\": \"dd097a9b-7b1d-4656-bcb4-98feeba7d8f7\",

\"productId\": \"SKU-DEMO-5537\",

\"revision\": 1,

\"product\": {

\"name\": \"Demo Product\",

\"description\": \"Demo Description\",

\"category\": \"demo\"

}

}"

```

HTTP 200 confirm successful request

`duplicate: true` - shows duplicate request for the same jobId/productId

`status`: `completed` - shows status for existing jobId

```json

{"jobId":"dd097a9b-7b1d-4656-bcb4-98feeba7d8f7","productId":"SKU-DEMO-5537","revision":1,"status":"completed","duplicate":true,"statusFresh":true,"statusAsOf":"2026-10-28T15:23:46.294Z"}



HTTP status: 200

```

#### Demo 3 - Partial failure, automatic retries, recovery

Stop the mongo db container. From the project root dir run:

```sh

docker compose -f mongoDb/mongoCompose.yaml stop mongo

```

Use curl to create a new product:

```sh

curl --request POST \

--url 'http://127.0.0.1:43187/approved-updates' \

--header 'Content-Type: application/json' \

--write-out '\nHTTP status: %{http_code}\n' \

--data "{

\"eventId\": \"$(uuidgen)\",

\"productId\": \"SKU-DEMO-${RANDOM}\",

\"revision\": 1,

\"product\": {

\"name\": \"Demo Product\",

\"description\": \"Demo Description\",

\"category\": \"demo\"

}

}"

```

NOTE: worker runs 8 attempts before marking a new creation product job with `needs_attention` (takes about 3.5 minutes). Make sure you go through the below steps more quickly.

HTTP 200 confirms successful request

`status:pending` indicates the product is pending to be created in one of the DBs (checks out since Mongo DB is down)

```json

{"jobId":"cc208711-3717-463e-9428-57524492fb9f","productId":"SKU-DEMO-2410","revision":1,"status":"pending","duplicate":false,"statusFresh":true,"statusAsOf":"2026-10-28T15:34:40.494Z"}



HTTP status: 202

```

Check job status reusing the `jobid`:

```sh

curl -sS 'http://127.0.0.1:43187/jobs/cc208711-3717-463e-9428-57524492fb9f' | jq

```

Response confirms MongoDB unavailable

`attemptCount` confirms worker attempts

`status` is still `pending`

Notice the initial product payload is actually saved in the jobs table as `approvePayload`

```json
{
  "jobId": "cc208711-3717-463e-9428-57524492fb9f",

  "kind": "update",

  "productId": "SKU-DEMO-2410",

  "revision": 1,

  "approvedPayload": {
    "name": "Demo Product",

    "description": "Demo Description",

    "category": "demo"
  },

  "status": "pending",

  "attemptCount": 2,

  "outcomes": {
    "sql": "unknown",

    "mongo": "retry"
  },

  "nextAttemptAt": "2026-10-28T15:35:10.109Z",

  "leaseExpiresAt": null,

  "lastError": {
    "code": "MONGO_UNAVAILABLE",

    "message": "The destination operation could not be confirmed; a submitted write may have applied."
  },

  "conflictDetails": null,

  "createdAt": "2026-10-28T15:34:37.449Z",

  "updatedAt": "2026-10-28T15:35:08.220Z",

  "completedAt": null
}
```

Restore MongoDB promptly, before exhausting eight started attempts

```sh

docker compose -f mongoDb/mongoCompose.yaml start mongo

```

Observe recovery by repeating these reads (use the saved `jobid` and `productId`):

```sh

curl -sS "http://127.0.0.1:43187/jobs/cc208711-3717-463e-9428-57524492fb9f" | jq

curl -sS "http://127.0.0.1:43187/products/SKU-DEMO-2410/state" | jq

```

Response should show `status:complete` and both outcomes `sql:satisfied`, `mongo:stasfied` .

#### Demo 4 - Reconciliation conflict

Stop the mongo db container and worker container. From the project root dir run:

```sh

docker compose -f mongoDb/mongoCompose.yaml stop mongo

docker compose -f reconciliation-worker/compose.yaml stop reconciliation-worker

```

Use curl to create a new product:

```sh

curl --request POST \

--url 'http://127.0.0.1:43187/approved-updates' \

--header 'Content-Type: application/json' \

--write-out '\nHTTP status: %{http_code}\n' \

--data "{

\"eventId\": \"$(uuidgen)\",

\"productId\": \"SKU-DEMO-${RANDOM}\",

\"revision\": 1,

\"product\": {

\"name\": \"Demo Product\",

\"description\": \"Demo Description\",

\"category\": \"demo\"

}

}"

```

Grab the jobId and confirm is `pending` and that PostgreSQL received the update

```sh

curl -sS 'http://127.0.0.1:43187/jobs/d93ec96d-b202-47f5-bf2a-cc9239e43071' | jq

curl -sS "http://127.0.0.1:43187/products/SKU-DEMO-13848/state" | jq

```

Start mongo container

```sh

docker compose -f mongoDb/mongoCompose.yaml start mongo

```

Use this shell command to create a product, with the same revision but with a different description (remember to replace the productId with the actual productId created in step 1)

```sh

docker compose -f mongoDb/mongoCompose.yaml exec -T mongo \

mongosh retail_catalog --quiet --eval 'db.products.insertOne({

_id: "SKU-DEMO-13848",

sourceRevision:1,

name: "Demo Product",

description: "Demo Description 2",

category: "demo",

appliedAt: new Date(),

seoTitle: null

})'

```

```json
{ "acknowledged": true, "insertedId": "SKU-DEMO-13848" }
```

Now we have in both PostgreSQL and MongoDB the same products, with the same revision but a different description

Start the worker container

```sh

docker compose -f reconciliation-worker/compose.yaml start reconciliation-worker

```

Query the job until the status reaches: conflict

```sh

curl -sS 'http://127.0.0.1:43187/jobs/d93ec96d-b202-47f5-bf2a-cc9239e43071' | jq

```

Check the `lastError.status` for the reason: `Highest revision candidates disagree on shared content.`

#### Demo 5 - Business decision resolving conflict

For the product created in Demo 4 we will update with a new revision (remember to replace the product ID with the one from Demo 4)

Use the curl to create a new product revision:

```sh

curl --request POST \

--url 'http://127.0.0.1:43187/approved-updates' \

--header 'Content-Type: application/json' \

--write-out '\nHTTP status: %{http_code}\n' \

--data "{

\"eventId\": \"$(uuidgen)\",

\"productId\": \"SKU-DEMO-13848\",

\"revision\": 2,

\"product\": {

\"name\": \"Demo Product\",

\"description\": \"Demo Description revision 2\",

\"category\": \"demo\"

}

}"

```

Response shows `status:completed` and `revision:2` .

## Observability

![Observability](observability/image.png)

## Use of AI

AI supported research, code generation, README development, and infrastructure provisioning. AI-generated code was manually reviewed, and infrastructure provisioning was carried out manually with AI guidance.
Setup, demos, and the observability dashboard were completed manually.
