# KLTN_ — Step-by-step: SigNoz base + Tail Sampling experiment (EN)

This document records the exact steps performed in this workspace to:
1) Clone the upstream repos and bring up the SigNoz Docker Compose stack.
2) Enable OTLP JSON export to files for offline comparison.
3) Build and run a custom `signoz-otel-collector` image (with an additional "model" tail-sampling policy).
4) Run a baseline vs model experiment and compare results.

> Repos in this workspace:
> - `signoz/` (SigNoz + docker compose deployment)
> - `signoz-otel-collector/` (custom collector, Go code)

---

## 0) Prerequisites

- Linux
- Docker Engine installed
- Docker Compose CLI as `docker-compose` (this setup used `docker-compose`, not the `docker compose` plugin)
- Git
- Python 3 (for the comparison script)

Quick checks:

```bash
git --version

docker --version

docker-compose --version

python3 --version
```

---

## 1) Clone repositories

From the workspace root:

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_

git clone https://github.com/SigNoz/signoz.git

git clone https://github.com/SigNoz/signoz-otel-collector.git
```

---

## 2) Bring up the SigNoz stack (base)

### 2.1) Start services

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker

docker-compose up -d

docker-compose ps
```

### 2.2) Open the UI

- Open: http://localhost:8080

### 2.3) Verify core containers

`docker-compose ps` should include at least:
- `signoz`
- `clickhouse`
- `zookeeper-1`
- `signoz-otel-collector`
- `schema-migrator-sync` (usually exits after sync)

---

## 3) Enable OTLP JSON export (offline diff)

Goal: keep exporting to ClickHouse (so the UI works) while also exporting to local JSON files.

### 3.1) Update the collector config

File: `signoz/deploy/docker/otel-collector-config.yaml`

Added exporters:
- `file/traces`
- `file/logs`

And wired them into the `traces` and `logs` pipelines.

### 3.2) Mount export folder to the host

File: `signoz/deploy/docker/docker-compose.yaml`

Mounted:
- `./otel-export:/var/signoz/otel-export`

Create the folder on host:

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker
mkdir -p otel-export
```

### 3.3) Recreate the collector

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker

docker-compose up -d --force-recreate otel-collector

docker logs --since=2m signoz-otel-collector | tail -n 80
```

---

## 4) Generate baseline traces

Using the bundled HotROD generator:

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker/generator/hotrod

docker-compose up -d
sleep 45
docker-compose down
```

Check exported files:

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker
ls -lh otel-export/
```

Snapshot baseline (example folder `baseline2/` exists in this workspace):

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker
mkdir -p otel-export/baseline2

ts=$(date +%Y%m%d-%H%M%S)
cp -f otel-export/traces.json otel-export/baseline2/${ts}-traces.json
cp -f otel-export/logs.json   otel-export/baseline2/${ts}-logs.json
```

Note: HotROD typically emits traces but not OTLP logs, so logs export may remain empty.

---

## 5) Build & use a custom collector image

### 5.1) Where are the sampling changes?

See: `README.sampling.md`.

### 5.2) Custom image tag

The custom image built in this workspace:
- `signoz/signoz-otel-collector:kltn-model`

### 5.3) Configure compose to use it

File: `signoz/deploy/docker/.env`

Set:
- `OTELCOL_TAG=kltn-model`

And keep schema migrator on an upstream tag:
- `SCHEMA_MIGRATOR_TAG=v0.129.13`

### 5.4) Two runtime fixes applied

1) The compose file originally used `OTELCOL_TAG` for schema migrator too, causing it to pull a non-existent image tag.
   - Fix: use a separate `SCHEMA_MIGRATOR_TAG`.

2) The custom collector crashed due to an invalid flag:
   - `--feature-gates=-pkg.translator.prometheus.NormalizeName`
   - The gate is stable and cannot be disabled.
   - Fix: remove the flag from the compose command.

### 5.5) Recreate collector services

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker

docker-compose up -d --force-recreate schema-migrator-sync otel-collector

docker-compose ps schema-migrator-sync otel-collector
```

Verify the image tag:

```bash
docker-compose ps otel-collector
```

---

## 6) Enable model sampling and generate the “after/model” dataset

### 6.1) Ensure tail sampling processor is enabled

File: `signoz/deploy/docker/otel-collector-config.yaml`

- `processors.signoz_tail_sampling` contains a policy with `type: model`.
- The `traces` pipeline processors include `signoz_tail_sampling` before `batch`.

### 6.2) Use separate output files to avoid overwriting baseline

In this workspace, the model run exports to:
- `otel-export/traces.model.json`
- `otel-export/logs.model.json`

### 6.3) Recreate collector + run HotROD for 45 seconds

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker

docker-compose up -d --force-recreate otel-collector

cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker/generator/hotrod

docker-compose up -d
sleep 45
docker-compose down

cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker
ls -lh otel-export/traces.model.json otel-export/logs.model.json
```

Snapshot the model run (example folder `otel-export/model1/` exists):

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker
mkdir -p otel-export/model1

ts=$(date +%Y%m%d-%H%M%S)
cp -f otel-export/traces.model.json otel-export/model1/${ts}-traces.model.json
cp -f otel-export/logs.model.json   otel-export/model1/${ts}-logs.model.json
```

---

## 7) Compare baseline vs model with a script

For compare + auto-saved reports, see: `README.compare.md`.

Script: `scripts/compare_otlp_traces.py`

Example command (using snapshots present in this workspace):

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_
python3 scripts/compare_otlp_traces.py \
  --base signoz/deploy/docker/otel-export/baseline2/20260202-225522-traces.json \
  --test signoz/deploy/docker/otel-export/model1/20260202-230513-traces.model.json \
  --details
```

---

## 8) Validate via UI

- Open: http://localhost:8080
- With model sampling enabled, the number of ingested traces should drop significantly compared to baseline.

---

## 9) Known limitations

- The logs export may remain empty because HotROD does not emit OTLP logs.
  If you must compare logs, add an OTLP logs generator.
