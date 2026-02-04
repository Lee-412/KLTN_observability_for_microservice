# KLTN – Observability for Microservices with SigNoz and Model-based Tail Sampling

## Overview

This repository contains my **graduation thesis (KLTN)** project focusing on **observability for microservice systems**, with a practical study on **tail-based trace sampling** using **SigNoz** and **OpenTelemetry**.

The project starts from the standard SigNoz stack and extends it by introducing a **model-based tail sampling policy**. Instead of sampling traces purely by rules or probability, this approach uses a **linear scoring model** based on trace features (such as duration, span count, and error signals) to decide which traces should be retained.

The goal is to:

* Preserve the default SigNoz tail sampling behavior.
* Extend the collector with a new sampling policy (`type: model`).
* Evaluate the impact of model-based sampling compared to a baseline setup.

This repository includes:

* A full SigNoz Docker Compose setup.
* A customized `signoz-otel-collector` with model-based tail sampling.
* Configuration and scripts to run baseline vs model experiments.

---

## Project Structure

```
KLTN_observability_for_microservice/
├── signoz/                   # SigNoz deployment (Docker Compose, ClickHouse, UI)
├── signoz-otel-collector/    # Custom OpenTelemetry Collector with model sampling
├── scripts/                  # Helper scripts (comparison, analysis)
├── docs/                     # Design notes and experiment documentation
└── README.md                 # Project documentation (this file)
```

---

## Architecture Overview

At a high level, the system consists of:

* **Instrumented microservices** emitting OTLP traces.
* **SigNoz OpenTelemetry Collector**

  * Applies tail-based sampling.
  * Supports a custom `model` policy for score-based sampling.
* **ClickHouse** for trace storage.
* **SigNoz UI** for visualization and analysis.

The model-based sampler computes a linear score per trace and samples traces whose score exceeds a configurable threshold.

---

## Prerequisites

Make sure the following tools are installed:

* Linux environment
* Docker Engine
* Docker Compose (`docker-compose` CLI)
* Git
* (Optional) Python 3 for offline comparison scripts

Check versions:

```bash
git --version
docker --version
docker-compose --version
python3 --version
```

---

## Installation and Setup

### 1) Clone the repository

```bash
git clone https://github.com/Lee-412/KLTN_observability_for_microservice.git
cd KLTN_observability_for_microservice
```

---

### 2) Build the custom collector image

The collector includes a custom **model-based tail sampling policy**.

```bash
cd signoz-otel-collector

docker build -t signoz/signoz-otel-collector:kltn-model .
```

---

### 3) Configure SigNoz to use the custom collector

Edit the environment file:

```bash
cd ../signoz/deploy/docker
nano .env
```

Set the collector image tag:

```env
OTELCOL_TAG=kltn-model
SCHEMA_MIGRATOR_TAG=v0.129.13
```

---

### 4) Start the SigNoz stack

```bash
cd signoz/deploy/docker

docker-compose up -d
```

Verify services:

```bash
docker-compose ps
```

Once started, open the SigNoz UI:

* [http://localhost:8080](http://localhost:8080)

---

### 5) Enable tail sampling with the model policy

The tail sampling processor is configured in:

```
signoz/deploy/docker/otel-collector-config.yaml
```

Example policy:

```yaml
processors:
  signoz_tail_sampling:
    policies:
      - name: model-policy
        type: model
        model:
          type: linear
          threshold: 2.0
          intercept: 0.0
          weights:
            duration_ms: 0.01
            span_count: 0.05
            has_error: 2.0
```

Make sure `signoz_tail_sampling` is included in the **traces pipeline** before the `batch` processor.

---

### 6) Generate test traces

You can use the built-in **HotROD** trace generator:

```bash
cd signoz/deploy/docker/generator/hotrod

docker-compose up -d
sleep 30
docker-compose down
```

Traces should now appear in the SigNoz UI.

---

## Baseline vs Model Experiment

The typical experiment flow is:

1. **Baseline run**: Tail sampling disabled or using default SigNoz policies.
2. **Model run**: Tail sampling enabled with the `model` policy.
3. Compare:

   * Number of ingested traces
   * Coverage of error traces
   * Trace duration distribution

Optional file exporters can be enabled to export OTLP traces as JSON for offline analysis.


## Future Work

* Support more advanced models (logistic regression, tree-based models).
* Learn weights automatically from historical traces.
* Extend feature set with service-level and attribute-based signals.

---

## Author

**Lee**
Bachelor Thesis – Observability for Microservices
Vietnam National University

---

## License

This project is for academic and research purposes.
