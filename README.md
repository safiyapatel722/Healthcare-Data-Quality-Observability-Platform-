# 🏥 Healthcare Data Quality & Observability Platform

> A production-grade batch data engineering platform built on Google Cloud Platform, designed to ingest, transform, validate, and monitor COVID-19 public health data with end-to-end observability.

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?style=flat-square&logo=apache-spark)
![dbt](https://img.shields.io/badge/dbt-1.9-red?style=flat-square&logo=dbt)
![BigQuery](https://img.shields.io/badge/BigQuery-GCP-blue?style=flat-square&logo=google-cloud)
![GX](https://img.shields.io/badge/Great_Expectations-1.1-green?style=flat-square)
![Elementary](https://img.shields.io/badge/Elementary-0.23-purple?style=flat-square)
![CI/CD](https://img.shields.io/badge/GitHub_Actions-CI%2FCD-black?style=flat-square&logo=github-actions)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Pipeline](#data-pipeline)
- [dbt Models](#dbt-models)
- [Data Quality](#data-quality)
- [Observability](#observability)
- [Orchestration](#orchestration)
- [CI/CD](#cicd)
- [Dashboards](#dashboards)
- [Setup & Installation](#setup--installation)
- [Environment Variables](#environment-variables)
- [Contributing](#contributing)

---

## 🔍 Overview

This platform demonstrates a production-ready batch data engineering pipeline that processes the **Google COVID-19 Open Data** dataset (~700 columns, millions of rows) through multiple layers of transformation, validation, and observability.

The platform answers three critical public health questions:

| Dashboard | Business Question |
|-----------|-------------------|
| 🦠 Pandemic Impact | How did hospital pressure and case fatality rates evolve across countries? |
| 💉 Vaccination Effectiveness | How did vaccination coverage correlate with case and death reductions? |
| 🏛️ Government Response | Did stricter government policies result in better health outcomes? |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA INGESTION LAYER                         │
│                                                                     │
│   BigQuery Public Dataset                                           │
│   (bigquery-public-data.covid19_open_data)                          │
│              │                                                      │
│              ▼                                                      │
│   PySpark Job (Dataproc Serverless)                                 │
│   ┌─────────────────────────────┐                                   │
│   │  extract_covid_data.py      │──────► GCS Raw Zone               │
│   │  load_to_bigquery.py        │──────► BigQuery Staging           │
│   └─────────────────────────────┘                                   │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      TRANSFORMATION LAYER (dbt)                     │
│                                                                     │
│              stg_covid19 (Staging Model)                            │
│              ┌──────────────────────────┐                           │
│              │ - Null handling          │                           │
│              │ - Type casting           │                           │
│              │ - Filter invalid records │                           │
│              └──────────────────────────┘                           │
│                           │                                         │
│          ┌────────────────┼────────────────┐                        │
│          ▼                ▼                ▼                        │
│  mart_pandemic_   mart_vaccination_  mart_government_               │
│  impact           effectiveness      response                       │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA QUALITY LAYER                               │
│                                                                     │
│   Great Expectations              Elementary                        │
│   ┌─────────────────────┐        ┌─────────────────────┐            │
│   │ 16 Expectations     │        │ 32 Monitoring Models │           │
│   │ Schema validation   │        │ Freshness tracking   │           │
│   │ Range checks        │        │ Anomaly detection    │           │
│   │ Regex validation    │        │ HTML Report          │           │
│   └─────────────────────┘        └─────────────────────┘            │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION & CI/CD                            │
│                                                                     │
│   Cloud Composer DAG              GitHub Actions                    │
│   ┌─────────────────────┐        ┌─────────────────────┐            │
│   │ Daily @ midnight    │        │ On push/PR to main  │            │
│   │ 7 task pipeline     │        │ dbt run + test      │            │
│   │ Retry logic         │        │ Failure alerts      │            │
│   └─────────────────────┘        └─────────────────────┘            │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      VISUALIZATION LAYER                            │
│                                                                     │
│              Looker Studio (3 Dashboards)                           │
│   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                │
│   │ Pandemic     │ │ Vaccination  │ │ Government   │                │
│   │ Impact       │ │ Effectiveness│ │ Response     │                │
│   └──────────────┘ └──────────────┘ └──────────────┘                │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Category | Tool | Purpose |
|----------|------|---------|
| **Batch Processing** | PySpark 3.5 on Dataproc Serverless | Large-scale data extraction and loading |
| **Data Warehouse** | Google BigQuery | Centralized analytical storage |
| **Object Storage** | Google Cloud Storage | Raw zone for Parquet files |
| **Transformation** | dbt 1.9 + BigQuery adapter | SQL-based modeling and testing |
| **Data Quality** | Great Expectations 1.1 | Expectation-based validation |
| **Observability** | Elementary 0.23 | dbt monitoring and reporting |
| **Orchestration** | Cloud Composer (Airflow) | Pipeline scheduling and dependency management |
| **CI/CD** | GitHub Actions | Automated testing on every push |
| **Visualization** | Looker Studio | Business intelligence dashboards |
| **Config Management** | Pydantic Settings | Environment-based configuration |
| **Language** | Python 3.11 | Primary development language |

---

## 📁 Project Structure

```
healthcare-data-quality-observability-platform/
│
├── .github/
│   └── workflows/
│       └── dbt_ci.yml                  # GitHub Actions CI/CD pipeline
│
├── config/
│   └── settings.py                     # Pydantic settings & env config
│
├── dags/
│   └── healthcare_pipeline_dag.py      # Cloud Composer Airflow DAG
│
├── gx/                                 # Great Expectations configuration
│   ├── great_expectations.yml
│   └── expectations/
│       └── covid19_quality_suite.json
│
├── healthcare_dbt/                     # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_covid19.sql         # Staging model
│   │   │   ├── stg_covid19.yml         # Tests & documentation
│   │   │   └── sources.yml             # Source definitions
│   │   └── marts/
│   │       ├── mart_pandemic_impact.sql
│   │       ├── mart_pandemic_impact.yml
│   │       ├── mart_vaccination_effectiveness.sql
│   │       ├── mart_vaccination_effectiveness.yml
│   │       ├── mart_government_response.sql
│   │       └── mart_government_response.yml
│   ├── packages.yml                    # Elementary + dbt_utils
│   └── dbt_project.yml
│
├── src/
│   ├── ingestion/
│   │   ├── extract_covid_data.py       # PySpark extraction job
│   │   └── load_to_bigquery.py         # PySpark loading job
│   └── quality/
│       ├── setup_gx.py                 # GX context initialization
│       ├── create_expectations.py      # Expectation suite definition
│       └── run_validations.py          # Validation execution
│
├── .env.example                        # Environment variable template
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 🔄 Data Pipeline

### Raw Data Source
- **Dataset:** `bigquery-public-data.covid19_open_data.covid19_open_data`
- **Size:** ~700 columns, millions of rows
- **Coverage:** Global COVID-19 data from 2020 to 2022

### Pipeline Stages

**Stage 1 — Extraction (PySpark)**
```
BigQuery Public Dataset
        ↓
PySpark reads via BigQuery connector
        ↓
Writes Parquet to GCS raw zone
gs://healthcare-data-platform/raw/covid19/
```

**Stage 2 — Loading (PySpark)**
```
GCS Parquet files
        ↓
PySpark reads Parquet
        ↓
Writes to BigQuery staging table
covid19_raw → healthcare_staging dataset
```

**Stage 3 — Transformation (dbt)**
```
healthcare_staging.covid19_raw
        ↓
stg_covid19 (cleaned view)
        ↓
3 mart models (aggregated views)
```

---

## 📊 dbt Models

### Staging Layer

**`stg_covid19`**
- Coalesces nulls to 0 for all numeric columns
- Filters out negative values (data correction artifacts)
- Filters invalid dates (pre-2020 or future dates)
- Selects only relevant columns from 700+ available

### Mart Layer

**`mart_pandemic_impact`**

| Column | Description |
|--------|-------------|
| `total_new_cases` | Daily new confirmed cases by country |
| `total_new_deaths` | Daily new deaths by country |
| `current_icu_patients` | Current ICU occupancy |
| `current_ventilator_patients` | Current ventilator usage |
| `case_fatality_rate` | Deaths / Confirmed cases × 100 (capped at 100%) |
| `hospitalization_rate` | Hospitalized / Cumulative confirmed × 100 |

**`mart_vaccination_effectiveness`**

| Column | Description |
|--------|-------------|
| `total_new_vaccinated` | New vaccinations per day |
| `total_cumulative_fully_vaccinated` | Running total fully vaccinated |
| `total_doses_pfizer` | Pfizer doses administered |
| `total_doses_moderna` | Moderna doses administered |
| `total_doses_janssen` | Janssen doses administered |
| `vaccination_coverage_pct` | Fully vaccinated / Population × 100 |

**`mart_government_response`**

| Column | Description |
|--------|-------------|
| `avg_stringency_index` | Oxford stringency index (0-100) |
| `school_closing` | School closure policy level (0-3) |
| `stay_at_home_requirements` | Stay-at-home policy level (0-3) |
| `international_travel_controls` | Travel restriction level (0-4) |
| `avg_hdi` | Human Development Index |
| `avg_gdp_per_capita` | GDP per capita in USD |

### dbt Test Coverage

```
Total tests: 36
├── stg_covid19:           8 tests
├── mart_pandemic_impact:  10 tests
├── mart_vaccination:      11 tests
└── mart_gov_response:     7 tests

All tests: PASSING ✅
```

---

## ✅ Data Quality

### Great Expectations Suite: `covid19_quality_suite`

| Expectation | Column | Rule |
|-------------|--------|------|
| `ExpectTableRowCountToBeBetween` | — | Minimum 1,000 rows |
| `ExpectColumnToExist` | Multiple | Key columns must exist |
| `ExpectColumnValuesToNotBeNull` | `date`, `country_code` | No nulls in key columns |
| `ExpectColumnValueLengthsToBeBetween` | `country_code` | Exactly 2 characters |
| `ExpectColumnValuesToMatchRegex` | `country_code` | Must match `^[A-Z]{2}$` |
| `ExpectColumnMinToBeBetween` | `total_new_cases` | Minimum value >= 0 |
| `ExpectColumnMinToBeBetween` | `total_new_deaths` | Minimum value >= 0 |
| `ExpectColumnValuesToBeBetween` | `case_fatality_rate` | Between 0 and 1000 (99% of rows) |
| `ExpectColumnMinToBeBetween` | `current_icu_patients` | Minimum value >= 0 |
| `ExpectColumnMinToBeBetween` | `current_ventilator_patients` | Minimum value >= 0 |

**Validation Results: 16/16 passing ✅**

### Data Quality Decisions

- **Negative case counts:** Filtered at staging layer — negative values represent country corrections to previously over-reported numbers
- **case_fatality_rate > 100%:** Capped at 100% using `LEAST()` function in dbt — extreme outliers occur in small regions with imported deaths
- **Null hospital metrics:** Coalesced to 0 — many countries did not report hospital-level data

---

## 🔭 Observability

### Elementary Monitoring

Elementary runs alongside dbt and automatically collects:

- **dbt invocation history** — every run logged to `elementary.dbt_invocations`
- **Model run results** — execution time, row counts per model
- **Test results history** — pass/fail trends over time
- **Schema change detection** — alerts on unexpected column changes
- **Data freshness tracking** — monitors when tables were last updated

**Elementary datasets in BigQuery:**
```
healthcare_staging_elementary/
├── dbt_models
├── dbt_tests
├── dbt_invocations
├── dbt_run_results
├── elementary_test_results
├── model_run_results
└── ... (32 monitoring tables total)
```

**Generate report locally:**
```bash
cd healthcare_dbt
edr report --profiles-dir ~/.dbt
```

---

## ⚙️ Orchestration

### Cloud Composer DAG: `healthcare_data_pipeline`

The DAG orchestrates the full pipeline daily at midnight UTC.

```
extract_covid_data          # PySpark: BigQuery → GCS Parquet
        ↓
load_to_bigquery            # PySpark: GCS Parquet → BigQuery staging
        ↓
validate_raw_data           # BigQuery check: data freshness
        ↓
dbt_run                     # dbt: run all models
        ↓
dbt_test                    # dbt: run all tests
        ↓
gx_validation               # Great Expectations: run validation suite
        ↓
elementary_report           # Elementary: generate observability report
```

**DAG Configuration:**
- Schedule: `0 0 * * *` (daily at midnight UTC)
- Retries: 2 attempts with 5-minute delay
- Email alerts on failure
- Catchup: disabled

> **Note:** DAG is written for Cloud Composer 2.x with Airflow 2.x operators. Deployment requires a Composer environment with dbt, Elementary, and GCP dependencies installed.

---

## 🔁 CI/CD

### GitHub Actions Workflow: `dbt CI Pipeline`

Triggers on every push to `main` or `develop` and on pull requests to `main`.

```yaml
Steps:
  1. Checkout code
  2. Set up Python 3.11
  3. Install dbt-bigquery
  4. Create GCP credentials from secret
  5. Generate dbt profiles dynamically
  6. Install dbt packages (dbt deps)
  7. Run dbt models (dbt run)
  8. Run dbt tests (dbt test)
  9. Notify on success/failure
```

**Required GitHub Secrets:**
| Secret | Description |
|--------|-------------|
| `GCP_SERVICE_ACCOUNT_KEY` | Full JSON content of GCP service account key |

---

## 📈 Dashboards

Three Looker Studio dashboards connect directly to BigQuery mart tables.

### Dashboard 1 — Pandemic Impact
- World map of cumulative cases by country
- Time series of daily new cases and deaths
- Case fatality rate bar chart by country
- Hospital pressure scorecards (ICU, ventilator, hospitalized)
- Country and date range filters

### Dashboard 2 — Vaccination Effectiveness
- Vaccination coverage % scorecard
- Doses by brand pie chart (Pfizer / Moderna / Janssen)
- Time series: vaccinations vs deaths trend
- Cumulative vaccination progress by country

### Dashboard 3 — Government Response
- Stringency index vs new cases time series
- Policy measures scorecards (school closing, travel controls, stay-at-home)
- HDI vs case fatality rate scatter chart
- GDP per capita vs vaccination coverage

---

## 🚀 Setup & Installation

### Prerequisites
- Python 3.11+
- Google Cloud SDK (`gcloud`)
- GCP project with BigQuery, GCS, and Dataproc APIs enabled
- Service account with required permissions

### Required GCP Permissions
| Role | Purpose |
|------|---------|
| `roles/bigquery.dataViewer` | Read BigQuery datasets |
| `roles/bigquery.jobUser` | Run BigQuery queries |
| `roles/storage.objectAdmin` | Read/write GCS bucket |
| `roles/dataproc.worker` | Submit Dataproc Serverless jobs |

### Installation

**1. Clone the repository**
```bash
git clone https://github.com/yourusername/healthcare-data-quality-observability-platform.git
cd healthcare-data-quality-observability-platform
```

**2. Create and activate virtual environment**
```bash
python -m venv virtual-env
source virtual-env/bin/activate  # Linux/Mac
virtual-env\Scripts\activate     # Windows
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Configure environment variables**
```bash
cp .env.example .env
# Edit .env with your GCP project details
```

**5. Set up dbt profiles**
```bash
# Add healthcare_dbt profile to ~/.dbt/profiles.yml
# See Environment Variables section for required values
```

**6. Install dbt packages**
```bash
cd healthcare_dbt
dbt deps
```

**7. Run the pipeline**
```bash
# Extract and load data
python src/ingestion/extract_covid_data.py
python src/ingestion/load_to_bigquery.py

# Run dbt models and tests
dbt run
dbt test

# Run GX validations
python src/quality/run_validations.py

# Generate Elementary report
edr report --profiles-dir ~/.dbt
```

---

## 🔐 Environment Variables

Create a `.env` file in the project root:

```env
GCP_PROJECT_ID=your-gcp-project-id
GCS_BUCKET_NAME=your-gcs-bucket-name
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
BQ_DATASET=healthcare_staging
BQ_TABLE=covid19_open_data
BQ_PUBLIC_PROJECT=bigquery-public-data
```

Add the following to `~/.dbt/profiles.yml`:

```yaml
healthcare_dbt:
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-gcp-project-id
      dataset: healthcare_staging
      keyfile: /path/to/your/service-account-key.json
      location: US
      threads: 4
  target: dev

elementary:
  outputs:
    default:
      type: bigquery
      method: service-account
      project: your-gcp-project-id
      dataset: healthcare_staging_elementary
      keyfile: /path/to/your/service-account-key.json
      location: US
      threads: 1
  target: default
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Make your changes
4. Run dbt tests (`dbt test`)
5. Run GX validations (`python src/quality/run_validations.py`)
6. Commit your changes (`git commit -m 'feat: add your feature'`)
7. Push to the branch (`git push origin feature/your-feature`)
8. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License.

---
