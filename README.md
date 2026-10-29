# FunnelVision --- Modern Data Stack Data Engineering

An end-to-end e-commerce data engineering project that transforms raw
marketing and transactional data into analytics-ready models using
Apache Airflow, Snowflake, dbt, Docker, and Streamlit.

**Author:** Mohammad AlMukadam\
**Repository:** [FunnelVision on
GitHub](https://github.com/MohammadAhmed-306/FunnelVision-Modern-Data-Stack-Data-Engineering-Ecommerce-Analytics)

------------------------------------------------------------------------

## Project Overview

FunnelVision demonstrates how raw e-commerce data can be ingested,
transformed, tested, and presented through a local analytics dashboard.

### Business Objectives

-   Analyze website sessions and pageview activity.
-   Measure session-to-order conversion.
-   Evaluate marketing channel performance.
-   Track order volume and revenue.
-   Analyze product performance and refunds.
-   Build reusable, tested analytical data models.

## Architecture

![FunnelVision Architecture](./FunnelVision%20Modern%20Data%20Stack%20Architecture.png)

### Pipeline Flow

1.  **Data ingestion:** Apache Airflow orchestrates source-data
    ingestion.
2.  **Data warehousing:** Snowflake stores the ingested data.
3.  **Staging:** dbt prepares source tables for downstream
    transformations.
4.  **Intermediate transformations:** dbt combines and prepares data for
    analytical use cases.
5.  **Data marts:** dbt produces models for sessions, orders, products,
    refunds, dates, and channels.
6.  **Data quality:** dbt tests validate important business rules and
    calculations.
7.  **Visualization:** Streamlit presents analytics from transformed
    data models.
8.  **Containerization:** Docker Compose runs the local services.

## Technology Stack

  Technology       Purpose
  ---------------- --------------------------------------------
  Python           Pipeline and application development
  SQL              Data transformation and analytical queries
  Apache Airflow   Workflow orchestration
  Snowflake        Cloud data warehouse
  dbt              Data transformation, modeling, and testing
  Docker           Containerization
  Docker Compose   Local multi-service orchestration
  Streamlit        Interactive analytics dashboard

## Data Source

**Dataset:** [Maven Analytics --- Toy Store E-Commerce
Database](https://mavenanalytics.io/data-playground/toy-store-e-commerce-database)

The project uses the Maven Fuzzy Factory e-commerce dataset to explore
website behavior, marketing acquisition, orders, and refunds.

  Source file                Description
  -------------------------- ---------------------------------------------
  `website_sessions.csv`     Website session and acquisition information
  `website_pageviews.csv`    Website pageview activity
  `orders.csv`               Order-level information
  `order_items.csv`          Individual items within orders
  `order_item_refunds.csv`   Refund records
  `products.csv`             Product information

Raw CSV files and the original ZIP archive are excluded from version
control to keep the repository lightweight. Download the dataset from
Maven Analytics and place the required CSV files in `data/raw/` before
running the pipeline.

## dbt Data Models

### Staging Models

-   `stg_orders`
-   `stg_order_items`
-   `stg_order_item_refunds`
-   `stg_products`
-   `stg_website_pageviews`
-   `stg_website_sessions`

### Intermediate Models

-   `int_order_items_with_refunds`
-   `int_session_to_order`

### Marts

  Model            Analytical purpose
  ---------------- ----------------------------
  `dim_channel`    Marketing channel analysis
  `dim_date`       Date-based analysis
  `dim_product`    Product analysis
  `fct_orders`     Order and revenue analysis
  `fct_refunds`    Refund analysis
  `fct_sessions`   Website session analysis

### Data Quality Tests

-   `assert_conversion_matches_order_count.sql`
-   `assert_item_net_revenue_calculation.sql`
-   `assert_order_profit_calculation.sql`
-   `assert_refund_amounts_valid.sql`

These custom SQL tests validate conversion calculations, item-level net
revenue, order profit calculations, and refund amounts.

## Dashboard

FunnelVision includes a Streamlit dashboard that runs locally through
Docker Compose. It presents analytics derived from the transformed
models, including website traffic, conversion, marketing channels,
orders, revenue, and refunds.

### Dashboard Screenshots

Create a `screenshots/` directory in the project root, add your actual
dashboard screenshots, and update the path below to match your filename:

``` markdown
![FunnelVision Dashboard](screenshots/funnelvision-dashboard.png)
```

## Project Structure

``` text
Modern Stack Project/
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── airflow/
│   └── dags/
│       └── funnelvision_dag.py
├── data/
│   └── raw/
│       └── Download source CSV files here
├── data_requirements/
├── dbt/
│   └── funnelvision/
│       ├── dbt_project.yml
│       ├── models/
│       │   ├── staging/
│       │   ├── intermediate/
│       │   ├── marts/
│       │   └── schema.yml
│       └── tests/
├── Snowflake_sql/
│   ├── 00_account_setup.sql
│   └── 01_setup.sql
└── streamlit/
    ├── app.py
    ├── Dockerfile
    └── requirements.txt
```

Generated files, local credentials, and runtime logs are intentionally
omitted from this overview.

## Getting Started

### Prerequisites

-   Git
-   Docker Desktop with the Docker engine running
-   Access to a Snowflake account
-   The source dataset downloaded from Maven Analytics

### 1. Clone the Repository

``` powershell
git clone https://github.com/MohammadAhmed-306/FunnelVision-Modern-Data-Stack-Data-Engineering-Ecommerce-Analytics.git
cd FunnelVision-Modern-Data-Stack-Data-Engineering-Ecommerce-Analytics
```

### 2. Configure Environment Variables

``` powershell
Copy-Item .env.example .env
```

Update `.env` with the values required by your project. Keep credentials
private and never commit the real `.env` file.

### 3. Download the Dataset

Download the [Toy Store E-Commerce
Database](https://mavenanalytics.io/data-playground/toy-store-e-commerce-database)
and place the required CSV files in `data/raw/`.

### 4. Start Docker Services

``` powershell
docker compose up -d --build
docker compose ps
```

### 5. Access the Local Applications

  Application   URL
  ------------- -------------------------
  Airflow       <http://localhost:8080>
  Streamlit     <http://localhost:8501>

Use Airflow to inspect the `funnelvision_daily_pipeline` DAG and its
runs. Use Streamlit to explore the dashboard. Snowflake configuration
and the exact ingestion/dbt execution sequence depend on the environment
variables and commands configured in the project.

## Project Status

The following components have been confirmed as working:

-   Apache Airflow
-   Data ingestion through Airflow
-   Snowflake
-   dbt transformations and tests
-   Streamlit dashboard running locally
-   Docker Compose-based environment

## Security and Version Control

The `.gitignore` file excludes local secrets, raw CSV datasets,
generated dbt artifacts, Python cache files, and Airflow logs.

Before publishing, verify that `.env` is not committed, Snowflake
credentials are absent from source files, raw datasets and large
archives remain excluded, and `.env.example` contains placeholders
rather than real credentials.

## Future Improvements

-   Add automated pipeline monitoring and failure notifications.
-   Expand data quality coverage and reconciliation checks.
-   Add dashboard screenshots and documentation for key business
    metrics.
-   Set up automated testing through a CI workflow.

## Author

**Mohammad AlMukadam**

[View the GitHub
Repository](https://github.com/MohammadAhmed-306/FunnelVision-Modern-Data-Stack-Data-Engineering-Ecommerce-Analytics)
