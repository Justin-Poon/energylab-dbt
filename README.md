# EnergyLab DBT Pipeline & Dashboard

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://lackadaisicalshiba-energylab-dbt-dashboard-h6afhv.streamlit.app/)

## Project Overview

This project demonstrates a simple, end-to-end data pipeline using dbt (Data Build Tool) to process simulated energy billing and payment data, culminating in an interactive Streamlit dashboard visualizing key metrics.

The primary goal is to showcase foundational data engineering skills relevant to the energy sector, specifically focusing on building and managing transformations for billing and financial data flows.

## Live Demo

A live version of the dashboard is hosted on Streamlit Community Cloud:

**[https://lackadaisicalshiba-energylab-dbt-dashboard-h6afhv.streamlit.app/](https://lackadaisicalshiba-energylab-dbt-dashboard-h6afhv.streamlit.app/)**


## Motivation

This project was developed as a practical demonstration of skills relevant to the **Kraken Analytics** role, particularly the focus on understanding and processing data related to **billing, payments, and debt** within the energy industry. While using sample data, the pipeline structure and transformations mirror the core logic required to build reliable datasets for financial analysis and reporting in a production environment.

It aims to showcase proficiency in:

*   Building data pipelines using **dbt**.
*   Writing clean, efficient **SQL** for data transformation and aggregation.
*   Applying data modeling concepts (staging, intermediate, fact layers).
*   Working with modern data tools (**DuckDB**, **Python**).
*   Basic data visualization and app creation (**Streamlit**).
*   Collaborative development using **Git**.

## Key Features & Demonstrated Skills

*   **dbt Project Structure:** Standard dbt project layout (`models`, `seeds`, `profiles.yml`, `dbt_project.yml`).
*   **Data Modeling:**
    *   **Staging (`models/staging/`):** Basic cleaning, renaming, and type casting from raw seed data.
    *   **Intermediate (`models/intermediate/`):** Joins invoice and payment data to calculate amounts paid per invoice.
    *   **Mart (`models/marts/`):** Creates final fact tables for invoices (`fct_invoices`) and aggregated monthly metrics (`fct_billing_metrics`), calculating insights like outstanding amounts, days-to-pay, and collection rates.
*   **SQL Transformations:** Demonstrates common patterns like CTEs, joins, aggregations (`SUM`, `COUNT`, `AVG`), window functions (`ROW_NUMBER`), date functions (`strftime`, `date_diff`), and `COALESCE`.
*   **Local Data Warehouse:** Uses **DuckDB** for easy local setup and execution.
*   **Streamlit Dashboard (`dashboard.py`):** Provides an interactive front-end to visualize the processed data (KPIs, monthly trends, invoice details). Leverages Streamlit's caching for efficient connection management.
*   **Version Control:** Managed using **Git**.

## Technologies Used

*   **dbt-core & dbt-duckdb:** For defining and running the data transformation pipeline.
*   **DuckDB:** As the embedded analytical data warehouse.
*   **Python:** For the Streamlit application logic.
*   **Streamlit:** For building the interactive dashboard.
*   **Pandas:** Used within Streamlit for data handling.
*   **Git:** For version control.

## Data Source

The pipeline uses two sample CSV files located in the `/seeds` directory:

*   `sample_billing_invoices.csv`: Simulates raw billing invoice records.
*   `sample_payments.csv`: Simulates payment transaction records.

*Note: This is simulated data for demonstration purposes only.*

## Local Setup & Development

1.  **Clone the Repository:**
    ```bash
    git clone <your-repo-url>
    cd energylab-dbt
    ```
2.  **Create & Activate Virtual Environment (Recommended):**
    ```bash
    python -m venv .venv
    # Windows
    .\.venv\Scripts\activate
    # macOS/Linux
    source .venv/bin/activate
    ```
3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *(Ensure `requirements.txt` includes `dbt-duckdb`, `streamlit`, `pandas`)*

## Running Locally

The project includes a pre-built DuckDB database file (`energylab.duckdb`) for immediate use with the dashboard.

1.  **Run the Streamlit Dashboard:**
    ```bash
    streamlit run dashboard.py
    ```
    The dashboard will connect to the included `energylab.duckdb` file.

2.  **(Optional) Rebuild the Database:**
    If you modify the dbt models or seeds and want to regenerate the database file:
    ```bash
    dbt build
    ```
    This command will rebuild `energylab.duckdb` in the project root. Remember to commit the updated file if you want it reflected in the repository and the live demo.

## Project Structure

```
├── dashboard.py          # Streamlit application script
├── dbt_project.yml       # dbt project configuration
├── energylab.duckdb      # Pre-built DuckDB database file
├── models                # dbt models (staging, intermediate, marts)
│   ├── intermediate
│   ├── marts
│   └── staging
├── profiles.yml          # dbt connection profile (uses DuckDB)
├── README.md             # This file
├── requirements.txt      # Python dependencies
├── seeds                 # CSV seed files
│   ├── sample_billing_invoices.csv
│   └── sample_payments.csv
└── target/               # dbt artifacts (compiled SQL, logs - auto-generated, gitignored)
```

---
