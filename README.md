# SQL Data Warehouse

A SQL Server data warehouse integrating CRM and ERP source data through a medallion architecture consisting of Bronze, Silver, and Gold layers.

The project implements data ingestion, cleansing, standardisation, deduplication, validation, dimensional modelling, and quality checks to transform raw operational data into an analytics-ready star schema.

---

## Architecture

### Bronze — Raw Ingestion
- Loads CRM and ERP source data with minimal transformation
- Preserves the original source structure for traceability

### Silver — Cleansing and Transformation
- Standardises categorical values and formats
- Handles nulls and invalid records
- Deduplicates customer data
- Validates and repairs dates and sales values
- Creates derived attributes used downstream

### Gold — Analytical Model
- Integrates CRM and ERP datasets
- Creates customer and product dimensions
- Creates a sales fact table
- Produces an analytics-ready star schema for reporting and business intelligence

---

## Project Structure

    sql-data-warehouse-project/
    ├── datasets/           # Data availability notes; source files excluded
    ├── docs/               # Architecture and data-model documentation
    ├── scripts/
    │   ├── bronze/         # Raw data ingestion scripts
    │   ├── silver/         # Data transformation and cleansing scripts
    │   ├── gold/           # Dimensional model and analytics layer
    │   └── init_database.sql
    ├── tests/              # Data-quality checks
    └── README.md

---

## Data Pipeline

The warehouse follows a three-layer medallion architecture:

    CRM + ERP Source Data
            ↓
         Bronze
       Raw ingestion
            ↓
         Silver
    Cleaning, validation,
    standardisation and
    deduplication
            ↓
          Gold
    Dimensional modelling
    and analytics-ready views

---

## Key Features

### Data Ingestion
- Loads data from CRM and ERP source systems
- Maintains separate raw-source tables in the Bronze layer
- Uses stored procedures to manage batch loading

### Data Cleaning and Transformation
- Removes duplicate customer records
- Standardises categorical values
- Trims and cleans text fields
- Handles missing and invalid values
- Validates and converts date fields
- Recalculates inconsistent sales values
- Derives additional attributes for downstream analysis

### Dimensional Modelling
- Integrates CRM and ERP data sources
- Creates customer and product dimensions
- Creates a sales fact table
- Uses surrogate keys for dimensional modelling
- Produces an analytics-ready star schema

### Data Quality
- Includes dedicated quality-check scripts for the Silver and Gold layers
- Validates uniqueness, referential integrity, data consistency, and transformation outputs

---

## Technologies Used

- SQL Server
- T-SQL
- Docker
- VS Code
- Git & GitHub
- Draw.io

---

## Getting Started

1. Clone the repository
2. Set up SQL Server
3. Run `scripts/init_database.sql` to create the database and schemas
4. Run the Bronze-layer scripts to create and load the raw tables
5. Run the Silver-layer scripts to clean and transform the data
6. Run the Gold-layer script to create the analytical model
7. Run the quality-check scripts in `tests/` to validate the final warehouse

---

## Documentation

The `docs/` folder contains supporting documentation for the warehouse, including:

- Data architecture
- ETL workflow
- Data flow
- Data integration
- Dimensional model
- Data catalogue
- Naming conventions

---

## Data Availability

The original source datasets are intentionally not included in this public repository.

The source data contains CRM and ERP information covering customers, products, sales, demographics, locations, and product categories.

The project structure, SQL scripts, transformation logic, quality checks, and documentation are included so the warehouse design and implementation can still be reviewed independently of the source data.

---

## Purpose

This project demonstrates practical SQL and data-engineering skills, including:

- Data warehousing
- ETL development
- Data cleansing
- Data quality validation
- Dimensional modelling
- Star-schema design
- SQL-based analytics preparation
