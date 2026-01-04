# springer-capital-data-engineer-test

## Overview
This project implements a data profiling and data processing pipeline for a user referral program.
The objective is to analyze referral data, apply business rules to detect potentially invalid referral rewards,
and generate a final validation report.

The solution includes:
- Data profiling for all source tables
- Data cleaning, joining, and timezone normalization
- Business logic validation to detect invalid referral rewards
- A Dockerized setup for reproducible execution
- Business-friendly documentation (Data Dictionary)

---

## Project Structure
springer_takehome/
│
├── src/
│ └── main.py # Main data pipeline script
│
├── data/ # Input CSV files (not committed if sensitive)
│
├── profiling/
│ └── data_profiling_summary.csv # Column-level profiling output
│
├── output/
│ └── referral_validation_report.csv # Final report (46 rows)
│
├── docs/
│ └── Data_Dictionary.xlsx # Business-friendly data dictionary
│
├── Dockerfile
├── requirements.txt
└── README.md


---

## Data Profiling
All source tables are profiled to understand data quality and structure.
For each column, the profiling includes:
- Data type
- Row count
- Null count
- Distinct value count

The profiling output is saved as: profiling/data_profiling_summary.csv


---

## Business Logic Validation
A new boolean column `is_business_logic_valid` is created based on referral business rules, including:
- Referral status validation
- Reward value validation
- Transaction status and timing checks
- Membership status checks
- Reward grant verification

The final output report contains **46 rows**, as required.

---

## How to Run the Project (Docker)

### Prerequisites
- Docker Desktop installed and running

### Step 1: Build the Docker image
```bash
docker build -t springer-referral .

### Step 2: Run the container
```
Windows PowerShell
docker run --rm `
  -v "${PWD}\output:/app/output" `
  -v "${PWD}\profiling:/app/profiling" `
  -v "${PWD}\docs:/app/docs" `
  springer-referral
Outputs Generated

After execution, the following files are created:

Data Profiling Summary

profiling/data_profiling_summary.csv


Final Referral Validation Report

output/referral_validation_report.csv


Data Dictionary

docs/Data_Dictionary.xlsx

Notes

All timestamps are converted from UTC to local time using the provided timezone columns.

String normalization (Initcap) is applied as required, except for club names.

No credentials are stored in code or configuration files.

Author

Akash Nair
