# Data Engineering Take-Home Assessment

## 📘 Start Here

The **primary deliverable** for this assessment is the Jupyter notebook:

**`notebook/case_study_solution.ipynb`**

The notebook contains the complete solution, including:

- Data loading
- Data quality review
- Data standardisation
- Data quality validation
- Business transformations
- Analysis questions
- Validation of transformation results
- Documentation of assumptions and design decisions

The notebook is fully documented and is intended to be read from start to finish.

A standalone Python implementation of the same pipeline is also included in **`case_study.py`**.

---

# Project Overview

This project implements an end-to-end data engineering pipeline using **PySpark**.

The objective was to ingest three raw datasets, assess and validate their quality, apply the required business transformations, and produce analytical datasets that answer the business questions provided in the assessment.

Rather than applying transformations immediately, the pipeline first profiles and validates the raw data so that any data quality issues can be identified and addressed before processing.

![Pipeline Workflow](images/workflow.svg)

The workflow follows a layered approach:

```
Raw Data
    │
    ▼
Data Review
    │
    ▼
Store Layer
(Standardisation & Data Type Assignment)
    │
    ▼
Data Quality Validation
    │
    ▼
Publish Layer
(Business Transformations)
    │
    ▼
Analysis
```

## Highlights

- End-to-end ETL pipeline built with PySpark
- Structured using Raw → Store → Publish layers
- Comprehensive data quality assessment and validation
- Business rule implementation with documented assumptions
- Analytical reporting using transformed datasets
- Fully documented notebook with supporting validation

---

# Repository Structure

```text
.
├── README.md
├── notebook/
│   └── data_engineering_take_home_assessment.ipynb
├── take_home_assessment.py
├── data/
│   ├── products.csv
│   ├── sales_order_header.csv
│   └── sales_order_detail.csv
└── .gitignore
```

| File | Description |
|------|-------------|
| **case_study_solution.ipynb** | Primary deliverable containing the complete assessment, documentation, validation and analysis. |
| **case_Study.py** | Standalone Python implementation of the same pipeline. |
| **data/** | Source datasets used throughout the assessment. |
| **README.md** | Project overview and implementation summary. |

---


