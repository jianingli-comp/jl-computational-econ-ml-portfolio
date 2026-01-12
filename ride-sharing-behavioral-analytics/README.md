## Ride-Sharing Behavioral Analytics (Synthetic Data)

This project analyzes **user-generated ride-sharing posts** and demonstrates a complete pipeline for transforming **noisy, unstructured text into a clean, structured dataset** suitable for analysis.

The focus of the project is **data cleaning, rule-based text parsing, and exploratory data analysis (EDA)** under realistic noise conditions commonly observed in online community platforms (e.g., Facebook groups).

---

### Project Overview

Ride-sharing posts often follow informal conventions rather than strict schemas. Key information such as ride type, date, route, and price is embedded in free-form text with inconsistent formatting, missing fields, and ambiguities.

This project addresses the following challenges:
- Extracting structured variables from heterogeneous user-generated text
- Handling missing, duplicated, and inconsistently formatted information
- Designing robust, interpretable parsing rules rather than relying on black-box models
- Quantifying data cleaning coverage and limitations

The analysis is conducted on **synthetic data** designed to closely mimic real-world ride-sharing group posts while avoiding privacy or scraping concerns.

---

### Project Structure

``` text
ride-sharing-behavioral-analytics/
├─ code/
│  ├─ 00_synthetic_posts_generator.ipynb
│  └─ 01_text_to_dataset_and_eda.ipynb
│
├─ data/
│  ├─ sample_posts_500.csv
│  ├─ sample_posts_2000.csv
│  ├─ sample_posts_5000.csv
│  └─ processed_dataset_2000_report_aligned.csv
│
├─ results/
│  ├─ figures/
│  │  ├─ top_destinations.png
│  │  ├─ top_routes.png
│  │  ├─ weekday_of_ride.png
│  │  └─ driver_price_hist.png
│  └─ tables/
│     ├─ coverage_summary.csv
│     ├─ top_destinations.csv
│     └─ top_routes.csv
│
├─ README.md
└─ .gitignore
```

---

### Data Generation (`00_synthetic_posts_generator.ipynb`)

The notebook `00_synthetic_posts_generator.ipynb` generates synthetic ride-sharing posts with **intentionally injected noise**, including:

- Multiple date hashtags in a single post
- Missing or duplicated hashtags
- Multiple route expression formats (`A->B`, `A to B`, `A-B`, `from A to B`)
- Price formats such as `$20`, `$20-30`, `20 dollars`, or missing prices
- Informal language, emojis, spacing inconsistencies, and light typos

Three datasets are generated (500, 2000, and 5000 posts) to verify that observed patterns are stable across sample sizes.

---

### Parsing and Cleaning (`01_text_to_dataset_and_eda.ipynb`)

The notebook `01_text_to_dataset_and_eda.ipynb` implements a **report-aligned, rule-based parsing, pipeline,** consisting of:

#### Parsed Fields
- **Ride type**: `ride/drive` extracted from hashtags
- **Date of ride and weekday**: inferred from the first valid hashtag
- **Price**: extracted primarily from driver posts with support for ranges and text-based prices
- **Route**: (location -> destination), parsed using multiple regex patterns and domain-aware post-processing

#### Design Choices
- Only the **first hashtag** is used when multiple date tags appear
- Rule-based parsing is preferred for transparency and interpretability
- Lightweight post-hoc repairs are applied to fix systematic truncation artifacts (e.g., recovering `San Jose` from partial matches)

#### Output
``` text
data/processed_dataset_2000_report_aligned.csv
```
This dataset represents the primary analysis-ready output of the project.

---

### Exploratory Data Analysis

The following results are generated and saved in `results/`:
- Distribution of top destinations
- Distribution of top routes (location -> destination)
- Weekday of ride inffered from hash tag dates
- Price distribution for driver posts

Cleaning coverage metrics are summarized in `results/tables/coverage_summary.csv`.

The main analysis is conducted on the **2,000-post dataset**, which is sufficient to capture structural variability in route expressions and pricing behavior. Smaller (500) and larger (5,000) datasets were generated for validation purposes but are not analyzed in detail.

---

### Key Results

- Over 99% destination coverage achieved using rule-based route parsing
- High robustness to heterogeneous route expressions and noisy text
- Clear separation between raw text, cleaned data, and analytical outputs

These results highlight the effectiveness of interpretable parsing strategies for messy real-world text data.

---

### Limitations

- Rule-based parsing may fail on highly unconventional or adversarial text
- Semantic ambiguity (e.g., multiple routes mentioned in one post) is resolved using simple heuristics
- The synthetic data, while realistic, does not capture all linguistic variation found in real communities

---

### Reproducibility

To reproduce the analysis:
1. Run `00_synthetic_posts_generator.ipynb` to generate raw synthetic posts
2. Run `01_text_to_dataset.ipynb` to perform parsing and analysis

All outputs are deterministic given the fixed random seed.

---

### Summary

This project demonstrates a complete, transparent pipeline for extracting structured behavioral data from noisy user-generated text, with an emphasis on data cleaning, interpretability, and robustness rather than black-box modeling.

---

### Original Project Report

This project is adapted from a course project.  
The original report is included for reference:

- `reports/original_project_report.pdf`

The current repository focuses on a cleaned, reproducible version of the pipeline using synthetic data.

---



