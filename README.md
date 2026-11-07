# 🏨 Hotel Booking Analytics Platform-DataBricks

### End-to-End Data Engineering & Business Intelligence Using Databricks

A complete data engineering project that transforms raw hotel booking records into clean, validated, and business-ready datasets using **Databricks, SQL, Lakeflow Spark Declarative Pipelines, and Medallion Architecture**.

The solution covers data ingestion, transformation, quality validation, analytical modelling, and interactive dashboard development.

**Project:** Data Engineering & Analytics Internship — Innomatics Research Labs

---

## 🎯 Project Objectives

- Build a structured and repeatable data pipeline using Databricks.
- Implement Bronze, Silver, and Gold data layers.
- Clean and standardize inconsistent hotel booking records.
- Detect duplicate, missing, and invalid data.
- Generate analytical datasets for hotel business performance.
- Validate processing outcomes through reconciliation.
- Develop an interactive business intelligence dashboard.

## 🛠️ Technologies Used

| Technology | Application |
|---|---|
| **Databricks** | Development and execution environment |
| **SQL** | Data cleansing, transformation, and aggregations |
| **Unity Catalog** | Data organization and governance |
| **Delta Lake** | Reliable table storage |
| **Lakeflow Spark Declarative Pipelines** | Data pipeline orchestration |
| **Medallion Architecture** | Bronze, Silver, and Gold processing layers |
| **Materialized Views** | Business-ready analytical datasets |
| **Databricks AI/BI** | Interactive business intelligence dashboards |

## 🏗️ Data Pipeline Architecture

The project follows the Medallion Architecture to organize data processing into three layers.

```text
Hotel Booking CSV
       |
       v
Unity Catalog Volume
       |
       v
Bronze — Raw Data Ingestion
       |
       v
Silver — Cleaning, Deduplication & Validation
       |
       v
Gold — Business Analytics & Data Quality
       |
       v
Databricks AI/BI Dashboard
```

### 🥉 Bronze Layer — Data Ingestion

The Bronze layer preserves the original source information.

- Ingested CSV data from Unity Catalog Volumes.
- Created a Bronze streaming table.
- Maintained raw booking records for further processing and validation.
- Processed **1,316 source records**.

**Table:** `bronze_hotel_bookings`

### 🥈 Silver Layer — Data Cleaning & Transformation

The Silver layer improves the quality and consistency of booking records.

Key transformations include:

- Standardizing text fields using SQL functions such as `TRIM()` and `INITCAP()`.
- Handling missing values and incomplete booking information.
- Identifying duplicates using booking identifiers.
- Validating check-in and check-out dates.
- Detecting negative room rates.
- Separating invalid records with rejection reasons.
- Retaining cancelled bookings as valid business records.

**Silver Datasets:**

- `silver_bookings_deduplicated`
- `silver_bookings_clean`
- `silver_bookings_rejected`

**Processing Output:** 1,269 clean bookings, 21 rejected records, and 26 duplicates.

### 🥇 Gold Layer — Business Analytics

The Gold layer contains aggregated datasets designed for business reporting.

| Gold Dataset | Analytical Purpose |
|---|---|
| `gold_monthly_bookings` | Monthly booking trends, room nights, and revenue |
| `gold_hotel_summary` | Hotel, city, and regional performance |
| `gold_room_type_summary` | Room-type revenue and guest ratings |
| `gold_channel_summary` | Booking-channel performance and cancellations |
| `gold_data_quality` | Data-quality metrics and reconciliation |

These datasets serve as the primary data sources for the AI/BI dashboard.

---

## 📊 Business Insights & Key Metrics

The Gold layer supports the following validated business metrics:

| KPI | Value |
|---|---:|
| **Total Valid Bookings** | **1,269** |
| **Net Revenue** | **₹1.51 Cr (approx.)** |
| **Completed Room Nights** | **3,107** |
| **Cancelled Bookings** | **186** |

**Metric Definitions:**

- **Total Bookings:** Valid reservations, including cancellations.
- **Net Revenue:** Revenue from completed bookings after applicable discounts.
- **Room Nights:** Total completed room nights.
- **Cancellation Rate:** Cancelled bookings as a percentage of total bookings.
- **Average Daily Rate (ADR):** Net Revenue divided by completed room nights.

Cancelled bookings are already included in total bookings and are excluded from completed revenue and room-night calculations.

---

## 🔍 Data Quality & Reconciliation

Data validation was implemented to ensure consistency across processing stages.

### Record Reconciliation

| Record Category | Count |
|---|---:|
| Bronze Records | 1,316 |
| Clean Records | 1,269 |
| Rejected Records | 21 |
| Duplicate Records | 26 |

**Reconciliation Status: PASSED ✅**

```text
Bronze = Clean + Rejected + Duplicates

1,316 = 1,269 + 21 + 26
```

### Validation Issues Identified

| Data Quality Issue | Count |
|---|---:|
| Missing Nights | 27 |
| Missing Amount | 17 |
| Missing Booking Channel | 13 |
| Invalid Dates | 12 |
| Missing City | 11 |
| Negative Room Rates | 9 |

Issue counts represent detected validation conditions and may overlap across records.

---

## 📈 Databricks AI/BI Dashboard

Developed an interactive **three-page Hotel Booking Analytics Dashboard** using Gold-layer datasets.

### Page 1 — Executive Overview

Provides an overall view of booking activity and revenue performance.

**KPI Cards:**
- Total Bookings
- Net Revenue
- Room Nights
- Cancelled Bookings

**Visualizations:**
- Net Revenue by Stay Month
- Net Revenue by City

**Interactive Filters:** Stay Month, Region, and Room Type.

### Page 2 — Hotels, Rooms & Channels

Focuses on hotel performance, room categories, and reservation channels.

**Visualizations:**
- Net Revenue by Hotel
- Net Revenue by Room Type
- Average Guest Rating by Room Type
- Cancellation Rate (%) by Booking Channel
- Hotel Performance Table with revenue, bookings, room nights, and ADR

### Page 3 — Data Quality

Provides transparency into the data processing and validation results.

**Dashboard Components:**
- Bronze, Clean, Rejected, and Duplicate record KPI cards
- Reconciliation Status
- Record Reconciliation Check
- Data Quality Issues by Category

---

## ⚙️ Project Configuration

| Component | Name |
|---|---|
| Catalog | `hospitality_catalog` |
| Schema | `bookings` |
| Unity Catalog Volume | `raw_files` |
| Source Dataset | `hotel_bookings_raw.csv` |
| Pipeline | `hotel_booking_pipeline` |
| Pipeline SQL File | `hotel_booking_pipeline.sql` |

**Source Path:**

```text
/Volumes/hospitality_catalog/bookings/raw_files/hotel_bookings_raw.csv
```

### Execution Workflow

1. Upload the raw CSV file into the Unity Catalog Volume.
2. Configure the Lakeflow Spark Declarative Pipeline.
3. Execute the Bronze, Silver, and Gold transformations.
4. Validate record counts and data-quality results.
5. Query the Gold analytical datasets.
6. Use the Gold datasets to build the Databricks AI/BI dashboard.

---

## 📁 Project Deliverables

- Lakeflow Spark Declarative Pipeline source code
- Bronze, Silver, and Gold datasets
- SQL transformation and analytical logic
- Data-quality validation and reconciliation results
- Pipeline graph and architecture screenshots
- Three-page Databricks AI/BI Dashboard
- Dashboard PDF and project documentation

## 💡 Key Learnings

This project provided hands-on experience in:

- Developing SQL-based data engineering pipelines.
- Implementing Medallion Architecture in Databricks.
- Working with Unity Catalog, Volumes, and Delta Lake.
- Applying data cleansing and record validation techniques.
- Designing reusable Gold analytical datasets.
- Implementing data-quality reconciliation.
- Creating interactive dashboards for business analysis.

It reinforced the importance of **data quality, consistent business metrics, and reliable data processing** in building analytical solutions.

**⭐ Feedback and suggestions are welcome!**
