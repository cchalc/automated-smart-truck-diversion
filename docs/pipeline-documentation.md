# ShovelSense Data Pipeline Documentation

## Overview

This document describes the 25 tables in the ShovelSense data pipeline, their relationships, and how they connect to the dialectical analysis and academic literature that informed the data model design.

The pipeline implements a **medallion architecture** (Bronze → Silver → Gold) following Kimball star schema principles, designed to answer the central question from the dialectical analysis:

> "Can XRF sensors mounted on a shovel bucket reliably measure copper grade in real-time — and will that capability translate to a heterogeneous copper porphyry deposit?"

---

## Table Summary

| Layer | Table Count | Purpose |
|-------|-------------|---------|
| Bronze | 6 | Raw ingestion with lineage metadata |
| Silver | 6 | Cleaned, validated, enriched data |
| Gold - Dimensions | 4 | Conformed dimensions for star schema |
| Gold - Facts | 4 | Transactional fact tables |
| Gold - Aggregates | 5 | Materialized views for analysis |
| **Total** | **25** | |

---

## Data Model Diagram

```
                              ┌─────────────────┐
                              │    dim_date     │
                              │  (date_key PK)  │
                              └────────┬────────┘
                                       │
    ┌─────────────┐    ┌───────────────┼───────────────┐    ┌─────────────┐
    │ dim_shovels │    │               │               │    │  dim_trucks │
    │(shovel_id)  │◄───┤               │               ├───►│ (truck_id)  │
    └──────┬──────┘    │               │               │    └──────┬──────┘
           │           │               │               │           │
           │    ┌──────▼───────────────▼───────────────▼──────┐    │
           │    │         fact_bucket_measurements            │    │
           ├───►│  (measurement_id, shovel_id, truck_id,      │◄───┤
           │    │   block_id, measurement_date_key)           │    │
           │    └──────────────────────┬──────────────────────┘    │
           │                           │                           │
           │    ┌──────────────────────▼──────────────────────┐    │
           │    │            fact_truck_loads                  │    │
           ├───►│  (load_id, shovel_id, truck_id, block_id,   │◄───┤
           │    │   load_date_key)                             │    │
           │    └──────────────────────┬──────────────────────┘    │
           │                           │                           │
           │                           ▼                           │
           │    ┌─────────────────────────────────────────────┐    │
           └───►│         fact_daily_diversions               │◄───┘
                │  (load_date, shovel_id)                     │
                └─────────────────────────────────────────────┘
                                       │
                              ┌────────▼────────┐
                              │ dim_block_model │
                              │   (block_id)    │
                              └─────────────────┘
```

---

## Bronze Layer: Raw Ingestion

Bronze tables ingest raw parquet files with minimal transformation, adding lineage columns (`_ingested_at`, `_source_file`) for auditability.

### bronze_block_model
**Purpose:** Raw geological block model data

| Column | Type | Description |
|--------|------|-------------|
| block_id | STRING | Unique block identifier |
| bench | INT | Mining bench level |
| easting, northing, elevation | DOUBLE | Spatial coordinates |
| planned_cu_grade | DOUBLE | Planned copper grade (%) from blast hole sampling |
| planned_fe_grade | DOUBLE | Planned iron grade (%) |
| geological_domain | STRING | Zonation: BORNITE_CORE, CHALCOPYRITE_ZONE, etc. |
| chalcopyrite_pct | DOUBLE | Chalcopyrite mineralogy percentage |
| bornite_pct | DOUBLE | Bornite mineralogy percentage |
| surface_volume_correlation | DOUBLE | Estimated surface-to-volume grade correlation |

**Dialectic Connection:** The `geological_domain` field implements the zonation pattern from Round 1: "Bornite-rich core → Chalcopyrite zone → Pyrite halo". The `surface_volume_correlation` field captures the "critical unknown" identified in Round 1.

**Paper Connection:** Geological zonation based on USGS Porphyry Copper Deposit Model (SIR 2010-5070-B).

---

### bronze_shovels
**Purpose:** XRF-equipped shovel fleet master data

| Column | Type | Description |
|--------|------|-------------|
| shovel_id | STRING | Unique shovel identifier |
| shovel_type | STRING | Equipment model |
| bucket_capacity_m3 | INT | Bucket volume |
| minesense_equipped | BOOLEAN | Has ShovelSense XRF sensors |
| sensor_heads | INT | Number of XRF sensor heads |
| xrf_detector_type | STRING | SDD detector type |
| detection_limit_ppm | INT | XRF detection limit |
| penetration_depth_mm | DOUBLE | XRF penetration depth |

**Dialectic Connection:** `penetration_depth_mm` reflects the Round 1 finding: "XRF penetration depth: Tens to hundreds of micrometers — essentially surface-only."

**Paper Connection:** XRF physics from SAGE Journals - XRF Surface Analysis for Copper Ore Classification (2025).

---

### bronze_trucks
**Purpose:** Haul truck fleet master data

| Column | Type | Description |
|--------|------|-------------|
| truck_id | STRING | Unique truck identifier |
| truck_model | STRING | Equipment model |
| payload_capacity_tonnes | INT | Maximum payload |
| fms_system | STRING | Fleet Management System |

**Paper Connection:** Fleet management integration approach from IIoT Smart Manufacturing paper.

---

### bronze_bucket_measurements
**Purpose:** Raw XRF sensor readings per bucket

| Column | Type | Description |
|--------|------|-------------|
| measurement_id | STRING | Unique measurement identifier |
| timestamp | STRING | Measurement timestamp |
| shovel_id, truck_id, block_id | STRING | Foreign keys |
| cu_grade_pct | DOUBLE | XRF-measured copper grade |
| fe_grade_pct | DOUBLE | XRF-measured iron grade |
| xrf_confidence | DOUBLE | Sensor confidence score (0-1) |
| matrix_effect_factor | DOUBLE | Iron-copper absorption correction |
| heterogeneity_error_est | DOUBLE | Estimated heterogeneity error |

**Dialectic Connection:**
- `matrix_effect_factor` implements Round 1: "Iron absorbs copper X-rays, reducing measured Cu intensity"
- `heterogeneity_error_est` captures: "Heterogeneity error from surface-only measurement is the most significant error source"

**Paper Connection:** Matrix effects from Es-sahly 2025 study; heterogeneity error from MDPI Cadia 2023.

---

### bronze_truck_loads
**Purpose:** Aggregated truck load data with classification decisions

| Column | Type | Description |
|--------|------|-------------|
| load_id | STRING | Unique load identifier |
| timestamp | STRING | Load timestamp |
| truck_id, shovel_id, block_id | STRING | Foreign keys |
| avg_cu_grade_pct | DOUBLE | Average XRF grade for load |
| planned_classification | STRING | Blast hole-based: ORE or WASTE |
| shovelsense_classification | STRING | XRF-based: ORE or WASTE |
| diversion_type | STRING | ALIGNED, ORE_FROM_WASTE, WASTE_FROM_ORE |
| destination | STRING | MILL or WASTE_DUMP |

**Dialectic Connection:** The classification comparison enables confusion matrix analysis per Round 1 Critical Assessment: "No confusion matrix — no precision/recall/F1 data for grade classification."

**Paper Connection:** Classification metrics from OpenMines and Deep RL Fleet papers.

---

### bronze_shift_summaries
**Purpose:** Shift-level operational aggregates

| Column | Type | Description |
|--------|------|-------------|
| date | STRING | Shift date |
| shift | STRING | DAY or NIGHT |
| shovel_id | STRING | Foreign key |
| n_trucks | INT | Trucks loaded |
| diversion_rate | DOUBLE | Proportion of diverted loads |
| f1_factor_estimate | DOUBLE | Estimated F1 score |

**Dialectic Connection:** `f1_factor_estimate` addresses Round 2 Direction B: "If your F1 factor is already above 0.92, the ROI case for any sensor collapses."

---

## Silver Layer: Cleaned and Validated

Silver tables apply data quality rules, parse timestamps, and add derived fields using domain-specific business logic.

### silver_block_model
**Transformations:**
- Add `is_ore` flag based on cutoff grade (0.32% Cu)
- Add `grade_bin` categorization (HIGH, MEDIUM, LOW, WASTE)
- Add `mineralogy_class` (BORNITE_DOMINANT, CHALCOPYRITE_DOMINANT, MIXED)
- Add `sv_correlation_quality` indicator

**Expectations:**
- `block_id IS NOT NULL`
- `planned_cu_grade >= 0 AND planned_cu_grade <= 5`

---

### silver_bucket_measurements
**Transformations:**
- Parse timestamps to DATE and HOUR components
- Add `measurement_date_key` for dimension joins
- Add `matrix_effect_severity` (HIGH, MODERATE, LOW)
- Add `measurement_quality_score` composite metric
- Add `is_high_confidence` flag (xrf_confidence >= 0.90)

**Expectations:**
- `measurement_id IS NOT NULL`
- `cu_grade_pct >= 0`
- `xrf_confidence >= 0.5`

**Dialectic Connection:** Quality scoring implements Round 1: "Site-specific calibration against reconciled mill feed" — providing the data structure to evaluate calibration effectiveness.

---

### silver_truck_loads
**Transformations:**
- Parse timestamps, add shift classification
- Add `is_diverted`, `is_ore_recovery`, `is_dilution_prevention` flags
- Add `estimated_cu_value_usd` economic calculation
- Add `xrf_reliability` composite indicator

**Economic Formula (from Round 3 Direction C):**
```
estimated_cu_value_usd = payload_tonnes × (avg_cu_grade_pct / 100) × 0.85 × $8,820
```

Where:
- 0.85 = Metallurgical recovery (85%)
- $8,820/tonne = Copper price ($4/lb)

---

## Gold Layer: Dimension Tables

### dim_shovels
**Purpose:** Slowly changing dimension for shovel equipment

| Column | Description |
|--------|-------------|
| shovel_id | Primary key |
| shovel_type | Equipment classification |
| bucket_capacity_m3 | Operational capacity |
| minesense_equipped | XRF sensor presence |
| days_in_service | Equipment age |

---

### dim_trucks
**Purpose:** Slowly changing dimension for haul trucks

| Column | Description |
|--------|-------------|
| truck_id | Primary key |
| truck_model | Equipment model |
| payload_capacity_tonnes | Rated capacity |
| fms_system | Integration system |

---

### dim_block_model
**Purpose:** Dimension containing geological and mineralogical attributes

| Column | Description | Dialectic Source |
|--------|-------------|------------------|
| block_id | Primary key | — |
| geological_domain | BORNITE_CORE, CHALCOPYRITE_ZONE, etc. | Round 1: Deposit zonation |
| chalcopyrite_pct | Mineralogy composition | Round 1: 80% chalcopyrite |
| bornite_pct | Mineralogy composition | Round 1: 20% bornite |
| surface_volume_correlation | XRF accuracy predictor | Round 1: "The critical unknown" |
| sv_correlation_quality | HIGH/MODERATE/LOW | Round 1 Determinate Negation |
| nugget_effect_variance | Short-scale variability | USGS Porphyry Model |
| vein_density_class | Grade control difficulty | AusIMM Sampling 2008 |

**Paper Connection:** Mineralogy from USGS SIR 2010-5070-B; nugget effect from Geostatistics Lessons.

---

### dim_date
**Purpose:** Standard date dimension for time-series analysis

| Column | Description |
|--------|-------------|
| date_key | Primary key (YYYYMMDD) |
| date | Calendar date |
| year, quarter, month, week_of_year | Time hierarchy |
| day_of_week, day_name | Day classification |
| is_weekend, is_weekday | Operational flags |

---

## Gold Layer: Fact Tables

### fact_bucket_measurements
**Purpose:** Grain: One row per XRF measurement

| Measure | Description | Dialectic Connection |
|---------|-------------|---------------------|
| cu_grade_pct | XRF-measured copper | Core measurement |
| xrf_confidence | Sensor reliability | Round 1: Calibration quality |
| matrix_effect_factor | Fe-Cu absorption | Round 1: Chalcopyrite matrix effects |
| matrix_effect_severity | Categorical severity | — |
| measurement_quality_score | Composite metric | — |

---

### fact_truck_loads
**Purpose:** Grain: One row per truck load

| Measure | Description | Dialectic Connection |
|---------|-------------|---------------------|
| avg_cu_grade_pct | Load-averaged grade | — |
| planned_classification | Blast hole decision | Round 1: Current grade control |
| shovelsense_classification | XRF decision | — |
| diversion_type | ALIGNED, ORE_FROM_WASTE, WASTE_FROM_ORE | Round 1: 11% diversion rate |
| surface_volume_correlation | XRF accuracy predictor | Round 1: "The critical unknown" |
| estimated_cu_value_usd | Economic impact | Round 3 Direction C |
| xrf_reliability | Composite quality | — |

---

## Gold Layer: Aggregate Fact Tables (Materialized Views)

### fact_daily_diversions
**Purpose:** Daily diversion statistics by shovel

| Metric | Formula | Target (White Paper) |
|--------|---------|---------------------|
| diversion_rate | diverted_trucks / total_trucks | ~11% |
| ore_recovery_rate | ore_from_waste / total_trucks | ~6.4% |
| dilution_prevention_rate | waste_from_ore / total_trucks | ~4.7% |

**Dialectic Connection:** These metrics directly measure the claims from the ShovelSense white paper, enabling validation of the "11% diversion rate" claim.

---

### fact_classification_accuracy
**Purpose:** Daily confusion matrix metrics

| Metric | Formula | Critical Assessment Gap Addressed |
|--------|---------|----------------------------------|
| true_positive | planned=ORE, xrf=ORE | |
| true_negative | planned=WASTE, xrf=WASTE | |
| false_positive | planned=WASTE, xrf=ORE | Ore from Waste |
| false_negative | planned=ORE, xrf=WASTE | Waste from Ore |
| accuracy | (TP + TN) / Total | — |
| precision_ore | TP / (TP + FP) | "No confusion matrix" gap |
| recall_ore | TP / (TP + FN) | "No confusion matrix" gap |
| f1_score | 2 × P × R / (P + R) | Round 2: F1 factor |

**Dialectic Connection:** Directly addresses Critical Assessment finding: "No confusion matrix — no precision/recall/F1 data for grade classification."

---

### fact_domain_classification_accuracy
**Purpose:** Classification accuracy stratified by geological domain

This table tests the hypothesis from Round 2 Direction D:

> "The 80/20 chalcopyrite/bornite split suggests XRF accuracy may vary spatially. A pilot could test this."

| Metric | Description |
|--------|-------------|
| geological_domain | Zone identifier |
| accuracy | Classification accuracy within zone |
| f1_score | F1 within zone |
| avg_surface_volume_corr | Zone-specific S-V correlation |
| avg_xrf_confidence | Zone-specific XRF reliability |
| total_cu_value_usd | Economic impact within zone |

**Key Question Answered:** Does XRF accuracy vary by geological domain?

---

### fact_sv_correlation_analysis
**Purpose:** Test the Round 1 "critical unknown"

This table directly tests the central hypothesis from Round 1:

> "The relationship between multi-surface XRF readings and true volumetric grade of a heterogeneous bucket load is NOT well-characterized in published literature."

| sv_corr_bin | Description |
|-------------|-------------|
| 0.70-1.00 | High correlation — XRF should predict volume well |
| 0.55-0.70 | Moderate correlation |
| 0.40-0.55 | Low correlation |
| 0.00-0.40 | Poor correlation — XRF may not predict volume |

**Key Question Answered:** Does higher surface-volume correlation lead to better classification accuracy?

---

### fact_sensor_performance
**Purpose:** XRF sensor reliability tracking

| Metric | Description | Dialectic Connection |
|--------|-------------|---------------------|
| avg_confidence | Daily average XRF confidence | Round 1: Calibration drift |
| high_confidence_rate | % above 0.90 threshold | — |
| dual_sensor_rate | % with both sensors active | Redundancy check |
| sensor_1_failure_rate | Primary sensor failures | Equipment reliability |
| sensor_2_failure_rate | Secondary sensor failures | — |

---

### fact_grade_distribution
**Purpose:** Grade distribution by geological domain

| Metric | Description |
|--------|-------------|
| geological_domain | Zone identifier |
| grade_bin | HIGH, MEDIUM, LOW, WASTE |
| block_count | Number of blocks |
| avg_cu_grade | Mean copper grade |
| std_cu_grade | Grade variability |

**Paper Connection:** Grade variability analysis based on Queen's University sampling error research.

---

### summary_overall_performance
**Purpose:** Single-row summary with key performance indicators

| Metric | Description | Source |
|--------|-------------|--------|
| avg_daily_diversion_rate | Overall diversion rate | White paper: ~11% |
| avg_f1 | Overall F1 score | Round 2: Key baseline metric |
| avg_sv_correlation | Overall surface-volume correlation | Round 1: Critical unknown |
| total_cu_value_usd | Total copper value processed | Round 3: Economic model |

---

## Relationship to Academic Papers

| Paper | Key Concept | Tables Using Concept |
|-------|-------------|---------------------|
| **OpenMines (arXiv 2404.00622)** | Grade-based dispatch simulation | fact_truck_loads, fact_daily_diversions |
| **Deep RL Fleet (arXiv 2512.24251)** | Cost accounting for decisions | estimated_cu_value_usd field |
| **VRP Heuristics (arXiv 2303.04147)** | Optimization metrics | fact_classification_accuracy |
| **IIoT Manufacturing (arXiv 2312.16174)** | Sensor integration architecture | silver_bucket_measurements |
| **Rock Classification (arXiv 2510.13937)** | Classification accuracy | fact_domain_classification_accuracy |
| **Blockchain AI IIoT (arXiv 2405.12550)** | Data integrity for decisions | _ingested_at, _source_file lineage |
| **Digital Transformation (arXiv 2503.04749)** | Validation methodology | fact_sv_correlation_analysis |
| **Es-sahly 2025 (SAGE)** | Matrix effects, heterogeneity | matrix_effect_factor, heterogeneity_error_est |
| **USGS SIR 2010-5070-B** | Porphyry zonation | geological_domain field |

---

## Relationship to Dialectical Analysis

### Round 1: Surface-Volume Correlation

**Key Finding:** "The relationship between multi-surface XRF readings and true volumetric grade of a heterogeneous bucket load is NOT well-characterized."

**Data Model Response:**
- `surface_volume_correlation` field in block_model and truck_loads
- `fact_sv_correlation_analysis` table to test correlation-accuracy relationship
- `sv_correlation_quality` categorization (HIGH/MODERATE/LOW)

---

### Round 2: Baseline Assessment First

**Key Finding:** "You can't evaluate ANY technology without knowing your baseline."

**Data Model Response:**
- `fact_classification_accuracy` provides F1 score tracking
- `planned_classification` vs `shovelsense_classification` comparison
- Confusion matrix components (TP, TN, FP, FN) for before/after analysis

---

### Round 3: Economic Model

**Key Finding:** "ShovelSense costs 6-7x more than alternatives, requires 7x the improvement to break even."

**Data Model Response:**
- `estimated_cu_value_usd` on every truck load
- `total_cu_value_usd` aggregations by domain and time
- Economic parameters embedded in pipeline configuration:
  - Copper price: $8,820/tonne ($4/lb)
  - Metallurgical recovery: 85%
  - Cutoff grade: 0.32% Cu

---

### Round 3 Direction D: Implementation Failures

**Key Finding:** "Six failure modes kill ROI regardless of technology choice."

**Data Model Response:**
- `fact_sensor_performance` tracks sensor reliability
- XRF confidence tracking enables calibration drift detection
- Dual sensor rate monitors redundancy
- Time-series structure enables trend analysis for "set-and-forget" detection

---

## Usage Examples

### 1. Calculate Overall Diversion Rate
```sql
SELECT * FROM summary_overall_performance
```

### 2. Compare XRF Accuracy by Geological Domain
```sql
SELECT
    geological_domain,
    accuracy,
    f1_score,
    avg_surface_volume_corr,
    total_cu_value_usd
FROM fact_domain_classification_accuracy
ORDER BY f1_score DESC
```

### 3. Test Surface-Volume Correlation Hypothesis
```sql
SELECT
    sv_corr_bin,
    total_loads,
    accuracy_rate,
    diversion_rate
FROM fact_sv_correlation_analysis
ORDER BY sv_corr_bin
```

### 4. Daily Classification Performance
```sql
SELECT
    load_date,
    accuracy,
    precision_ore,
    recall_ore,
    f1_score
FROM fact_classification_accuracy
ORDER BY load_date
```

### 5. Sensor Reliability Trends
```sql
SELECT
    measurement_date,
    shovel_id,
    avg_confidence,
    high_confidence_rate,
    dual_sensor_rate
FROM fact_sensor_performance
ORDER BY measurement_date, shovel_id
```

---

## Conclusion

This data pipeline was designed to answer the questions that the dialectical analysis identified as critical for evaluating ShovelSense ROI:

1. **Does XRF surface measurement correlate with volumetric grade?** → `fact_sv_correlation_analysis`
2. **What is the baseline classification accuracy?** → `fact_classification_accuracy`
3. **Does accuracy vary by geological domain?** → `fact_domain_classification_accuracy`
4. **What is the economic impact of decisions?** → `estimated_cu_value_usd` throughout
5. **Are sensors reliable over time?** → `fact_sensor_performance`

The pipeline provides the measurement infrastructure to empirically validate whether XRF-based truck diversion adds value at a specific deposit — exactly what Round 2 concluded was missing from ShovelSense's validation approach.

---

## References

### Dialectical Analysis
- `dialectics/shovelsense-roi/round_1_context_briefing.md`
- `dialectics/shovelsense-roi/round_2_synthesis.md`
- `dialectics/shovelsense-roi/round_3_synthesis.md`

### Critical Assessment
- `docs/analysis/00-critical-assessment-summary.md`

### Academic Papers
- `docs/references/README.md` (full paper list)
- Individual analysis files in `docs/analysis/`
