# Research Analysis Plan: Mining Operations Simulation & Visualization

**Date:** 2026-04-30
**Source of Truth:** `/dialectics/shovelsense-roi/` analysis (Rounds 1-3)
**Reference Papers:** 9 academic papers in `/docs/references/`

---

## Executive Summary

This plan identifies opportunities to recreate and extend concepts from academic research papers using Databricks functionality. The goal is to build interactive Plotly visualizations and simulations that demonstrate key concepts from the dialectical analysis while leveraging the existing synthetic data pipeline.

The dialectics analysis concluded:
1. **ShovelSense lacks independent validation** - No academic benchmarks for grade-based truck dispatch
2. **Surface-volume correlation is the critical unknown** - The key determinant of XRF sensor value
3. **Blast hole optimization delivers better ROI** - 6x better risk-adjusted return than ShovelSense
4. **MRT/PGNAA alternatives have validation** - Peer-reviewed evidence exists for competitors

---

## Feature Request Categories

### Category A: OpenMines-Inspired Dispatch Simulation
**Source:** `openmines-truck-dispatching.pdf` (arXiv:2404.00622)

### Category B: Economic & ROI Modeling
**Source:** Round 3 dialectics (Directions B, C, D)

### Category C: XRF Physics & Measurement Error Simulation
**Source:** Round 1 Context Briefing, Deep Learning IIoT papers

### Category D: Classification Accuracy Analysis
**Source:** Confusion matrix metrics, F1 score analysis

### Category E: Surface-Volume Correlation Studies
**Source:** Round 1 - "The Critical Unknown"

---

## Feature Requests

### A1: Discrete Event Mining Simulation (OpenMines Recreation)

**Paper Reference:** OpenMines uses SimPy for discrete event simulation with:
- 71 trucks, 21 shovels, 5 loading/unloading points
- Random event modeling (traffic jams, breakdowns)
- Match Factor calculation for fleet efficiency

**Implementation:**
```
notebooks/
  01_openmines_simulation/
    01_discrete_event_simulation.py    # SimPy-based truck dispatch simulation
    02_dispatch_algorithm_comparison.py # Compare SQ, SPTF, Random, Fixed Group
    03_visualization_dashboard.py       # Plotly dashboard for simulation results
```

**Key Visualizations (Plotly):**
1. **Production Over Time Curve** - Animated line chart showing cumulative tonnage by dispatch algorithm
2. **Waiting Trucks Over Time** - Animated area chart showing queue lengths
3. **Match Factor Heatmap** - Shows fleet balance efficiency across different configurations
4. **Traffic Jam Scatter Plot** - Spatial visualization of congestion events

**Databricks Features:**
- Spark Structured Streaming for real-time simulation data
- Delta Lake for storing simulation runs
- MLflow for tracking dispatch algorithm experiments

**Acceptance Criteria:**
- [ ] Implement 4 dispatch algorithms from OpenMines (Random, Nearest, SQ, SPTF)
- [ ] Generate Match Factor, Production, Total Wait Time KPIs
- [ ] Produce animated Plotly visualizations matching Figure 3 from paper
- [ ] Compare results to Table I benchmarks (14909 tons for FixedGroup)

---

### A2: Grade-Based Dispatch Extension (Novel Research)

**Gap Identified:** OpenMines does not model grade-based routing. This is the "academic blind spot" ShovelSense operates in.

**Implementation:**
```
notebooks/
  02_grade_dispatch/
    01_grade_aware_dispatch_algorithm.py  # Extend OpenMines with grade data
    02_destination_optimization.py         # CRUSHER vs WASTE_DUMP routing
    03_fleet_efficiency_tradeoff.py        # Grade vs throughput optimization
```

**Key Visualizations:**
1. **Grade-Weighted Match Factor** - New KPI accounting for ore value, not just tonnage
2. **Destination Congestion Analysis** - Does grade-based routing create crusher bottlenecks?
3. **Pareto Front: Throughput vs Grade Recovery** - Multi-objective optimization visualization
4. **Diversion Decision Tree** - Sankey diagram showing truck routing decisions

**Acceptance Criteria:**
- [ ] Implement grade-aware dispatch logic using existing `shovelsense_classification`
- [ ] Calculate "Grade-Adjusted Match Factor" KPI
- [ ] Quantify throughput penalty of grade-based diversion
- [ ] Produce Pareto optimization curves

---

### B1: Five-Year TCO Comparison Dashboard

**Source:** Round 3, Direction C - Economic Model

**Key Data Points:**
| Technology | 5-Year TCO | Required Improvement | Success Probability |
|------------|------------|---------------------|---------------------|
| ShovelSense | $12.0M | 7.3% | 35% |
| PGNAA | $1.8M | 1.1% | 75% |
| MRT | $4.0M | 2.4% | 65% |
| Blast Hole Opt | $1.6M | 1.0% | 60% |

**Implementation:**
```
notebooks/
  03_economic_analysis/
    01_tco_calculator.py          # Interactive TCO model with parameters
    02_roi_sensitivity.py         # Monte Carlo simulation of ROI outcomes
    03_breakeven_analysis.py      # Required improvement visualization
    04_risk_adjusted_ev.py        # Expected value under uncertainty
```

**Key Visualizations:**
1. **TCO Waterfall Chart** - Breakdown of costs by component over 5 years
2. **ROI Probability Distribution** - Monte Carlo simulation results as violin plots
3. **Breakeven Surface** - 3D plot showing (copper price, grade improvement, success prob)
4. **Decision Matrix Heatmap** - Risk-adjusted EV across technology options

**Acceptance Criteria:**
- [ ] Build interactive Plotly dashboard with parameter sliders
- [ ] Implement Monte Carlo simulation (10,000 runs) for each technology
- [ ] Calculate and visualize risk-adjusted expected value
- [ ] Enable "what-if" scenario analysis

---

### B2: Blast Hole Optimization Cost-Benefit Model

**Source:** Round 3, Direction B - Seven Optimization Levers

**Optimization Levers:**
1. Blast Movement Monitoring ($150-300K/yr, 5-7% improvement)
2. Sampling Protocol Improvements ($50K, 5-15% FSE reduction)
3. Portable XRF Screening ($50K, 2-4 hour faster decisions)
4. Drill Spacing Optimization (variable)
5. ML-Geostatistics ($50-100K, 5-10% improvement)
6. GPS Dig Guidance ($200-400K, near-elimination of dig error)

**Implementation:**
```
notebooks/
  04_blast_hole_optimization/
    01_lever_comparison.py          # Compare 7 optimization levers
    02_implementation_phasing.py    # Phase 1/2/3 timeline visualization
    03_cumulative_improvement.py    # Stacked improvement contributions
```

**Key Visualizations:**
1. **Lever Impact Matrix** - Cost vs Improvement bubble chart
2. **Phased Implementation Gantt** - Timeline with cost/benefit milestones
3. **Cumulative Improvement Curve** - How each lever stacks
4. **Break-Even Timeline** - When does investment pay back?

**Acceptance Criteria:**
- [ ] Create interactive lever selection tool
- [ ] Calculate cumulative improvement with diminishing returns
- [ ] Visualize Phase 1/2/3 implementation path
- [ ] Compare to ShovelSense as baseline alternative

---

### C1: XRF Physics Simulation

**Source:** Round 1 Context Briefing - XRF measurement characteristics

**Key Parameters:**
- Detection limits: <10 ppm for Cu
- Penetration depth: <1mm (surface only)
- Matrix effects: Fe absorbs Cu X-rays (chalcopyrite 30.5% Fe)
- Classification accuracy: 75-93% depending on cutoff grade

**Implementation:**
```
notebooks/
  05_xrf_physics/
    01_penetration_depth_model.py     # Visualize surface-only measurement
    02_matrix_effect_simulation.py     # Fe-Cu absorption modeling
    03_heterogeneity_error_model.py    # Surface vs volume grade distribution
    04_measurement_uncertainty.py      # Total error budget breakdown
```

**Key Visualizations:**
1. **X-ray Penetration Diagram** - Cross-section showing measurement depth
2. **Matrix Effect Scatter** - Fe% vs Cu measurement error
3. **Heterogeneity Error Distribution** - How surface samples differ from volume
4. **Error Budget Sunburst** - Breakdown of total measurement uncertainty

**Acceptance Criteria:**
- [ ] Model XRF penetration depth physics
- [ ] Simulate matrix effect based on chalcopyrite/bornite ratio
- [ ] Generate synthetic measurement error based on heterogeneity
- [ ] Produce error budget visualization

---

### C2: Surface-Volume Correlation Analysis

**Source:** Round 1 - "The Critical Unknown"

The dialectics identified that the correlation between XRF surface measurements and volumetric bucket grade is the key determinant of ShovelSense value.

**Decision Rule from Synthesis:**
- If R² > 0.6: XRF adds substantial signal
- If R² = 0.4-0.6: XRF adds marginal signal
- If R² < 0.4: XRF adds noise

**Implementation:**
```
notebooks/
  06_sv_correlation/
    01_correlation_by_zone.py          # S-V correlation by geological domain
    02_correlation_vs_accuracy.py       # Does higher S-V lead to better F1?
    03_measurement_study_design.py      # Statistical power analysis
    04_decision_boundary_analysis.py    # ROI sensitivity to correlation
```

**Key Visualizations:**
1. **S-V Correlation by Domain** - Bar chart with confidence intervals
2. **Correlation vs Classification Accuracy** - Scatter with regression line
3. **Power Analysis Curves** - Sample size needed for statistical significance
4. **ROI Decision Surface** - 3D surface of ROI vs (S-V correlation, grade variance)

**Acceptance Criteria:**
- [ ] Calculate S-V correlation from synthetic data by geological domain
- [ ] Correlate S-V correlation with classification accuracy (F1 score)
- [ ] Design measurement study with statistical power analysis
- [ ] Produce decision boundary visualization

---

### D1: Confusion Matrix Deep Dive

**Source:** Existing `fact_classification_accuracy` table

**Implementation:**
```
notebooks/
  07_classification_analysis/
    01_confusion_matrix_explorer.py    # Interactive confusion matrix
    02_threshold_optimization.py        # Optimal cutoff grade finder
    03_cost_sensitive_analysis.py       # Asymmetric cost confusion matrix
    04_roc_curve_analysis.py            # ROC and AUC by geological domain
```

**Key Visualizations:**
1. **Animated Confusion Matrix** - Changes over time/by shovel
2. **F1 Score Trend** - Time series with anomaly detection
3. **ROC Curves by Domain** - Compare classification across zones
4. **Cost-Weighted Precision-Recall** - Account for asymmetric error costs

**Acceptance Criteria:**
- [ ] Build interactive confusion matrix with drill-down
- [ ] Calculate cost-weighted metrics (ore loss vs dilution)
- [ ] Generate ROC curves by geological domain
- [ ] Identify optimal cutoff grade threshold

---

### D2: Domain-Stratified Classification Analysis

**Source:** Round 2 Direction D - Zone-dependent XRF accuracy

The dialectics noted: "The 80/20 chalcopyrite/bornite split suggests XRF accuracy may vary spatially."

**Implementation:**
```
notebooks/
  08_domain_analysis/
    01_accuracy_by_domain.py           # Compare domains (BORNITE_CORE, etc.)
    02_mineralogy_impact.py            # Chalcopyrite % vs accuracy
    03_vein_density_analysis.py        # Vein density impact on heterogeneity
    04_zone_recommendation.py          # Where XRF adds value (if anywhere)
```

**Key Visualizations:**
1. **Domain Accuracy Heatmap** - F1/Precision/Recall by geological zone
2. **Mineralogy vs Accuracy Scatter** - Does bornite-rich perform better?
3. **Spatial Accuracy Map** - Geographic visualization of classification accuracy
4. **Zone Recommendation Dashboard** - Traffic light for XRF deployment zones

**Acceptance Criteria:**
- [ ] Calculate all confusion matrix metrics by geological domain
- [ ] Correlate mineralogy (chalcopyrite %) with accuracy
- [ ] Produce spatial visualization of accuracy
- [ ] Generate zone-specific XRF value recommendations

---

### E1: VRP Heuristics Benchmark

**Source:** `vrp-heuristics-survey.pdf` (arXiv:2303.04147)

**Implementation:**
```
notebooks/
  09_vrp_benchmarks/
    01_vrp_algorithms.py              # Implement GA, SA, Tabu Search
    02_benchmark_comparison.py         # Compare against OpenMines algorithms
    03_mining_specific_constraints.py  # Add grade, capacity, cycle time
```

**Key Visualizations:**
1. **Algorithm Performance Radar** - Multi-metric comparison
2. **Convergence Curves** - Optimization progress over iterations
3. **Solution Quality vs Computation Time** - Pareto front

**Acceptance Criteria:**
- [ ] Implement 3+ VRP heuristics from survey
- [ ] Benchmark against OpenMines dispatch algorithms
- [ ] Add mining-specific constraints (grade, loading time)

---

### E2: Deep RL Fleet Optimization Exploration

**Source:** `deep-rl-fleet-size-vrp.pdf` (arXiv:2512.24251)

**Implementation:**
```
notebooks/
  10_deep_rl_exploration/
    01_environment_setup.py           # Gymnasium-compatible mining env
    02_baseline_agents.py             # PPO, DQN baseline implementations
    03_reward_engineering.py          # Grade-weighted reward functions
```

**Key Visualizations:**
1. **Training Curves** - Reward over episodes
2. **Policy Visualization** - What decisions does the agent make?
3. **Comparison Dashboard** - RL vs heuristic vs rule-based

**Acceptance Criteria:**
- [ ] Create Gymnasium environment from synthetic data
- [ ] Train baseline RL agent
- [ ] Compare to rule-based dispatch

---

## Notebook Structure

```
notebooks/
├── 00_data_exploration/
│   ├── 01_synthetic_data_overview.py      # Existing data profiling
│   ├── 02_dialectics_key_findings.py      # Summary of Round 1-3 insights
│   └── 03_data_quality_assessment.py      # DQ checks and validation
│
├── 01_openmines_simulation/               # Feature A1
├── 02_grade_dispatch/                     # Feature A2
├── 03_economic_analysis/                  # Feature B1
├── 04_blast_hole_optimization/            # Feature B2
├── 05_xrf_physics/                        # Feature C1
├── 06_sv_correlation/                     # Feature C2
├── 07_classification_analysis/            # Feature D1
├── 08_domain_analysis/                    # Feature D2
├── 09_vrp_benchmarks/                     # Feature E1
└── 10_deep_rl_exploration/                # Feature E2
```

---

## Implementation Priority

### Phase 1: Foundation (High Priority)
1. **D1: Confusion Matrix Deep Dive** - Immediate value, uses existing data
2. **C2: Surface-Volume Correlation Analysis** - Tests the core hypothesis
3. **B1: Five-Year TCO Comparison Dashboard** - High-impact visualization

### Phase 2: Simulation (Medium Priority)
4. **A1: OpenMines Dispatch Simulation** - Recreates paper results
5. **C1: XRF Physics Simulation** - Educational, builds understanding
6. **D2: Domain-Stratified Classification** - Extends D1

### Phase 3: Advanced (Lower Priority)
7. **A2: Grade-Based Dispatch Extension** - Novel research contribution
8. **B2: Blast Hole Optimization Model** - Alternative investment analysis
9. **E1: VRP Heuristics Benchmark** - Academic comparison
10. **E2: Deep RL Exploration** - Experimental

---

## Technical Requirements

### Databricks Capabilities Used
- **Spark Declarative Pipelines** - Existing Bronze/Silver/Gold architecture
- **Delta Lake** - Storing simulation runs and experiment results
- **MLflow** - Tracking experiments, logging metrics, model registry
- **Unity Catalog** - Governance for all tables and assets
- **Databricks SQL** - Dashboard queries
- **Notebooks** - Interactive analysis with Plotly

### Python Dependencies
```python
# Add to requirements.txt
plotly>=5.18.0
simpy>=4.1.1      # For OpenMines simulation
gymnasium>=0.29.1  # For RL environment
stable-baselines3>=2.2.1  # For RL training
scipy>=1.11.0     # For statistical analysis
scikit-learn>=1.3.0  # For ML metrics
seaborn>=0.13.0   # For statistical visualizations
```

---

## Success Metrics

1. **Reproducibility**: Can recreate OpenMines Table I results within 10% tolerance
2. **Novel Insights**: Identify at least 3 findings not present in original papers
3. **Visualization Quality**: All Plotly dashboards are interactive and publication-ready
4. **Business Value**: Produce decision-support visualizations for ShovelSense evaluation
5. **Code Quality**: All notebooks pass linting, have docstrings, and include unit tests

---

## References

### Primary Sources (Dialectics)
- Round 1 Context Briefing: XRF physics, deposit parameters
- Round 2 Synthesis: Technology comparison, baseline assessment
- Round 3 Synthesis: Economic model, final recommendation

### Academic Papers
1. OpenMines (arXiv:2404.00622) - Truck dispatch simulation
2. Deep RL Fleet (arXiv:2512.24251) - Fleet size optimization
3. VRP Heuristics Survey (arXiv:2303.04147) - Routing algorithms
4. Deep Learning IIoT (arXiv:2008.06701) - IIoT architecture
5. IIoT Smart Manufacturing (arXiv:2312.16174) - Digital twin patterns
6. Rock Classification (arXiv:2510.13937) - ML for mineral ID
7. Blockchain AI IIoT (arXiv:2405.12550) - Data integrity
8. Digital Transformation (arXiv:2503.04749) - Industry 4.0 patterns
9. Fleet-mix EV Routing (arXiv:2408.00663) - Heterogeneous fleets

### Existing Codebase
- `scripts/generate_mining_data.py` - Synthetic data generation
- `bundles/src/shovelsense_pipeline.py` - Spark Declarative Pipeline
- `docs/analysis/*.md` - Per-paper critical assessments
