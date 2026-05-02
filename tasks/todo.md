# Research Analysis Implementation Tasks

**Created:** 2026-04-30
**Plan:** `tasks/research-analysis-plan.md`

---

## Pending Setup: Fizzy Webhook Integration

Code is ready in `integrations/fizzy-webhooks/`. Complete these steps to activate:

- [ ] Deploy webhook receiver (local with ngrok, or to cloud)
- [ ] Configure Fizzy webhook in board settings (click "world" icon)
- [ ] Copy signing secret to `integrations/fizzy-webhooks/.env`
- [ ] (Optional) Set up Twilio WhatsApp sandbox
- [ ] Test with a card move in Fizzy

See `integrations/fizzy-webhooks/README.md` for full instructions.

---

## Phase 1: Foundation (High Priority)

### D1: Diversion Value Analysis (Reframed)
**Key Insight:** Diversions (XRF ≠ Block Model) are VALUE CREATION, not errors.
- [x] Create `notebooks/07_classification_analysis/01_diversion_value_analysis.py`
- [x] Diversion breakdown: Ore Recovery vs Dilution Prevention
- [x] Economic value calculation per diversion type
- [x] Value by geological domain analysis
- [x] Diversion trends over time
- [x] Shovel-level diversion performance
- [x] Create `notebooks/07_classification_analysis/02_diversion_quality_analysis.py`
- [x] Confidence classification (high/marginal based on distance from cutoff)
- [x] XRF vs Block Model grade scatter analysis
- [x] S-V correlation impact on diversion quality

### C2: Surface-Volume Correlation Analysis
- [ ] Create `notebooks/06_sv_correlation/01_correlation_by_zone.py`
- [ ] Calculate S-V correlation from synthetic data by geological domain
- [ ] Correlate S-V correlation with classification accuracy (F1 score)
- [ ] Design measurement study statistical power analysis
- [ ] Produce decision boundary visualization (R² thresholds)

### B1: Five-Year TCO Comparison Dashboard
- [ ] Create `notebooks/03_economic_analysis/01_tco_calculator.py`
- [ ] Build interactive TCO waterfall chart
- [ ] Implement Monte Carlo ROI simulation (10K runs)
- [ ] Create risk-adjusted EV comparison visualization
- [ ] Add parameter sliders for sensitivity analysis

---

## Phase 2: Simulation (Medium Priority)

### A1: OpenMines Dispatch Simulation
- [ ] Create `notebooks/01_openmines_simulation/01_discrete_event_simulation.py`
- [ ] Implement SimPy discrete event simulation
- [ ] Port 4 dispatch algorithms (Random, Nearest, SQ, SPTF)
- [ ] Calculate Match Factor KPI
- [ ] Reproduce Table I results from paper
- [ ] Create animated production curves (Plotly)

### C1: XRF Physics Simulation
- [ ] Create `notebooks/05_xrf_physics/01_penetration_depth_model.py`
- [ ] Model XRF penetration depth physics
- [ ] Simulate matrix effect (Fe-Cu absorption)
- [ ] Create error budget sunburst visualization
- [ ] Document measurement uncertainty breakdown

### D2: Domain-Stratified Classification
- [ ] Create `notebooks/08_domain_analysis/01_accuracy_by_domain.py`
- [ ] Calculate confusion matrix by geological zone
- [ ] Correlate mineralogy (chalcopyrite %) with accuracy
- [ ] Create spatial accuracy map visualization
- [ ] Generate zone-specific XRF value recommendations

---

## Phase 3: Advanced (Lower Priority)

### A2: Grade-Based Dispatch Extension
- [ ] Extend OpenMines with grade-aware routing
- [ ] Calculate Grade-Adjusted Match Factor
- [ ] Visualize throughput vs grade recovery tradeoffs
- [ ] Create Sankey diagram of routing decisions

### B2: Blast Hole Optimization Model
- [ ] Model 7 optimization levers from Round 3 Direction B
- [ ] Create lever impact matrix (cost vs improvement)
- [ ] Visualize Phase 1/2/3 implementation path
- [ ] Compare cumulative improvement to ShovelSense

### E1: VRP Heuristics Benchmark
- [ ] Implement GA, SA, Tabu Search from survey paper
- [ ] Benchmark against OpenMines algorithms
- [ ] Add mining-specific constraints

### E2: Deep RL Exploration
- [ ] Create Gymnasium environment
- [ ] Train baseline RL agent
- [ ] Compare to rule-based dispatch

---

## Completed Tasks

### D1: Diversion Value Analysis (2026-05-01) - REFRAMED
**Critical Reframe:** Diversions are not "errors" - they are value creation events.
The XRF disagrees with the block model when it detects something the model missed.

- Notebooks:
  - `01_diversion_value_analysis.py` - Economic value of diversions
  - `02_diversion_quality_analysis.py` - Confidence and quality metrics
- Key analyses:
  - Diversion breakdown (Ore Recovery vs Dilution Prevention)
  - Economic value per diversion type
  - Value by geological domain
  - Diversion confidence classification (distance from cutoff)
  - XRF vs Block Model grade comparison
  - S-V correlation impact on quality

---

## Review Section
(Add review notes after implementation)
