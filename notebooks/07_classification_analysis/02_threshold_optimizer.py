# Databricks notebook source
# MAGIC %md
# MAGIC # D1.2: Threshold Optimization & ROC Analysis
# MAGIC
# MAGIC Detailed analysis of classification thresholds with:
# MAGIC - **Multi-threshold ROC curves** by geological domain
# MAGIC - **Cost-optimized threshold selection**
# MAGIC - **Sensitivity analysis** across operating conditions
# MAGIC
# MAGIC **Builds on:** `01_confusion_matrix_explorer.py`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from sklearn.metrics import roc_curve, auc, precision_recall_curve

# Configuration - serverless compute doesn't support custom spark.conf keys
try:
    CATALOG = spark.conf.get("catalog")
except Exception:
    CATALOG = "cjc_aws_workspace_catalog"

try:
    SCHEMA = spark.conf.get("schema")
except Exception:
    SCHEMA = "shovelsense"

# Economic parameters
COPPER_PRICE = 8820  # $/tonne
RECOVERY = 0.85
AVG_PAYLOAD = 200  # tonnes

print(f"Catalog: {CATALOG}, Schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Prepare Data

# COMMAND ----------

# Load truck loads with block model grades
truck_loads = (
    spark.table(f"{CATALOG}.{SCHEMA}.fact_truck_loads")
    .join(
        spark.table(f"{CATALOG}.{SCHEMA}.dim_block_model")
        .select("block_id", "planned_cu_grade", "geological_domain",
                "surface_volume_correlation", "chalcopyrite_pct"),
        "block_id"
    )
)

# Convert to pandas for ROC analysis
df = truck_loads.select(
    "load_id",
    "avg_cu_grade_pct",
    "planned_cu_grade",
    "geological_domain",
    "surface_volume_correlation",
    "payload_tonnes",
    "planned_classification",
    "shovelsense_classification"
).toPandas()

print(f"Loaded {len(df)} truck loads for analysis")

# COMMAND ----------

# Create binary labels for ROC curve
# Ground truth: Is it actually ore? (planned_cu_grade >= 0.32)
df['y_true'] = (df['planned_cu_grade'] >= 0.32).astype(int)
# Prediction score: XRF measured grade
df['y_score'] = df['avg_cu_grade_pct']

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Overall ROC Curve

# COMMAND ----------

# Calculate ROC curve
fpr, tpr, thresholds = roc_curve(df['y_true'], df['y_score'])
roc_auc = auc(fpr, tpr)

# Find optimal threshold using Youden's J statistic
j_scores = tpr - fpr
optimal_idx = np.argmax(j_scores)
optimal_threshold = thresholds[optimal_idx]

fig_roc = go.Figure()

# ROC curve
fig_roc.add_trace(go.Scatter(
    x=fpr, y=tpr,
    mode='lines',
    name=f'ROC Curve (AUC = {roc_auc:.3f})',
    line=dict(color='#1f77b4', width=2)
))

# Random baseline
fig_roc.add_trace(go.Scatter(
    x=[0, 1], y=[0, 1],
    mode='lines',
    name='Random Classifier',
    line=dict(color='gray', dash='dash')
))

# Mark optimal point
fig_roc.add_trace(go.Scatter(
    x=[fpr[optimal_idx]],
    y=[tpr[optimal_idx]],
    mode='markers+text',
    name=f'Optimal (threshold={optimal_threshold:.3f})',
    marker=dict(color='red', size=12),
    text=[f'{optimal_threshold:.3f}%'],
    textposition='top right'
))

# Mark default cutoff (0.32%)
default_idx = np.argmin(np.abs(thresholds - 0.32))
fig_roc.add_trace(go.Scatter(
    x=[fpr[default_idx]],
    y=[tpr[default_idx]],
    mode='markers+text',
    name='Default (0.32% Cu)',
    marker=dict(color='green', size=10, symbol='diamond'),
    text=['0.32%'],
    textposition='bottom right'
))

fig_roc.update_layout(
    title='ROC Curve: ShovelSense Ore Classification',
    xaxis_title='False Positive Rate',
    yaxis_title='True Positive Rate',
    height=500,
    width=600,
    legend=dict(x=0.5, y=0.1)
)

fig_roc.show()

print(f"\nOptimal Threshold: {optimal_threshold:.3f}% Cu")
print(f"At optimal: TPR={tpr[optimal_idx]:.3f}, FPR={fpr[optimal_idx]:.3f}")
print(f"Youden's J: {j_scores[optimal_idx]:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ROC Curves by Geological Domain

# COMMAND ----------

domains = df['geological_domain'].unique()

fig_domain_roc = go.Figure()

# Add random baseline
fig_domain_roc.add_trace(go.Scatter(
    x=[0, 1], y=[0, 1],
    mode='lines',
    name='Random',
    line=dict(color='gray', dash='dash')
))

# Calculate ROC for each domain
domain_aucs = {}
for domain in domains:
    domain_df = df[df['geological_domain'] == domain]
    if len(domain_df) > 100:  # Need sufficient samples
        fpr_d, tpr_d, _ = roc_curve(domain_df['y_true'], domain_df['y_score'])
        auc_d = auc(fpr_d, tpr_d)
        domain_aucs[domain] = auc_d

        fig_domain_roc.add_trace(go.Scatter(
            x=fpr_d, y=tpr_d,
            mode='lines',
            name=f'{domain} (AUC={auc_d:.3f})'
        ))

fig_domain_roc.update_layout(
    title='ROC Curves by Geological Domain',
    xaxis_title='False Positive Rate',
    yaxis_title='True Positive Rate',
    height=600,
    width=800,
    legend=dict(x=1.02, y=1)
)

fig_domain_roc.show()

# Print domain AUC ranking
print("\nDomain AUC Ranking (Higher = Better XRF Discrimination):")
for domain, auc_val in sorted(domain_aucs.items(), key=lambda x: x[1], reverse=True):
    print(f"  {domain}: {auc_val:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Precision-Recall Curve

# COMMAND ----------

precision, recall, pr_thresholds = precision_recall_curve(df['y_true'], df['y_score'])

# Calculate F1 scores at each threshold
f1_scores = 2 * (precision * recall) / (precision + recall + 1e-10)
optimal_f1_idx = np.argmax(f1_scores[:-1])  # Last value is always 1
optimal_f1_threshold = pr_thresholds[optimal_f1_idx]

fig_pr = make_subplots(rows=1, cols=2,
    subplot_titles=('Precision-Recall Curve', 'F1 Score by Threshold'))

# PR curve
fig_pr.add_trace(go.Scatter(
    x=recall, y=precision,
    mode='lines',
    name='PR Curve',
    line=dict(color='#2ca02c', width=2)
), row=1, col=1)

# Mark optimal F1 point
fig_pr.add_trace(go.Scatter(
    x=[recall[optimal_f1_idx]],
    y=[precision[optimal_f1_idx]],
    mode='markers',
    name=f'Max F1 ({f1_scores[optimal_f1_idx]:.3f})',
    marker=dict(color='red', size=12)
), row=1, col=1)

# F1 by threshold
fig_pr.add_trace(go.Scatter(
    x=pr_thresholds,
    y=f1_scores[:-1],
    mode='lines',
    name='F1 Score',
    line=dict(color='#1f77b4', width=2)
), row=1, col=2)

# Mark optimal and default
fig_pr.add_vline(x=optimal_f1_threshold, line_dash="dash", line_color="red",
                 row=1, col=2, annotation_text=f"Optimal: {optimal_f1_threshold:.2f}%")
fig_pr.add_vline(x=0.32, line_dash="dash", line_color="green",
                 row=1, col=2, annotation_text="Default: 0.32%")

fig_pr.update_layout(
    height=400,
    width=900,
    title_text="Precision-Recall Analysis"
)

fig_pr.update_xaxes(title_text="Recall", row=1, col=1)
fig_pr.update_yaxes(title_text="Precision", row=1, col=1)
fig_pr.update_xaxes(title_text="Threshold (%Cu)", row=1, col=2)
fig_pr.update_yaxes(title_text="F1 Score", row=1, col=2)

fig_pr.show()

print(f"\nOptimal F1 Threshold: {optimal_f1_threshold:.3f}% Cu")
print(f"Max F1 Score: {f1_scores[optimal_f1_idx]:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cost-Optimized Threshold Selection
# MAGIC
# MAGIC Find the threshold that minimizes economic loss, accounting for asymmetric costs.

# COMMAND ----------

def calculate_economic_loss(threshold: float, df: pd.DataFrame,
                            fn_cost_per_tonne: float = 75,  # Lost ore value
                            fp_cost_per_tonne: float = 12   # Processing waste
                            ) -> float:
    """
    Calculate total economic loss at a given threshold.

    Args:
        threshold: Cutoff grade for ore/waste classification
        df: DataFrame with y_true, y_score, payload_tonnes
        fn_cost_per_tonne: Cost per tonne for false negatives (ore sent to waste)
        fp_cost_per_tonne: Cost per tonne for false positives (waste sent to mill)

    Returns:
        Total economic loss in dollars
    """
    # Classify at threshold
    y_pred = (df['y_score'] >= threshold).astype(int)

    # False negatives: actual ore (y_true=1) predicted as waste (y_pred=0)
    fn_mask = (df['y_true'] == 1) & (y_pred == 0)
    fn_tonnes = df.loc[fn_mask, 'payload_tonnes'].sum()
    fn_loss = fn_tonnes * fn_cost_per_tonne

    # False positives: actual waste (y_true=0) predicted as ore (y_pred=1)
    fp_mask = (df['y_true'] == 0) & (y_pred == 1)
    fp_tonnes = df.loc[fp_mask, 'payload_tonnes'].sum()
    fp_loss = fp_tonnes * fp_cost_per_tonne

    return fn_loss + fp_loss

# Test thresholds
thresholds_test = np.arange(0.15, 0.55, 0.01)
losses = [calculate_economic_loss(t, df) for t in thresholds_test]

# Find minimum loss threshold
optimal_cost_idx = np.argmin(losses)
optimal_cost_threshold = thresholds_test[optimal_cost_idx]

fig_cost = go.Figure()

fig_cost.add_trace(go.Scatter(
    x=thresholds_test,
    y=[l / 1e6 for l in losses],  # Convert to millions
    mode='lines',
    name='Total Economic Loss',
    line=dict(color='#d62728', width=2)
))

fig_cost.add_vline(x=optimal_cost_threshold, line_dash="dash", line_color="blue",
                   annotation_text=f"Cost-Optimal: {optimal_cost_threshold:.2f}%")
fig_cost.add_vline(x=0.32, line_dash="dash", line_color="green",
                   annotation_text="Default: 0.32%")
fig_cost.add_vline(x=optimal_f1_threshold, line_dash="dash", line_color="orange",
                   annotation_text=f"F1-Optimal: {optimal_f1_threshold:.2f}%")

fig_cost.update_layout(
    title='Economic Loss by Cutoff Threshold',
    xaxis_title='Cutoff Grade (%Cu)',
    yaxis_title='Total Economic Loss ($M)',
    height=400,
    width=700
)

fig_cost.show()

print(f"\nCost-Optimal Threshold: {optimal_cost_threshold:.3f}% Cu")
print(f"Loss at optimal: ${losses[optimal_cost_idx]/1e6:.2f}M")
print(f"Loss at default (0.32%): ${calculate_economic_loss(0.32, df)/1e6:.2f}M")
print(f"Potential savings: ${(calculate_economic_loss(0.32, df) - losses[optimal_cost_idx])/1e6:.2f}M")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sensitivity Analysis: Cost Ratio Impact

# COMMAND ----------

# Test different FN/FP cost ratios
cost_ratios = [2, 4, 6, 8, 10, 15, 20]
base_fp_cost = 12

results = []
for ratio in cost_ratios:
    fn_cost = base_fp_cost * ratio
    losses = [calculate_economic_loss(t, df, fn_cost, base_fp_cost) for t in thresholds_test]
    optimal_idx = np.argmin(losses)
    results.append({
        'cost_ratio': ratio,
        'optimal_threshold': thresholds_test[optimal_idx],
        'min_loss': losses[optimal_idx]
    })

results_df = pd.DataFrame(results)

fig_sensitivity = px.line(
    results_df,
    x='cost_ratio',
    y='optimal_threshold',
    markers=True,
    title='Optimal Threshold Sensitivity to FN/FP Cost Ratio',
    labels={
        'cost_ratio': 'FN/FP Cost Ratio',
        'optimal_threshold': 'Optimal Cutoff (%Cu)'
    }
)

fig_sensitivity.add_hline(y=0.32, line_dash="dash", line_color="green",
                          annotation_text="Default: 0.32%")

fig_sensitivity.update_layout(height=400, width=600)
fig_sensitivity.show()

print("\nSensitivity Analysis Results:")
print("-" * 50)
for _, row in results_df.iterrows():
    print(f"Cost Ratio {row['cost_ratio']:2.0f}x → Optimal Threshold: {row['optimal_threshold']:.2f}% Cu")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary & Recommendations

# COMMAND ----------

print("=" * 70)
print("THRESHOLD OPTIMIZATION SUMMARY")
print("=" * 70)
print(f"\n{'Metric':<35} {'Optimal Threshold':<20} {'Value'}")
print("-" * 70)
print(f"{'Youden J Statistic (ROC):':<35} {optimal_threshold:.3f}% Cu{'':<15} J={j_scores[optimal_idx]:.3f}")
print(f"{'Maximum F1 Score:':<35} {optimal_f1_threshold:.3f}% Cu{'':<15} F1={f1_scores[optimal_f1_idx]:.3f}")
print(f"{'Minimum Economic Loss:':<35} {optimal_cost_threshold:.3f}% Cu{'':<15} ${losses[optimal_cost_idx]/1e6:.2f}M")
print(f"{'Current Default:':<35} 0.320% Cu")
print("-" * 70)

print("\nRECOMMENDATIONS:")
print("-" * 70)
if optimal_cost_threshold < 0.32:
    print(f"→ Consider LOWERING cutoff to {optimal_cost_threshold:.2f}% to capture more ore value")
    print(f"  Potential annual savings: ${(calculate_economic_loss(0.32, df) - losses[optimal_cost_idx])/1e6:.2f}M")
elif optimal_cost_threshold > 0.32:
    print(f"→ Consider RAISING cutoff to {optimal_cost_threshold:.2f}% to reduce processing costs")
else:
    print("→ Current default threshold is near-optimal")

print("\nDOMAIN-SPECIFIC INSIGHTS:")
print("-" * 70)
for domain, auc_val in sorted(domain_aucs.items(), key=lambda x: x[1], reverse=True)[:3]:
    print(f"→ {domain}: AUC={auc_val:.3f} - Good XRF discrimination")
for domain, auc_val in sorted(domain_aucs.items(), key=lambda x: x[1])[:2]:
    print(f"→ {domain}: AUC={auc_val:.3f} - Poor XRF discrimination, consider alternative methods")

print("=" * 70)
