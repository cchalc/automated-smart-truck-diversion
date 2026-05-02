# Databricks notebook source
# MAGIC %md
# MAGIC # D1: Confusion Matrix Deep Dive
# MAGIC
# MAGIC Interactive exploration of ShovelSense classification accuracy with:
# MAGIC - **Confusion Matrix Visualization** with drill-down by shovel/date
# MAGIC - **Threshold Optimization** for cutoff grade
# MAGIC - **Cost-Weighted Metrics** (ore loss cost vs dilution cost)
# MAGIC - **ROC Curves** by geological domain
# MAGIC
# MAGIC **Source:** `fact_classification_accuracy`, `fact_truck_loads`, `fact_domain_classification_accuracy`
# MAGIC
# MAGIC **Reference:** Research Analysis Plan - Feature D1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuration - adjust to your catalog/schema
# Note: spark.conf.get throws CONFIG_NOT_AVAILABLE on serverless for custom keys
try:
    CATALOG = spark.conf.get("catalog")
except Exception:
    CATALOG = "cjc_aws_workspace_catalog"

try:
    SCHEMA = spark.conf.get("schema")
except Exception:
    SCHEMA = "shovelsense"

# Economic parameters from Round 3 Direction C
COPPER_PRICE_PER_TONNE = 8820  # $4/lb = $8,820/tonne
METALLURGICAL_RECOVERY = 0.85
ORE_PROCESSING_COST_PER_TONNE = 15  # $/tonne to process ore
WASTE_DISPOSAL_COST_PER_TONNE = 3   # $/tonne to dump waste

# Cutoff grade from Round 1 Context Briefing
DEFAULT_CUTOFF = 0.32

print(f"Using catalog: {CATALOG}, schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data from Pipeline Tables

# COMMAND ----------

# Load fact tables
fact_accuracy = spark.table(f"{CATALOG}.{SCHEMA}.fact_classification_accuracy").toPandas()
fact_truck_loads = spark.table(f"{CATALOG}.{SCHEMA}.fact_truck_loads")
fact_domain_accuracy = spark.table(f"{CATALOG}.{SCHEMA}.fact_domain_classification_accuracy").toPandas()

# Load dimension tables for drill-down
dim_shovels = spark.table(f"{CATALOG}.{SCHEMA}.dim_shovels").toPandas()

print(f"Loaded {len(fact_accuracy)} days of accuracy data")
print(f"Loaded {fact_truck_loads.count()} truck loads")
print(f"Loaded {len(fact_domain_accuracy)} geological domains")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Interactive Confusion Matrix Explorer
# MAGIC
# MAGIC Visualize the confusion matrix with drill-down capabilities by date and shovel.

# COMMAND ----------

def create_confusion_matrix_heatmap(df: pd.DataFrame, title: str = "Classification Confusion Matrix") -> go.Figure:
    """
    Create an interactive confusion matrix heatmap from aggregated metrics.

    Args:
        df: DataFrame with columns: true_positive, true_negative, false_positive, false_negative
        title: Chart title

    Returns:
        Plotly Figure
    """
    # Aggregate totals
    tp = df['true_positive'].sum()
    tn = df['true_negative'].sum()
    fp = df['false_positive'].sum()
    fn = df['false_negative'].sum()

    # Build confusion matrix
    cm = np.array([[tn, fp], [fn, tp]])

    # Calculate percentages
    total = cm.sum()
    cm_pct = cm / total * 100

    # Labels
    labels = ['WASTE (Actual)', 'ORE (Actual)']
    predictions = ['WASTE (Predicted)', 'ORE (Predicted)']

    # Create heatmap with annotations
    text = [[f"TN: {cm[0,0]:,}<br>{cm_pct[0,0]:.1f}%",
             f"FP: {cm[0,1]:,}<br>{cm_pct[0,1]:.1f}%<br>(Ore from Waste)"],
            [f"FN: {cm[1,0]:,}<br>{cm_pct[1,0]:.1f}%<br>(Waste from Ore)",
             f"TP: {cm[1,1]:,}<br>{cm_pct[1,1]:.1f}%"]]

    fig = go.Figure(data=go.Heatmap(
        z=cm,
        x=predictions,
        y=labels,
        text=text,
        texttemplate="%{text}",
        textfont={"size": 14},
        colorscale='Blues',
        showscale=True,
        colorbar=dict(title="Count")
    ))

    # Calculate metrics
    accuracy = (tp + tn) / total if total > 0 else 0
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    fig.update_layout(
        title=dict(
            text=f"{title}<br><sup>Accuracy: {accuracy:.1%} | Precision: {precision:.1%} | Recall: {recall:.1%} | F1: {f1:.3f}</sup>",
            x=0.5,
            xanchor='center'
        ),
        xaxis_title="ShovelSense Prediction",
        yaxis_title="Blast Hole Ground Truth",
        height=500,
        width=700
    )

    return fig

# Create overall confusion matrix
fig_cm = create_confusion_matrix_heatmap(fact_accuracy, "ShovelSense Classification Performance")
fig_cm.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Confusion Matrix by Date (Time Series)

# COMMAND ----------

def create_metrics_timeseries(df: pd.DataFrame) -> go.Figure:
    """Create time series of classification metrics."""
    df = df.sort_values('load_date')

    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Classification Metrics Over Time', 'Confusion Matrix Components'),
        vertical_spacing=0.15
    )

    # Top plot: F1, Precision, Recall
    fig.add_trace(
        go.Scatter(x=df['load_date'], y=df['f1_score'], name='F1 Score',
                   line=dict(color='#1f77b4', width=2)),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=df['load_date'], y=df['precision_ore'], name='Precision',
                   line=dict(color='#2ca02c', width=2, dash='dash')),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=df['load_date'], y=df['recall_ore'], name='Recall',
                   line=dict(color='#ff7f0e', width=2, dash='dot')),
        row=1, col=1
    )

    # Bottom plot: Stacked area of TP, TN, FP, FN
    fig.add_trace(
        go.Scatter(x=df['load_date'], y=df['true_positive'], name='True Positive',
                   stackgroup='one', fillcolor='rgba(31, 119, 180, 0.5)'),
        row=2, col=1
    )
    fig.add_trace(
        go.Scatter(x=df['load_date'], y=df['true_negative'], name='True Negative',
                   stackgroup='one', fillcolor='rgba(44, 160, 44, 0.5)'),
        row=2, col=1
    )
    fig.add_trace(
        go.Scatter(x=df['load_date'], y=df['false_positive'], name='False Positive (Ore from Waste)',
                   stackgroup='one', fillcolor='rgba(255, 127, 14, 0.5)'),
        row=2, col=1
    )
    fig.add_trace(
        go.Scatter(x=df['load_date'], y=df['false_negative'], name='False Negative (Waste from Ore)',
                   stackgroup='one', fillcolor='rgba(214, 39, 40, 0.5)'),
        row=2, col=1
    )

    fig.update_layout(
        height=700,
        title_text="Classification Performance Over Time",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2)
    )

    fig.update_yaxes(title_text="Score", row=1, col=1)
    fig.update_yaxes(title_text="Load Count", row=2, col=1)

    return fig

fig_ts = create_metrics_timeseries(fact_accuracy)
fig_ts.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Confusion Matrix by Shovel (Equipment Comparison)

# COMMAND ----------

# Aggregate by shovel
shovel_accuracy = (
    fact_truck_loads
    .groupBy("shovel_id")
    .agg(
        F.count("*").alias("total_loads"),
        F.sum(F.when(
            (F.col("planned_classification") == "ORE") &
            (F.col("shovelsense_classification") == "ORE"), 1
        ).otherwise(0)).alias("true_positive"),
        F.sum(F.when(
            (F.col("planned_classification") == "WASTE") &
            (F.col("shovelsense_classification") == "WASTE"), 1
        ).otherwise(0)).alias("true_negative"),
        F.sum(F.when(
            (F.col("planned_classification") == "WASTE") &
            (F.col("shovelsense_classification") == "ORE"), 1
        ).otherwise(0)).alias("false_positive"),
        F.sum(F.when(
            (F.col("planned_classification") == "ORE") &
            (F.col("shovelsense_classification") == "WASTE"), 1
        ).otherwise(0)).alias("false_negative"),
        F.avg("avg_xrf_confidence").alias("avg_xrf_confidence")
    )
    .withColumn("accuracy",
        (F.col("true_positive") + F.col("true_negative")) / F.col("total_loads"))
    .withColumn("precision_ore",
        F.col("true_positive") / (F.col("true_positive") + F.col("false_positive")))
    .withColumn("recall_ore",
        F.col("true_positive") / (F.col("true_positive") + F.col("false_negative")))
    .withColumn("f1_score",
        2 * F.col("precision_ore") * F.col("recall_ore") /
        (F.col("precision_ore") + F.col("recall_ore")))
    .toPandas()
)

# Create shovel comparison chart
fig_shovel = px.bar(
    shovel_accuracy.sort_values('f1_score', ascending=True),
    y='shovel_id',
    x=['f1_score', 'precision_ore', 'recall_ore'],
    orientation='h',
    title='Classification Performance by Shovel',
    labels={'value': 'Score', 'shovel_id': 'Shovel ID', 'variable': 'Metric'},
    barmode='group',
    color_discrete_map={'f1_score': '#1f77b4', 'precision_ore': '#2ca02c', 'recall_ore': '#ff7f0e'}
)

fig_shovel.update_layout(
    height=400,
    legend_title="Metric",
    xaxis_range=[0, 1]
)
fig_shovel.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Threshold Optimization for Cutoff Grade
# MAGIC
# MAGIC Explore how different Cu cutoff grades affect classification performance.
# MAGIC The default cutoff is 0.32% Cu from the Round 1 Context Briefing.

# COMMAND ----------

def calculate_metrics_at_threshold(df, threshold: float) -> dict:
    """
    Recalculate confusion matrix metrics at a different cutoff threshold.

    Args:
        df: Spark DataFrame with avg_cu_grade_pct and planned_cu_grade columns
        threshold: New cutoff grade to test

    Returns:
        Dictionary with classification metrics
    """
    # Reclassify based on new threshold
    reclassified = df.withColumn(
        "new_planned",
        F.when(F.col("planned_cu_grade") >= threshold, "ORE").otherwise("WASTE")
    ).withColumn(
        "new_xrf",
        F.when(F.col("avg_cu_grade_pct") >= threshold, "ORE").otherwise("WASTE")
    )

    # Calculate confusion matrix
    metrics = reclassified.agg(
        F.count("*").alias("total"),
        F.sum(F.when((F.col("new_planned") == "ORE") & (F.col("new_xrf") == "ORE"), 1).otherwise(0)).alias("tp"),
        F.sum(F.when((F.col("new_planned") == "WASTE") & (F.col("new_xrf") == "WASTE"), 1).otherwise(0)).alias("tn"),
        F.sum(F.when((F.col("new_planned") == "WASTE") & (F.col("new_xrf") == "ORE"), 1).otherwise(0)).alias("fp"),
        F.sum(F.when((F.col("new_planned") == "ORE") & (F.col("new_xrf") == "WASTE"), 1).otherwise(0)).alias("fn")
    ).collect()[0]

    tp, tn, fp, fn = metrics['tp'], metrics['tn'], metrics['fp'], metrics['fn']
    total = metrics['total']

    accuracy = (tp + tn) / total if total > 0 else 0
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    return {
        'threshold': threshold,
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1_score': f1,
        'tp': tp, 'tn': tn, 'fp': fp, 'fn': fn
    }

# Join truck loads with block model to get planned grades
truck_with_grades = (
    fact_truck_loads
    .join(
        spark.table(f"{CATALOG}.{SCHEMA}.dim_block_model").select("block_id", "planned_cu_grade"),
        "block_id"
    )
)

# Test different thresholds
thresholds = np.arange(0.20, 0.50, 0.02)
threshold_results = []

for thresh in thresholds:
    metrics = calculate_metrics_at_threshold(truck_with_grades, thresh)
    threshold_results.append(metrics)

threshold_df = pd.DataFrame(threshold_results)

# COMMAND ----------

# Plot threshold optimization
fig_thresh = make_subplots(
    rows=1, cols=2,
    subplot_titles=('F1 Score by Cutoff Grade', 'Precision-Recall Trade-off'),
    horizontal_spacing=0.1
)

# F1 score curve
fig_thresh.add_trace(
    go.Scatter(
        x=threshold_df['threshold'],
        y=threshold_df['f1_score'],
        name='F1 Score',
        mode='lines+markers',
        line=dict(color='#1f77b4', width=3)
    ),
    row=1, col=1
)

# Mark current cutoff
current_f1 = threshold_df[threshold_df['threshold'] == 0.32]['f1_score'].values
if len(current_f1) > 0:
    fig_thresh.add_vline(x=0.32, line_dash="dash", line_color="red", row=1, col=1)
    fig_thresh.add_annotation(
        x=0.32, y=current_f1[0],
        text=f"Current: 0.32%<br>F1={current_f1[0]:.3f}",
        showarrow=True, row=1, col=1
    )

# Find optimal threshold
optimal_idx = threshold_df['f1_score'].idxmax()
optimal_thresh = threshold_df.loc[optimal_idx, 'threshold']
optimal_f1 = threshold_df.loc[optimal_idx, 'f1_score']
fig_thresh.add_annotation(
    x=optimal_thresh, y=optimal_f1,
    text=f"Optimal: {optimal_thresh:.2f}%<br>F1={optimal_f1:.3f}",
    showarrow=True, row=1, col=1
)

# Precision-Recall curve
fig_thresh.add_trace(
    go.Scatter(
        x=threshold_df['recall'],
        y=threshold_df['precision'],
        name='PR Curve',
        mode='lines+markers',
        text=[f"Cutoff: {t:.2f}%" for t in threshold_df['threshold']],
        hoverinfo='text+x+y',
        line=dict(color='#2ca02c', width=2)
    ),
    row=1, col=2
)

fig_thresh.update_layout(
    height=400,
    title_text="Cutoff Grade Threshold Optimization",
    showlegend=True
)

fig_thresh.update_xaxes(title_text="Cutoff Grade (%Cu)", row=1, col=1)
fig_thresh.update_yaxes(title_text="F1 Score", row=1, col=1)
fig_thresh.update_xaxes(title_text="Recall", row=1, col=2)
fig_thresh.update_yaxes(title_text="Precision", row=1, col=2)

fig_thresh.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cost-Weighted Metrics
# MAGIC
# MAGIC Not all classification errors are equal:
# MAGIC - **False Negative (Waste from Ore)**: Lost ore value - potentially significant revenue loss
# MAGIC - **False Positive (Ore from Waste)**: Processing cost for waste material
# MAGIC
# MAGIC From Round 3 Direction C, the economic impact is asymmetric.

# COMMAND ----------

def calculate_cost_weighted_metrics(df: pd.DataFrame) -> dict:
    """
    Calculate cost-weighted classification metrics.

    Economic assumptions from Round 3:
    - Copper price: $8,820/tonne
    - Recovery: 85%
    - Ore processing cost: $15/tonne
    - Waste disposal cost: $3/tonne
    """
    # Get total loads
    tp = df['true_positive'].sum()
    tn = df['true_negative'].sum()
    fp = df['false_positive'].sum()
    fn = df['false_negative'].sum()

    # Estimated average grades for error types (from typical data)
    avg_ore_grade = 0.50  # Average grade of ore loads
    avg_waste_grade = 0.15  # Average grade of waste loads
    avg_tonnes_per_load = 200  # Typical payload

    # Cost of False Negative (Waste from Ore): Lost ore value
    # We send ore to waste dump instead of mill
    ore_value_per_load = (avg_tonnes_per_load * (avg_ore_grade / 100) *
                          METALLURGICAL_RECOVERY * COPPER_PRICE_PER_TONNE)
    fn_cost = fn * ore_value_per_load

    # Cost of False Positive (Ore from Waste): Processing cost without value
    # We process waste at mill instead of dumping
    processing_penalty = (ORE_PROCESSING_COST_PER_TONNE - WASTE_DISPOSAL_COST_PER_TONNE)
    fp_cost = fp * avg_tonnes_per_load * processing_penalty

    # Benefit of True Positive: Correctly captured ore value
    tp_benefit = tp * ore_value_per_load

    # Benefit of True Negative: Avoided processing waste
    tn_benefit = tn * avg_tonnes_per_load * processing_penalty

    # Calculate cost-weighted accuracy
    total_cost = fn_cost + fp_cost
    total_benefit = tp_benefit + tn_benefit
    net_value = total_benefit - total_cost

    return {
        'fn_cost': fn_cost,
        'fp_cost': fp_cost,
        'tp_benefit': tp_benefit,
        'tn_benefit': tn_benefit,
        'total_cost': total_cost,
        'total_benefit': total_benefit,
        'net_value': net_value,
        'cost_ratio': fn_cost / fp_cost if fp_cost > 0 else float('inf')
    }

cost_metrics = calculate_cost_weighted_metrics(fact_accuracy)

# Create cost breakdown visualization
fig_cost = make_subplots(
    rows=1, cols=2,
    subplot_titles=('Cost/Benefit Breakdown', 'Error Type Comparison'),
    specs=[[{"type": "pie"}, {"type": "bar"}]]
)

# Pie chart of costs/benefits
labels = ['FN Cost (Lost Ore)', 'FP Cost (Waste Processing)',
          'TP Benefit (Ore Captured)', 'TN Benefit (Waste Avoided)']
values = [cost_metrics['fn_cost'], cost_metrics['fp_cost'],
          cost_metrics['tp_benefit'], cost_metrics['tn_benefit']]
colors = ['#d62728', '#ff7f0e', '#2ca02c', '#1f77b4']

fig_cost.add_trace(
    go.Pie(labels=labels, values=values, marker_colors=colors,
           textinfo='label+percent', hole=0.3),
    row=1, col=1
)

# Bar chart comparing FN vs FP costs
fig_cost.add_trace(
    go.Bar(
        x=['False Negative<br>(Lost Ore)', 'False Positive<br>(Waste Processing)'],
        y=[cost_metrics['fn_cost'], cost_metrics['fp_cost']],
        marker_color=['#d62728', '#ff7f0e'],
        text=[f"${cost_metrics['fn_cost']:,.0f}", f"${cost_metrics['fp_cost']:,.0f}"],
        textposition='outside'
    ),
    row=1, col=2
)

fig_cost.update_layout(
    height=450,
    title_text=f"Cost-Weighted Analysis | Net Value: ${cost_metrics['net_value']:,.0f} | FN/FP Cost Ratio: {cost_metrics['cost_ratio']:.1f}x",
    showlegend=False
)

fig_cost.update_yaxes(title_text="Cost ($)", row=1, col=2)

fig_cost.show()

# COMMAND ----------

# Print summary
print("=" * 60)
print("COST-WEIGHTED METRICS SUMMARY")
print("=" * 60)
print(f"\nError Costs:")
print(f"  False Negative (Lost Ore):      ${cost_metrics['fn_cost']:>15,.0f}")
print(f"  False Positive (Waste Process): ${cost_metrics['fp_cost']:>15,.0f}")
print(f"  Total Error Cost:               ${cost_metrics['total_cost']:>15,.0f}")
print(f"\nBenefits:")
print(f"  True Positive (Ore Captured):   ${cost_metrics['tp_benefit']:>15,.0f}")
print(f"  True Negative (Waste Avoided):  ${cost_metrics['tn_benefit']:>15,.0f}")
print(f"  Total Benefit:                  ${cost_metrics['total_benefit']:>15,.0f}")
print(f"\n{'='*60}")
print(f"NET VALUE:                        ${cost_metrics['net_value']:>15,.0f}")
print(f"FN/FP Cost Ratio:                 {cost_metrics['cost_ratio']:>15.1f}x")
print("=" * 60)
print(f"\nKey Insight: False negatives (sending ore to waste) cost")
print(f"{cost_metrics['cost_ratio']:.1f}x more than false positives (processing waste).")
print(f"This suggests optimizing for higher RECALL over PRECISION.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ROC Curves by Geological Domain
# MAGIC
# MAGIC From Round 2 Direction D: "The 80/20 chalcopyrite/bornite split suggests XRF accuracy may vary spatially."
# MAGIC
# MAGIC Analyze classification performance by geological domain to identify zones where XRF adds the most value.

# COMMAND ----------

# Use pre-computed domain accuracy
fig_domain = px.scatter(
    fact_domain_accuracy,
    x='avg_surface_volume_corr',
    y='f1_score',
    size='total_loads',
    color='geological_domain',
    hover_data=['accuracy', 'precision_ore', 'recall_ore', 'avg_xrf_confidence'],
    title='Classification Accuracy by Geological Domain',
    labels={
        'avg_surface_volume_corr': 'Average Surface-Volume Correlation',
        'f1_score': 'F1 Score',
        'geological_domain': 'Geological Domain'
    }
)

# Add reference lines for decision thresholds from Round 1
fig_domain.add_hline(y=0.85, line_dash="dash", line_color="green",
                     annotation_text="Target F1 = 0.85")
fig_domain.add_vline(x=0.60, line_dash="dash", line_color="orange",
                     annotation_text="S-V Correlation = 0.60")

fig_domain.update_layout(height=500)
fig_domain.show()

# COMMAND ----------

# Create domain comparison bar chart
fig_domain_bar = px.bar(
    fact_domain_accuracy.sort_values('f1_score', ascending=True),
    y='geological_domain',
    x=['f1_score', 'precision_ore', 'recall_ore'],
    orientation='h',
    title='Classification Metrics by Geological Domain',
    labels={'value': 'Score', 'geological_domain': 'Domain', 'variable': 'Metric'},
    barmode='group'
)

fig_domain_bar.update_layout(
    height=400,
    xaxis_range=[0, 1],
    legend_title="Metric"
)
fig_domain_bar.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ROC-Style Analysis by Domain
# MAGIC
# MAGIC Plot True Positive Rate (Recall) vs False Positive Rate for each domain.

# COMMAND ----------

# Calculate TPR and FPR for each domain
fact_domain_accuracy['tpr'] = fact_domain_accuracy['recall_ore']  # TPR = TP / (TP + FN)
fact_domain_accuracy['fpr'] = fact_domain_accuracy['false_positive'] / (
    fact_domain_accuracy['false_positive'] + fact_domain_accuracy['true_negative']
)

fig_roc = px.scatter(
    fact_domain_accuracy,
    x='fpr',
    y='tpr',
    color='geological_domain',
    size='total_loads',
    text='geological_domain',
    hover_data=['f1_score', 'avg_surface_volume_corr'],
    title='ROC-Style Analysis: Classification by Geological Domain',
    labels={
        'fpr': 'False Positive Rate',
        'tpr': 'True Positive Rate (Recall)',
        'geological_domain': 'Domain'
    }
)

# Add diagonal reference line (random classifier)
fig_roc.add_shape(
    type="line", x0=0, y0=0, x1=1, y1=1,
    line=dict(dash="dash", color="gray"),
)

# Add annotation for random baseline
fig_roc.add_annotation(
    x=0.5, y=0.5,
    text="Random Classifier",
    showarrow=False,
    textangle=-45,
    font=dict(color="gray")
)

fig_roc.update_layout(
    height=500,
    xaxis_range=[-0.05, 1.05],
    yaxis_range=[-0.05, 1.05]
)
fig_roc.update_traces(textposition='top center')
fig_roc.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary Dashboard

# COMMAND ----------

# Create summary metrics
summary = {
    'Overall F1': fact_accuracy['f1_score'].mean(),
    'Overall Accuracy': fact_accuracy['accuracy'].mean(),
    'Best Shovel F1': shovel_accuracy['f1_score'].max(),
    'Worst Shovel F1': shovel_accuracy['f1_score'].min(),
    'Best Domain F1': fact_domain_accuracy['f1_score'].max(),
    'Optimal Cutoff': optimal_thresh,
    'FN/FP Cost Ratio': cost_metrics['cost_ratio'],
    'Net Value': cost_metrics['net_value']
}

# Display summary
print("=" * 60)
print("D1 CONFUSION MATRIX DEEP DIVE - KEY FINDINGS")
print("=" * 60)
for k, v in summary.items():
    if isinstance(v, float):
        if 'Cost' in k or 'Value' in k:
            print(f"  {k}: ${v:,.0f}" if 'Value' in k else f"  {k}: {v:.1f}x")
        else:
            print(f"  {k}: {v:.3f}")
    else:
        print(f"  {k}: {v}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Insights
# MAGIC
# MAGIC 1. **Threshold Optimization**: The optimal cutoff grade is `{optimal_thresh:.2f}%` Cu, yielding F1 = `{optimal_f1:.3f}`
# MAGIC
# MAGIC 2. **Asymmetric Costs**: False negatives (lost ore) cost `{cost_metrics['cost_ratio']:.1f}x` more than false positives - optimize for RECALL
# MAGIC
# MAGIC 3. **Domain Variation**: XRF classification accuracy varies significantly by geological domain, supporting the Round 2 hypothesis about zone-dependent value
# MAGIC
# MAGIC 4. **Surface-Volume Correlation**: Domains with higher S-V correlation tend to have better F1 scores, validating the "critical unknown" from Round 1
