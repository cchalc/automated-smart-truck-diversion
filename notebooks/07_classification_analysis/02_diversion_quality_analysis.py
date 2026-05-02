# Databricks notebook source
# MAGIC %md
# MAGIC # D1.2: Diversion Quality Analysis
# MAGIC
# MAGIC **Question:** Are the diversions good calls?
# MAGIC
# MAGIC Since we don't have mill assay data, we use XRF grade as a proxy:
# MAGIC - **Good Ore Recovery**: XRF grade significantly above cutoff → worth recovering
# MAGIC - **Marginal Ore Recovery**: XRF grade barely above cutoff → questionable value
# MAGIC - **Good Dilution Prevention**: XRF grade well below cutoff → correct rejection
# MAGIC - **Marginal Dilution Prevention**: XRF grade near cutoff → may have lost ore

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

# Configuration
try:
    CATALOG = spark.conf.get("catalog")
except Exception:
    CATALOG = "cjc_aws_workspace_catalog"

try:
    SCHEMA = spark.conf.get("schema")
except Exception:
    SCHEMA = "shovelsense"

# Cutoff grade
CUTOFF = 0.32

# Quality thresholds (distance from cutoff in %Cu)
HIGH_CONFIDENCE_MARGIN = 0.10  # ±0.10% from cutoff = marginal zone
STRONG_SIGNAL_MARGIN = 0.20   # ±0.20% from cutoff = strong signal

print(f"Catalog: {CATALOG}, Schema: {SCHEMA}")
print(f"Cutoff: {CUTOFF}%, Marginal zone: ±{HIGH_CONFIDENCE_MARGIN}%")

# COMMAND ----------

# Load data
fact_truck_loads = spark.table(f"{CATALOG}.{SCHEMA}.fact_truck_loads").toPandas()
print(f"Loaded {len(fact_truck_loads)} truck loads")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Diversion Confidence Classification
# MAGIC
# MAGIC Classify each diversion by how confident we are in the call.

# COMMAND ----------

def classify_diversion_confidence(row):
    """
    Classify diversion confidence based on distance from cutoff.

    Returns:
        str: Confidence level (HIGH_CONFIDENCE, MARGINAL, NOT_DIVERTED)
    """
    grade = row['avg_cu_grade_pct']
    diversion_type = row['diversion_type']

    if diversion_type == 'ALIGNED':
        return 'NOT_DIVERTED'

    distance_from_cutoff = abs(grade - CUTOFF)

    if diversion_type == 'ORE_FROM_WASTE':
        # Ore recovery: higher grade = more confident
        if grade >= CUTOFF + STRONG_SIGNAL_MARGIN:
            return 'HIGH_CONFIDENCE'
        elif grade >= CUTOFF + HIGH_CONFIDENCE_MARGIN:
            return 'MODERATE_CONFIDENCE'
        else:
            return 'MARGINAL'

    elif diversion_type == 'WASTE_FROM_ORE':
        # Dilution prevention: lower grade = more confident
        if grade <= CUTOFF - STRONG_SIGNAL_MARGIN:
            return 'HIGH_CONFIDENCE'
        elif grade <= CUTOFF - HIGH_CONFIDENCE_MARGIN:
            return 'MODERATE_CONFIDENCE'
        else:
            return 'MARGINAL'

    return 'UNKNOWN'

fact_truck_loads['diversion_confidence'] = fact_truck_loads.apply(classify_diversion_confidence, axis=1)

# Summary
confidence_summary = fact_truck_loads[fact_truck_loads['diversion_type'] != 'ALIGNED'].groupby(
    ['diversion_type', 'diversion_confidence']
).size().unstack(fill_value=0)

print("DIVERSION CONFIDENCE BREAKDOWN")
print("=" * 60)
print(confidence_summary)
print("=" * 60)

# COMMAND ----------

# Visualize confidence distribution
diversions_only = fact_truck_loads[fact_truck_loads['diversion_type'] != 'ALIGNED']

fig_conf = px.histogram(
    diversions_only,
    x='avg_cu_grade_pct',
    color='diversion_type',
    facet_row='diversion_type',
    nbins=50,
    title='Diversion Grade Distribution with Confidence Zones',
    labels={'avg_cu_grade_pct': 'XRF Grade (%Cu)'}
)

# Add confidence zone shading
for i in range(2):
    # Marginal zone (gray)
    fig_conf.add_vrect(
        x0=CUTOFF - HIGH_CONFIDENCE_MARGIN,
        x1=CUTOFF + HIGH_CONFIDENCE_MARGIN,
        fillcolor='rgba(128,128,128,0.2)',
        layer='below',
        line_width=0,
        row=i+1, col=1
    )

# Add cutoff line
fig_conf.add_vline(x=CUTOFF, line_dash='dash', line_color='red',
                   annotation_text=f'Cutoff: {CUTOFF}%')

fig_conf.update_layout(height=600)
fig_conf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Value by Confidence Level
# MAGIC
# MAGIC How much value comes from high-confidence vs marginal diversions?

# COMMAND ----------

# Calculate value by confidence
value_by_confidence = diversions_only.groupby(['diversion_type', 'diversion_confidence']).agg({
    'diversion_value': 'sum',
    'load_id': 'count',
    'avg_cu_grade_pct': 'mean'
}).rename(columns={'load_id': 'count'}).reset_index()

# Create stacked bar
fig_value_conf = px.bar(
    value_by_confidence,
    x='diversion_type',
    y='diversion_value',
    color='diversion_confidence',
    title='Diversion Value by Confidence Level',
    labels={'diversion_value': 'Total Value ($)', 'diversion_type': 'Diversion Type'},
    color_discrete_map={
        'HIGH_CONFIDENCE': '#27ae60',
        'MODERATE_CONFIDENCE': '#f39c12',
        'MARGINAL': '#e74c3c'
    },
    text='diversion_value'
)

fig_value_conf.update_traces(texttemplate='$%{text:,.0f}', textposition='inside')
fig_value_conf.update_layout(height=450)
fig_value_conf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Grade Accuracy Analysis
# MAGIC
# MAGIC Compare XRF grade to block model grade to understand measurement agreement.

# COMMAND ----------

# Join with block model to get planned grades
truck_with_planned = spark.table(f"{CATALOG}.{SCHEMA}.fact_truck_loads").join(
    spark.table(f"{CATALOG}.{SCHEMA}.dim_block_model").select("block_id", "planned_cu_grade"),
    "block_id"
).toPandas()

# Calculate grade difference
truck_with_planned['grade_diff'] = truck_with_planned['avg_cu_grade_pct'] - truck_with_planned['planned_cu_grade']

# Scatter plot: XRF vs Block Model grade
fig_scatter = px.scatter(
    truck_with_planned.sample(n=min(5000, len(truck_with_planned))),  # Sample for performance
    x='planned_cu_grade',
    y='avg_cu_grade_pct',
    color='diversion_type',
    opacity=0.5,
    title='XRF Grade vs Block Model Grade',
    labels={
        'planned_cu_grade': 'Block Model Grade (%Cu)',
        'avg_cu_grade_pct': 'XRF Measured Grade (%Cu)'
    },
    color_discrete_map={
        'ALIGNED': '#95a5a6',
        'ORE_FROM_WASTE': '#27ae60',
        'WASTE_FROM_ORE': '#3498db'
    }
)

# Add perfect agreement line
fig_scatter.add_trace(go.Scatter(
    x=[0, 1.5], y=[0, 1.5],
    mode='lines',
    name='Perfect Agreement',
    line=dict(color='gray', dash='dash')
))

# Add cutoff lines
fig_scatter.add_hline(y=CUTOFF, line_dash='dot', line_color='red',
                      annotation_text='XRF Cutoff')
fig_scatter.add_vline(x=CUTOFF, line_dash='dot', line_color='red',
                      annotation_text='Block Model Cutoff')

fig_scatter.update_layout(height=600, width=700)
fig_scatter.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quadrant Analysis
# MAGIC
# MAGIC The scatter plot divides into 4 quadrants:
# MAGIC - **Top-Right (ORE/ORE)**: Both agree it's ore → Aligned
# MAGIC - **Bottom-Left (WASTE/WASTE)**: Both agree it's waste → Aligned
# MAGIC - **Top-Left (ORE/WASTE)**: XRF says ore, Block says waste → **Ore Recovery opportunity**
# MAGIC - **Bottom-Right (WASTE/ORE)**: XRF says waste, Block says ore → **Dilution Prevention opportunity**

# COMMAND ----------

# Quadrant counts
quadrants = {
    'Top-Right (Both Ore)': len(truck_with_planned[
        (truck_with_planned['avg_cu_grade_pct'] >= CUTOFF) &
        (truck_with_planned['planned_cu_grade'] >= CUTOFF)
    ]),
    'Bottom-Left (Both Waste)': len(truck_with_planned[
        (truck_with_planned['avg_cu_grade_pct'] < CUTOFF) &
        (truck_with_planned['planned_cu_grade'] < CUTOFF)
    ]),
    'Top-Left (Ore Recovery)': len(truck_with_planned[
        (truck_with_planned['avg_cu_grade_pct'] >= CUTOFF) &
        (truck_with_planned['planned_cu_grade'] < CUTOFF)
    ]),
    'Bottom-Right (Dilution Prev)': len(truck_with_planned[
        (truck_with_planned['avg_cu_grade_pct'] < CUTOFF) &
        (truck_with_planned['planned_cu_grade'] >= CUTOFF)
    ])
}

total = sum(quadrants.values())

print("QUADRANT ANALYSIS")
print("=" * 50)
for quadrant, count in quadrants.items():
    print(f"  {quadrant}: {count:,} ({count/total*100:.1f}%)")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. XRF Confidence Impact
# MAGIC
# MAGIC Does XRF sensor confidence correlate with diversion quality?

# COMMAND ----------

# Bin by XRF confidence
diversions_only['confidence_bin'] = pd.cut(
    diversions_only['avg_xrf_confidence'],
    bins=[0, 0.8, 0.9, 0.95, 1.0],
    labels=['<80%', '80-90%', '90-95%', '>95%']
)

conf_analysis = diversions_only.groupby('confidence_bin').agg({
    'diversion_value': 'sum',
    'load_id': 'count',
    'avg_cu_grade_pct': ['mean', 'std']
}).round(3)

conf_analysis.columns = ['_'.join(col).strip('_') for col in conf_analysis.columns]
conf_analysis = conf_analysis.reset_index()

print("DIVERSION VALUE BY XRF CONFIDENCE")
print("=" * 60)
print(conf_analysis.to_string(index=False))

# COMMAND ----------

fig_xrf_conf = px.bar(
    conf_analysis,
    x='confidence_bin',
    y='diversion_value_sum',
    title='Diversion Value by XRF Sensor Confidence',
    labels={
        'confidence_bin': 'XRF Confidence Level',
        'diversion_value_sum': 'Total Diversion Value ($)'
    },
    color='avg_cu_grade_pct_std',
    color_continuous_scale='RdYlGn_r'  # Red = high variance, Green = low variance
)

fig_xrf_conf.update_layout(height=400)
fig_xrf_conf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Surface-Volume Correlation Impact
# MAGIC
# MAGIC From Round 1: S-V correlation is "the critical unknown."
# MAGIC Does higher S-V correlation lead to better diversion outcomes?

# COMMAND ----------

# Bin by S-V correlation
diversions_only['sv_bin'] = pd.cut(
    diversions_only['surface_volume_correlation'],
    bins=[0, 0.4, 0.55, 0.7, 1.0],
    labels=['Low (<0.4)', 'Marginal (0.4-0.55)', 'Moderate (0.55-0.7)', 'High (>0.7)']
)

sv_analysis = diversions_only.groupby('sv_bin').agg({
    'diversion_value': 'sum',
    'load_id': 'count',
    'diversion_confidence': lambda x: (x == 'HIGH_CONFIDENCE').sum() / len(x) * 100
}).round(2)

sv_analysis.columns = ['total_value', 'diversion_count', 'high_confidence_pct']
sv_analysis = sv_analysis.reset_index()

fig_sv = make_subplots(
    rows=1, cols=2,
    subplot_titles=('Diversion Value by S-V Correlation', 'High-Confidence Diversion Rate')
)

fig_sv.add_trace(
    go.Bar(
        x=sv_analysis['sv_bin'],
        y=sv_analysis['total_value'],
        marker_color='#3498db',
        name='Value'
    ),
    row=1, col=1
)

fig_sv.add_trace(
    go.Bar(
        x=sv_analysis['sv_bin'],
        y=sv_analysis['high_confidence_pct'],
        marker_color='#27ae60',
        name='High Confidence %'
    ),
    row=1, col=2
)

fig_sv.update_layout(height=400, title_text='Surface-Volume Correlation Impact on Diversion Quality')
fig_sv.update_yaxes(title_text='Value ($)', row=1, col=1)
fig_sv.update_yaxes(title_text='High Confidence %', row=1, col=2)

fig_sv.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

# Calculate key metrics
total_diversions = len(diversions_only)
high_conf_diversions = len(diversions_only[diversions_only['diversion_confidence'] == 'HIGH_CONFIDENCE'])
marginal_diversions = len(diversions_only[diversions_only['diversion_confidence'] == 'MARGINAL'])

high_conf_value = diversions_only[diversions_only['diversion_confidence'] == 'HIGH_CONFIDENCE']['diversion_value'].sum()
marginal_value = diversions_only[diversions_only['diversion_confidence'] == 'MARGINAL']['diversion_value'].sum()
total_value = diversions_only['diversion_value'].sum()

print("=" * 70)
print("DIVERSION QUALITY ANALYSIS - SUMMARY")
print("=" * 70)

print(f"\nDIVERSION CONFIDENCE BREAKDOWN:")
print(f"  Total Diversions:       {total_diversions:>10,}")
print(f"  High Confidence:        {high_conf_diversions:>10,} ({high_conf_diversions/total_diversions*100:.1f}%)")
print(f"  Moderate Confidence:    {total_diversions - high_conf_diversions - marginal_diversions:>10,}")
print(f"  Marginal (near cutoff): {marginal_diversions:>10,} ({marginal_diversions/total_diversions*100:.1f}%)")

print(f"\nVALUE BY CONFIDENCE:")
print(f"  Total Diversion Value:    ${total_value:>12,.0f}")
print(f"  High Confidence Value:    ${high_conf_value:>12,.0f} ({high_conf_value/total_value*100:.1f}%)")
print(f"  Marginal Value:           ${marginal_value:>12,.0f} ({marginal_value/total_value*100:.1f}%)")

print(f"\nKEY INSIGHTS:")
print(f"  • {high_conf_diversions/total_diversions*100:.0f}% of diversions are high-confidence (grade far from cutoff)")
print(f"  • {marginal_diversions/total_diversions*100:.0f}% are marginal (grade near cutoff) - these need scrutiny")
print(f"  • Higher S-V correlation → higher proportion of high-confidence diversions")

print("=" * 70)
