# Databricks notebook source
# MAGIC %md
# MAGIC # D1: Diversion Value Analysis
# MAGIC
# MAGIC **Key Insight:** The value of XRF technology comes from DIVERSIONS - where XRF disagrees
# MAGIC with the block model. These aren't "errors" - they're opportunities to capture value.
# MAGIC
# MAGIC ## Value Proposition
# MAGIC
# MAGIC | Scenario | Block Model | XRF Says | Action | Value |
# MAGIC |----------|-------------|----------|--------|-------|
# MAGIC | **Ore Recovery** | WASTE | ORE | Divert to mill | Capture ore that would be lost |
# MAGIC | **Dilution Prevention** | ORE | WASTE | Divert to dump | Avoid processing waste |
# MAGIC | Aligned (Ore) | ORE | ORE | To mill | No change from baseline |
# MAGIC | Aligned (Waste) | WASTE | WASTE | To dump | No change from baseline |
# MAGIC
# MAGIC The block model is NOT ground truth - it's an estimate with its own errors.
# MAGIC XRF provides additional information that may be MORE accurate in some cases.

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

# Configuration - serverless compute doesn't support custom spark.conf keys
try:
    CATALOG = spark.conf.get("catalog")
except Exception:
    CATALOG = "cjc_aws_workspace_catalog"

try:
    SCHEMA = spark.conf.get("schema")
except Exception:
    SCHEMA = "shovelsense"

# Economic parameters from Round 3
COPPER_PRICE_PER_TONNE = 8820  # $/tonne Cu
METALLURGICAL_RECOVERY = 0.85
ORE_PROCESSING_COST = 15  # $/tonne
WASTE_DISPOSAL_COST = 3   # $/tonne

print(f"Catalog: {CATALOG}, Schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# Load truck loads with full context
fact_truck_loads = spark.table(f"{CATALOG}.{SCHEMA}.fact_truck_loads").toPandas()
fact_daily_diversions = spark.table(f"{CATALOG}.{SCHEMA}.fact_daily_diversions").toPandas()

print(f"Loaded {len(fact_truck_loads)} truck loads")
print(f"Loaded {len(fact_daily_diversions)} daily diversion records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Diversion Overview
# MAGIC
# MAGIC How often does XRF disagree with the block model? These are the value-creating events.

# COMMAND ----------

# Calculate diversion breakdown
diversion_counts = fact_truck_loads['diversion_type'].value_counts()
total_loads = len(fact_truck_loads)

diversion_summary = pd.DataFrame({
    'Type': ['Aligned (No Diversion)', 'Ore Recovery', 'Dilution Prevention'],
    'Count': [
        diversion_counts.get('ALIGNED', 0),
        diversion_counts.get('ORE_FROM_WASTE', 0),
        diversion_counts.get('WASTE_FROM_ORE', 0)
    ]
})
diversion_summary['Percentage'] = diversion_summary['Count'] / total_loads * 100
diversion_summary['Description'] = [
    'XRF agrees with block model',
    'XRF found ore in planned waste (VALUE CAPTURE)',
    'XRF found waste in planned ore (DILUTION PREVENTION)'
]

# Display summary
print("=" * 70)
print("DIVERSION SUMMARY")
print("=" * 70)
for _, row in diversion_summary.iterrows():
    print(f"{row['Type']:<25} {row['Count']:>8,} loads ({row['Percentage']:>5.1f}%)")
    print(f"  → {row['Description']}")
print("=" * 70)
print(f"Total Diversions: {diversion_summary[diversion_summary['Type'] != 'Aligned (No Diversion)']['Count'].sum():,} loads")
print(f"Diversion Rate: {(1 - diversion_counts.get('ALIGNED', 0)/total_loads)*100:.1f}%")

# COMMAND ----------

# Pie chart of diversion types
fig_pie = px.pie(
    diversion_summary,
    values='Count',
    names='Type',
    title='XRF Diversion Breakdown: Where Value is Created',
    color='Type',
    color_discrete_map={
        'Aligned (No Diversion)': '#95a5a6',
        'Ore Recovery': '#27ae60',
        'Dilution Prevention': '#3498db'
    },
    hole=0.4
)

fig_pie.update_traces(
    textinfo='percent+label',
    textposition='outside'
)

fig_pie.add_annotation(
    text=f"Total Diversions<br>{(1 - diversion_counts.get('ALIGNED', 0)/total_loads)*100:.1f}%",
    x=0.5, y=0.5,
    font_size=14,
    showarrow=False
)

fig_pie.update_layout(height=500)
fig_pie.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Economic Value of Diversions
# MAGIC
# MAGIC Each diversion has economic impact:
# MAGIC - **Ore Recovery**: Capturing ore that would have been dumped = recovered copper value
# MAGIC - **Dilution Prevention**: Avoiding waste in mill = saved processing costs + avoided grade dilution

# COMMAND ----------

def calculate_diversion_value(row):
    """Calculate the economic value/cost of a diversion."""
    payload = row['payload_tonnes']
    grade = row['avg_cu_grade_pct'] / 100  # Convert to fraction

    if row['diversion_type'] == 'ORE_FROM_WASTE':
        # Ore Recovery: Value of copper recovered
        # This ore would have gone to waste dump, now goes to mill
        cu_value = payload * grade * METALLURGICAL_RECOVERY * COPPER_PRICE_PER_TONNE
        # Net value = Cu value - processing cost (we weren't going to process it)
        return cu_value - (payload * ORE_PROCESSING_COST)

    elif row['diversion_type'] == 'WASTE_FROM_ORE':
        # Dilution Prevention: Saved processing cost + avoided dilution
        # This waste would have gone to mill, now goes to dump
        saved_processing = payload * (ORE_PROCESSING_COST - WASTE_DISPOSAL_COST)
        # Also: avoided diluting mill feed (harder to quantify, but real)
        return saved_processing

    else:
        return 0

# Calculate value for each load
fact_truck_loads['diversion_value'] = fact_truck_loads.apply(calculate_diversion_value, axis=1)

# Summarize by diversion type
value_by_type = fact_truck_loads.groupby('diversion_type').agg({
    'diversion_value': 'sum',
    'load_id': 'count',
    'payload_tonnes': 'sum',
    'avg_cu_grade_pct': 'mean'
}).rename(columns={'load_id': 'count'})

value_by_type['value_per_load'] = value_by_type['diversion_value'] / value_by_type['count']

print("=" * 70)
print("ECONOMIC VALUE BY DIVERSION TYPE")
print("=" * 70)
for dtype, row in value_by_type.iterrows():
    if dtype != 'ALIGNED':
        print(f"\n{dtype}:")
        print(f"  Total Value:     ${row['diversion_value']:>15,.0f}")
        print(f"  Load Count:      {row['count']:>15,}")
        print(f"  Value per Load:  ${row['value_per_load']:>15,.0f}")
        print(f"  Avg Grade:       {row['avg_cu_grade_pct']:>15.2f}%")

total_diversion_value = value_by_type[value_by_type.index != 'ALIGNED']['diversion_value'].sum()
print(f"\n{'='*70}")
print(f"TOTAL DIVERSION VALUE: ${total_diversion_value:,.0f}")
print("=" * 70)

# COMMAND ----------

# Visualize diversion value
fig_value = go.Figure()

# Ore Recovery value
ore_recovery_value = value_by_type.loc['ORE_FROM_WASTE', 'diversion_value'] if 'ORE_FROM_WASTE' in value_by_type.index else 0
dilution_prevention_value = value_by_type.loc['WASTE_FROM_ORE', 'diversion_value'] if 'WASTE_FROM_ORE' in value_by_type.index else 0

fig_value.add_trace(go.Bar(
    x=['Ore Recovery<br>(Found ore in waste)', 'Dilution Prevention<br>(Found waste in ore)'],
    y=[ore_recovery_value / 1e6, dilution_prevention_value / 1e6],
    marker_color=['#27ae60', '#3498db'],
    text=[f'${ore_recovery_value/1e6:.2f}M', f'${dilution_prevention_value/1e6:.2f}M'],
    textposition='outside'
))

fig_value.update_layout(
    title=f'Economic Value of XRF Diversions | Total: ${total_diversion_value/1e6:.2f}M',
    yaxis_title='Value ($M)',
    height=400,
    showlegend=False
)

fig_value.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Diversion Value by Geological Domain
# MAGIC
# MAGIC Does XRF add more value in certain zones? This tells us where to deploy the technology.

# COMMAND ----------

# Aggregate by geological domain
domain_value = fact_truck_loads.groupby('geological_domain').agg({
    'diversion_value': 'sum',
    'load_id': 'count',
    'is_diverted': 'sum',
    'payload_tonnes': 'sum',
    'surface_volume_correlation': 'mean'
}).rename(columns={'load_id': 'total_loads', 'is_diverted': 'diverted_loads'})

domain_value['diversion_rate'] = domain_value['diverted_loads'] / domain_value['total_loads']
domain_value['value_per_tonne'] = domain_value['diversion_value'] / domain_value['payload_tonnes']
domain_value = domain_value.reset_index()

# Plot
fig_domain = px.scatter(
    domain_value,
    x='surface_volume_correlation',
    y='value_per_tonne',
    size='total_loads',
    color='geological_domain',
    hover_data=['diversion_rate', 'diversion_value'],
    title='Diversion Value by Geological Domain',
    labels={
        'surface_volume_correlation': 'Avg Surface-Volume Correlation',
        'value_per_tonne': 'Value per Tonne ($)',
        'geological_domain': 'Domain'
    }
)

fig_domain.update_layout(height=500)
fig_domain.show()

# COMMAND ----------

# Bar chart ranking domains by value
fig_domain_bar = px.bar(
    domain_value.sort_values('diversion_value', ascending=True),
    y='geological_domain',
    x='diversion_value',
    orientation='h',
    title='Total Diversion Value by Geological Domain',
    labels={'diversion_value': 'Total Value ($)', 'geological_domain': 'Domain'},
    color='diversion_rate',
    color_continuous_scale='Greens'
)

fig_domain_bar.update_layout(height=400)
fig_domain_bar.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Diversion Trends Over Time
# MAGIC
# MAGIC Is diversion value consistent, or does it vary by day/shift?

# COMMAND ----------

# Daily diversion value
daily_value = fact_truck_loads.groupby('load_date').agg({
    'diversion_value': 'sum',
    'load_id': 'count',
    'is_diverted': 'sum'
}).rename(columns={'load_id': 'total_loads', 'is_diverted': 'diverted_loads'})

daily_value['diversion_rate'] = daily_value['diverted_loads'] / daily_value['total_loads']
daily_value = daily_value.reset_index()

fig_trend = make_subplots(
    rows=2, cols=1,
    subplot_titles=('Daily Diversion Value', 'Daily Diversion Rate'),
    vertical_spacing=0.15
)

# Value trend
fig_trend.add_trace(
    go.Scatter(
        x=daily_value['load_date'],
        y=daily_value['diversion_value'] / 1000,
        mode='lines',
        name='Daily Value ($K)',
        line=dict(color='#27ae60', width=2)
    ),
    row=1, col=1
)

# Add rolling average
daily_value['value_ma7'] = daily_value['diversion_value'].rolling(7, min_periods=1).mean()
fig_trend.add_trace(
    go.Scatter(
        x=daily_value['load_date'],
        y=daily_value['value_ma7'] / 1000,
        mode='lines',
        name='7-day Moving Avg',
        line=dict(color='#2c3e50', width=2, dash='dash')
    ),
    row=1, col=1
)

# Rate trend
fig_trend.add_trace(
    go.Scatter(
        x=daily_value['load_date'],
        y=daily_value['diversion_rate'] * 100,
        mode='lines',
        name='Diversion Rate (%)',
        line=dict(color='#3498db', width=2)
    ),
    row=2, col=1
)

# Target rate from white paper (~11%)
fig_trend.add_hline(y=11, line_dash='dash', line_color='red',
                    annotation_text='Target: 11%', row=2, col=1)

fig_trend.update_layout(
    height=600,
    title_text='Diversion Performance Over Time',
    showlegend=True
)

fig_trend.update_yaxes(title_text='Value ($K)', row=1, col=1)
fig_trend.update_yaxes(title_text='Diversion Rate (%)', row=2, col=1)

fig_trend.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Diversion Quality Analysis
# MAGIC
# MAGIC Are the diversions good calls? We can estimate this by looking at the grades:
# MAGIC - **Good Ore Recovery**: High grade in the "found" ore (worth diverting)
# MAGIC - **Good Dilution Prevention**: Low grade in the "rejected" waste (correct to reject)

# COMMAND ----------

# Analyze grade distribution by diversion type
ore_recovery = fact_truck_loads[fact_truck_loads['diversion_type'] == 'ORE_FROM_WASTE']
dilution_prevention = fact_truck_loads[fact_truck_loads['diversion_type'] == 'WASTE_FROM_ORE']
aligned_ore = fact_truck_loads[(fact_truck_loads['diversion_type'] == 'ALIGNED') &
                                (fact_truck_loads['planned_classification'] == 'ORE')]
aligned_waste = fact_truck_loads[(fact_truck_loads['diversion_type'] == 'ALIGNED') &
                                  (fact_truck_loads['planned_classification'] == 'WASTE')]

fig_grades = go.Figure()

# Add histograms
fig_grades.add_trace(go.Histogram(
    x=ore_recovery['avg_cu_grade_pct'],
    name='Ore Recovery (found ore in waste)',
    marker_color='#27ae60',
    opacity=0.7,
    nbinsx=30
))

fig_grades.add_trace(go.Histogram(
    x=dilution_prevention['avg_cu_grade_pct'],
    name='Dilution Prevention (found waste in ore)',
    marker_color='#3498db',
    opacity=0.7,
    nbinsx=30
))

fig_grades.add_trace(go.Histogram(
    x=aligned_ore['avg_cu_grade_pct'],
    name='Aligned Ore (both say ore)',
    marker_color='#95a5a6',
    opacity=0.5,
    nbinsx=30
))

# Add cutoff line
fig_grades.add_vline(x=0.32, line_dash='dash', line_color='red',
                     annotation_text='Cutoff: 0.32%')

fig_grades.update_layout(
    title='Grade Distribution by Diversion Type',
    xaxis_title='XRF Measured Grade (%Cu)',
    yaxis_title='Count',
    barmode='overlay',
    height=500
)

fig_grades.show()

# COMMAND ----------

# Grade statistics by diversion type
print("=" * 70)
print("GRADE STATISTICS BY DIVERSION TYPE")
print("=" * 70)

for name, df in [
    ('Ore Recovery (ORE_FROM_WASTE)', ore_recovery),
    ('Dilution Prevention (WASTE_FROM_ORE)', dilution_prevention),
    ('Aligned Ore', aligned_ore),
    ('Aligned Waste', aligned_waste)
]:
    if len(df) > 0:
        print(f"\n{name}:")
        print(f"  Count:       {len(df):>10,}")
        print(f"  Mean Grade:  {df['avg_cu_grade_pct'].mean():>10.3f}%")
        print(f"  Median:      {df['avg_cu_grade_pct'].median():>10.3f}%")
        print(f"  Std Dev:     {df['avg_cu_grade_pct'].std():>10.3f}%")
        print(f"  Min-Max:     {df['avg_cu_grade_pct'].min():.3f}% - {df['avg_cu_grade_pct'].max():.3f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Shovel-Level Diversion Performance
# MAGIC
# MAGIC Which shovels are generating the most diversion value?

# COMMAND ----------

shovel_value = fact_truck_loads.groupby('shovel_id').agg({
    'diversion_value': 'sum',
    'load_id': 'count',
    'is_diverted': 'sum',
    'is_ore_recovery': 'sum',
    'is_dilution_prevention': 'sum',
    'avg_xrf_confidence': 'mean'
}).rename(columns={
    'load_id': 'total_loads',
    'is_diverted': 'total_diversions',
    'is_ore_recovery': 'ore_recoveries',
    'is_dilution_prevention': 'dilution_preventions'
})

shovel_value['diversion_rate'] = shovel_value['total_diversions'] / shovel_value['total_loads']
shovel_value['value_per_load'] = shovel_value['diversion_value'] / shovel_value['total_loads']
shovel_value = shovel_value.reset_index()

fig_shovel = px.bar(
    shovel_value.sort_values('diversion_value', ascending=True),
    y='shovel_id',
    x='diversion_value',
    orientation='h',
    title='Diversion Value by Shovel',
    labels={'diversion_value': 'Total Value ($)', 'shovel_id': 'Shovel'},
    color='diversion_rate',
    color_continuous_scale='Viridis',
    hover_data=['total_loads', 'ore_recoveries', 'dilution_preventions']
)

fig_shovel.update_layout(height=400)
fig_shovel.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary Dashboard

# COMMAND ----------

# Key metrics
total_loads = len(fact_truck_loads)
total_diversions = fact_truck_loads['is_diverted'].sum()
ore_recoveries = fact_truck_loads['is_ore_recovery'].sum()
dilution_preventions = fact_truck_loads['is_dilution_prevention'].sum()
total_value = fact_truck_loads['diversion_value'].sum()

avg_ore_recovery_grade = ore_recovery['avg_cu_grade_pct'].mean() if len(ore_recovery) > 0 else 0
avg_dilution_prevention_grade = dilution_prevention['avg_cu_grade_pct'].mean() if len(dilution_prevention) > 0 else 0

print("=" * 70)
print("DIVERSION VALUE ANALYSIS - EXECUTIVE SUMMARY")
print("=" * 70)
print(f"\nOPERATIONAL METRICS")
print(f"  Total Truck Loads:        {total_loads:>12,}")
print(f"  Total Diversions:         {total_diversions:>12,} ({total_diversions/total_loads*100:.1f}%)")
print(f"    - Ore Recoveries:       {ore_recoveries:>12,} ({ore_recoveries/total_loads*100:.1f}%)")
print(f"    - Dilution Preventions: {dilution_preventions:>12,} ({dilution_preventions/total_loads*100:.1f}%)")

print(f"\nECONOMIC VALUE")
print(f"  Total Diversion Value:    ${total_value:>12,.0f}")
print(f"  Value per Diversion:      ${total_value/total_diversions:>12,.0f}")
print(f"  Value per Load:           ${total_value/total_loads:>12,.2f}")

print(f"\nQUALITY INDICATORS")
print(f"  Avg Ore Recovery Grade:   {avg_ore_recovery_grade:>12.2f}% Cu")
print(f"  Avg Dilution Prev Grade:  {avg_dilution_prevention_grade:>12.2f}% Cu")
print(f"  Grade Cutoff:             {0.32:>12.2f}% Cu")

print("=" * 70)
print("\nKEY INSIGHT: Diversions are not errors - they are value creation events.")
print("The XRF technology's value comes from disagreeing with the block model")
print("when the block model is wrong.")
print("=" * 70)
