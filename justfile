# ShovelSense Automated Smart Truck Diversion - Reproducible Data Pipeline
#
# Usage:
#   just setup              # Configure Databricks CLI and validate connection
#   just create-infra       # Create catalog, schema, and volumes
#   just generate-data      # Generate all synthetic data
#   just generate-pdfs      # Generate PDF documents for RAG
#   just deploy             # Deploy Databricks Asset Bundle
#   just all                # Run full pipeline

set shell := ["fish", "-c"]

# Configuration
export DATABRICKS_HOST := "https://fevm-cjc-aws-workspace.cloud.databricks.com"
export DATABRICKS_CONFIG_PROFILE := "fevm-cjc"
export CATALOG := "cjc_aws_workspace_catalog"
export SCHEMA := "shovelsense"
export VOLUME_PATH := "/Volumes/cjc_aws_workspace_catalog/shovelsense/raw_data"

# Default recipe
default:
    @just --list

# Validate Databricks connection
setup:
    @echo "Validating Databricks connection..."
    databricks auth describe --profile {{DATABRICKS_CONFIG_PROFILE}}
    @echo "Connection validated!"

# Create catalog, schema, and volumes in Databricks
create-infra:
    @echo "Creating infrastructure in Databricks..."
    databricks sql execute --profile {{DATABRICKS_CONFIG_PROFILE}} \
        --statement "CREATE CATALOG IF NOT EXISTS {{CATALOG}}"
    databricks sql execute --profile {{DATABRICKS_CONFIG_PROFILE}} \
        --statement "CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}"
    databricks sql execute --profile {{DATABRICKS_CONFIG_PROFILE}} \
        --statement "CREATE VOLUME IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}.raw_data"
    databricks sql execute --profile {{DATABRICKS_CONFIG_PROFILE}} \
        --statement "CREATE VOLUME IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}.pdf_documents"
    @echo "Infrastructure created!"

# Generate synthetic XRF and mining operations data
generate-data:
    @echo "Generating synthetic mining data..."
    $HOME/.virtualenvs/automated-smart-truck-diversion/bin/python scripts/generate_mining_data.py
    @echo "Data generation complete!"

# Upload generated data to Databricks Volume
upload-data:
    @echo "Uploading data to Databricks Volume..."
    for f in data/generated/*.parquet; databricks fs cp $f dbfs:{{VOLUME_PATH}}/(basename $f) --profile {{DATABRICKS_CONFIG_PROFILE}} --overwrite; end
    @echo "Upload complete!"

# Generate PDF documents for RAG testing
generate-pdfs:
    @echo "Generating PDF documents..."
    python scripts/generate_pdfs.py
    @echo "PDF generation complete!"

# Validate generated data (exit 0=pass, 1=errors, 2=warnings only)
validate-data:
    @echo "Validating generated data..."
    $HOME/.virtualenvs/automated-smart-truck-diversion/bin/python scripts/validate_data.py; or test $status -eq 2

# Run pytest tests
test:
    @echo "Running pytest..."
    $HOME/.virtualenvs/automated-smart-truck-diversion/bin/pytest tests/ -v

# Run all validation (validate-data + test)
validate-all:
    @echo "Running data validation..."
    $HOME/.virtualenvs/automated-smart-truck-diversion/bin/python scripts/validate_data.py; or test $status -eq 2
    @echo ""
    @echo "Running pytest..."
    $HOME/.virtualenvs/automated-smart-truck-diversion/bin/pytest tests/ -v
    @echo ""
    @echo "All validations complete!"

# Verify tables exist in Databricks
verify-tables:
    @echo "Verifying tables in {{CATALOG}}.{{SCHEMA}}..."
    databricks api post /api/2.0/sql/statements --profile {{DATABRICKS_CONFIG_PROFILE}} --json '{"warehouse_id": "751fe324525584e5", "statement": "SHOW TABLES IN {{CATALOG}}.{{SCHEMA}}", "wait_timeout": "30s"}' | python -c "import json,sys; d=json.load(sys.stdin); tables=[r[1] for r in d.get('result',{}).get('data_array',[])]; print(f'Found {len(tables)} tables:'); [print(f'  - {t}') for t in sorted(tables)]"

# Deploy Databricks Asset Bundle
deploy:
    @echo "Deploying Databricks Asset Bundle..."
    set -x DATABRICKS_TF_EXEC_PATH /tmp/terraform; set -x DATABRICKS_TF_VERSION 1.9.8; cd bundles && databricks bundle deploy --profile {{DATABRICKS_CONFIG_PROFILE}}

# Run deployed jobs
run-pipeline:
    @echo "Running data pipeline..."
    set -x DATABRICKS_TF_EXEC_PATH /tmp/terraform; set -x DATABRICKS_TF_VERSION 1.9.8; cd bundles && databricks bundle run shovelsense_pipeline --profile {{DATABRICKS_CONFIG_PROFILE}}

# Full pipeline: setup -> create-infra -> generate-data -> generate-pdfs -> deploy
all: setup create-infra generate-data generate-pdfs deploy
    @echo "Full pipeline complete!"

# Clean up generated local files (not Databricks data)
clean:
    @echo "Cleaning local artifacts..."
    rm -rf bundles/.databricks
    rm -rf __pycache__ scripts/__pycache__
    @echo "Clean complete!"

# Show current configuration
config:
    @echo "Current Configuration:"
    @echo "  DATABRICKS_HOST: {{DATABRICKS_HOST}}"
    @echo "  PROFILE: {{DATABRICKS_CONFIG_PROFILE}}"
    @echo "  CATALOG: {{CATALOG}}"
    @echo "  SCHEMA: {{SCHEMA}}"
    @echo "  VOLUME_PATH: {{VOLUME_PATH}}"

# List data in the volume
list-data:
    databricks fs ls {{VOLUME_PATH}} --profile {{DATABRICKS_CONFIG_PROFILE}}

# Drop all data (USE WITH CAUTION)
drop-data:
    @echo "WARNING: This will delete all data in {{CATALOG}}.{{SCHEMA}}"
    @read -P "Type 'yes' to confirm: " confirm; and test "$confirm" = "yes"; or exit 1
    databricks sql execute --profile {{DATABRICKS_CONFIG_PROFILE}} \
        --statement "DROP SCHEMA IF EXISTS {{CATALOG}}.{{SCHEMA}} CASCADE"
    @echo "Schema dropped!"
