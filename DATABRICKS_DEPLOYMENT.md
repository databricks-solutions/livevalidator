# Databricks Asset Bundle Deployment

This directory contains a Databricks Asset Bundle (DAB) for the LiveValidator validation workflow.

## Structure

```
/
├── databricks.yml                  # Bundle configuration
├── resources/
│   └── validation_job.yml          # Job resource definition
└── jobs/
    └── run_validation.py           # Validation notebook
```

## Prerequisites

1. **Databricks CLI** installed and configured
   ```bash
   pip install databricks-cli
   databricks configure
   ```

2. **Databricks Secrets** configured:
   ```bash
   # Create secret scope
   databricks secrets create-scope livevalidator
   
   # Add secrets for each system connection
   # Example: for a system with user_secret_key="netezza_user" and pass_secret_key="netezza_pass"
   databricks secrets put-secret livevalidator netezza_user
   databricks secrets put-secret livevalidator netezza_pass
   ```

3. **Update workspace URLs** in `databricks.yml`:
   - Replace `dbc-your-workspace-id.cloud.databricks.com` with your actual workspace URL
   - Update backend API URLs for dev and prod targets

## Deployment

### Development
```bash
# Validate bundle configuration
databricks bundle validate -t dev

# Deploy to dev workspace
databricks bundle deploy -t dev

# Run manually (for testing)
databricks bundle run -t dev run_validation \
  --param name="test_validation" \
  --param source_system_id=1 \
  --param target_system_id=2 \
  --param source_table="schema.table" \
  --param target_table="schema.table" \
  --param compare_mode="except_all"
```

### Production
```bash
# Deploy to production workspace
databricks bundle deploy -t prod
```

## Job Parameters

The validation job accepts the following parameters:

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `trigger_id` | No | `""` | Trigger ID from control.triggers table |
| `name` | Yes | - | Validation name for display |
| `source_system_id` | Yes | - | ID of source system |
| `target_system_id` | Yes | - | ID of target system |
| `source_table` | No* | `""` | Source table (schema.table format) |
| `target_table` | No* | `""` | Target table (schema.table format) |
| `sql` | No* | `""` | SQL query (for query validation) |
| `compare_mode` | No | `except_all` | Comparison mode |
| `pk_columns` | No | `""` | JSON array of PK columns |
| `include_columns` | No | `""` | JSON array of columns to include |
| `exclude_columns` | No | `""` | JSON array of columns to exclude |
| `options` | No | `"{}"` | JSON object of additional options |

\* Either `source_table`/`target_table` OR `sql` must be provided.

## Integration with Backend

The validation job interacts with the backend API:

1. **Fetch system details**: `GET /api/systems/{id}` - Get connection info
2. **Report results**: `POST /api/validation-history` - Save validation results
3. **Report failures**: `PUT /api/triggers/{id}/fail` - Report job failures (if trigger_id provided)

## Validation Workflow

1. **Schema Validation**: Compares column names between source and target
2. **Row Count Validation**: Compares total row counts
3. **Row-level Validation**: 
   - Only runs if row counts match
   - Uses `EXCEPT ALL` to find differences
   - Respects `exclude_columns`
   - Returns sample of up to 100 different rows

## System Connection Types

### Databricks (Unity Catalog)
- Uses `catalog` field from system config
- Direct table/query access via Spark
- Format: `{catalog}.{schema}.{table}`

### Other Systems (JDBC)
- Uses `jdbc_string` from system config
- Credentials fetched from Databricks secrets using `user_secret_key` and `pass_secret_key`
- Supports: Netezza, Teradata, Oracle, Postgres, SQL Server, MySQL

## Troubleshooting

### Secrets not found
```
Error: Secret 'xyz' not found in scope 'livevalidator'
```
Solution: Create the secret in Databricks:
```bash
databricks secrets put-secret livevalidator xyz
```

### JDBC connection failed
- Verify `jdbc_string` format in `control.systems` table
- Ensure secrets are properly configured
- Check network connectivity from Databricks to target system

### Backend API unreachable
- Verify `backend_api_url` in `databricks.yml`
- Check that backend is running and accessible
- For local dev, ensure backend is started: `uvicorn backend.app:api --reload`
