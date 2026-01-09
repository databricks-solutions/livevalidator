# LiveValidator - DAB Template

A Databricks Asset Bundle template for deploying LiveValidator, a data validation platform for comparing tables and queries across heterogeneous database systems.

## Quick Start

### Prerequisites

- Databricks CLI installed and configured
- Lakehouse Apps enabled on your workspace
- Lakebase enabled on your workspace
- Elevated/admin privileges for deployment

### Initialize the Template

```bash
databricks bundle init https://github.com/databrickslabs/LiveValidator
```

You'll be prompted for:

| Prompt | Description | Example |
|--------|-------------|---------|
| Target name | Environment name for deployment | `dev`, `prod` |
| Workspace URL | Your Databricks workspace | `https://my-workspace.cloud.databricks.com/` |
| Cloud provider | AWS, Azure, or GCP | `aws` |
| Admin group | Group with CAN_MANAGE permissions | `data-platform-admins` |
| Your email | Databricks account email | `jane@company.com` |

### Deploy

```bash
cd LiveValidator
databricks bundle deploy -t <your-target>
databricks apps start live-validator -t <your-target>
```

### Complete Setup

1. Navigate to **Compute → Apps** in your workspace
2. Open the LiveValidator app URL
3. Follow the setup wizard to initialize the database
4. See the full [README](template/README.md) for detailed instructions

## What's Included

- **FastAPI Backend** - REST API for validation configuration and history
- **React Frontend** - Modern web UI built with Vite and Tailwind CSS
- **PostgreSQL Control Database** - Stores configuration and 7-day history
- **Databricks Spark Jobs** - Executes validation at scale

## Documentation

- [Full Documentation](template/README.md) - Complete setup and usage guide
- [Type Mappings](template/TYPE_MAPPINGS_FEATURE.md) - Custom type transformations
- [Queue System](template/QUEUE_AND_HISTORY_IMPLEMENTATION.md) - Architecture details
- [Deployment Guide](template/DATABRICKS_DEPLOYMENT.md) - Databricks-specific deployment

## License

MIT License - see [LICENSE](template/LICENSE) for details.
