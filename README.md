# <img src="src/app/frontend/favicon.ico" alt="LiveValidator" width="32" height="32" style="vertical-align: middle;"/> LiveValidator

## Table of Contents

- [Overview](#overview)
  - [What It Does](#what-it-does)
  - [Arch Overview](#arch-overview)
  - [Key Capabilities](#key-capabilities)
  - [Who It's For](#who-its-for)
- [Quick Start](#-quick-start)
  - [Prerequisites](#prerequisites)
  - [Setup](#setup)
- [Architecture](#️-architecture)
  - [Component Overview](#component-overview)
  - [Data Flow](#data-flow)
  - [File Structure](#file-structure)
- [Environment Variables](#-environment-variables)
- [Features](#-features)
  - [Setting Up](#setting-up)
  - [Configuring Validations](#configuring-validations)
  - [Running Validations](#running-validations)
  - [Monitoring & Results](#monitoring--results)
- [Advanced Configuration](#-advanced-configuration)
  - [Type Transformation Functions](#type-transformation-functions)
  - [Unicode Normalization Options](#unicode-normalization-options)
  - [System Configuration](#system-configuration)
  - [Schedule Bindings](#schedule-bindings)
- [Additional Documentation](#-additional-documentation)
- [Security Notes](#-security-notes)
- [Development](#️-development)
- [Contributing](#-contributing)
- [License](#-license)
- [Updating](#-updating)
- [Troubleshooting](#-troubleshooting)

---

## Overview

**LiveValidator** is a data validation platform designed to ensure data integrity across heterogeneous database systems. It automates the comparison of tables and query results between source and target databases, detecting schema mismatches, row count discrepancies, and row-level differences.

### What It Does

LiveValidator performs three-tier validation between any two database systems:

1. **Schema Validation** - Compares column names and identifies missing or extra columns
2. **Row Count Validation** - Compares total row counts between source and target
3. **Row-Level Validation** - Detects actual data differences using either set-based comparison (EXCEPT ALL), or Primary Key.

When differences are found, LiveValidator captures sample records and provides detailed reports through a modern web interface.

### Arch Overview

![Architecture](docs/images/LiveValidator_arch_color.png)

**Workflow:**
1. **Configure** systems, tables, and queries through the web UI, bulk uploads supported
2. **Schedule** automated validations or trigger them manually
3. **Queue** manages job execution on Databricks Spark clusters
4. **Execute** validation logic compares data between systems
5. **Review** results in the validation history with filtering and tagging

### Key Capabilities

- **Snappy LakeBase Backend**: Quickly iterate in the web UI with a polished, modern user experience.
- **Tagging**: Organize your table and query comparisons with tags and use them to do bulk actions.
- **Multi-Database Support**: Databricks, Netezza, Teradata, SQL Server, MySQL, Postgres, Snowflake, or custom JDBC sources
- **Custom Type Transformations**: Handle data type differences with custom Python functions per system pair
- **Primary Key Tracking**: Optionally configure primary key columns for proper row identification and tracking
- **Smart Comparison**: Unicode normalization, special character handling, and configurable column filtering
- **Scheduling**: Cron-based automation with priority queue management
- **History & Analytics**: 7-day UI retention with tags, filters, and drill-down capabilities

### Who It's For

- **Data Engineers** validating data migrations and replication pipelines
- **QA Teams** ensuring data quality across environments
- **Analytics Teams** verifying data consistency for reporting
- **Platform Teams** monitoring ongoing data synchronization

## 🚀 Quick Start

### Prerequisites
- Clone this repository: `git clone https://github.com/databricks-field-eng/livevalidator`
- Databricks CLI installed and configured
- Lakehouse Apps enabled on your workspace
- Lakebase enabled on your workspace
- Elevated/admin privileges for the deployer

### Setup

---

#### Step 1: Configure Your Environment

```bash
# Copy the example config
cp databricks.yml.example databricks.yml
```

Edit `databricks.yml` with your settings:
- **Workspace host URL** - your Databricks workspace (e.g., `https://my-workspace.cloud.databricks.com/`)
- **Admin group name** - group that will have CAN_MANAGE permissions
- **Target name** - environment name (e.g., `dev`, `prod`)

> **Note:** `databricks.yml` is gitignored - your config stays local and won't conflict with updates.

---

#### Step 2: Deploy DAB

```bash
databricks bundle deploy -t <your-target>
```

> **Note:** This may take 10+ minutes as it spins up Databricks App, LakeBase, etc.

---

#### Step 3: Start App

```bash
# start the app's compute
databricks apps start live-validator -t <your-target>

# deploy the app
databricks apps deploy live-validator --source-code-path /Workspace/LiveValidator/files/src/app -t <your-target>
```

Or simply navigate to **Compute → Apps** and start it from the UI.

---

#### Step 4: Navigate to the App

Inspect the app configuration page under **Compute → Apps**:

![Compute Apps](docs/images/find-apps.png)

Verify the app is running, then open the URL:

```
https://live-validator-<workspace-id>.aws.databricksapps.com/
```

On first launch, you'll see a banner prompting you to go to the Setup page.

---

#### Step 5: Setup Database

> **Important:** Must be run by the LakeBase owner (whoever deployed the DAB).

Click **"Go to Setup →"** to proceed with database initialization:

![Database Not Initialized Banner](docs/images/setup-banner.png)

Open **Databricks SQL Editor** attached to the `live-validator` Postgres compute and run:

```sql
CREATE USER apprunner WITH PASSWORD 'beepboop123';

CREATE SCHEMA IF NOT EXISTS control;
GRANT USAGE ON SCHEMA control to apprunner;
GRANT apprunner TO CURRENT_USER;
ALTER SCHEMA control OWNER TO apprunner;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA control TO apprunner;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA control TO apprunner;

ALTER DEFAULT PRIVILEGES IN SCHEMA control 
  GRANT USAGE, SELECT ON SEQUENCES TO apprunner;
```

![Init database in SQL Editor](docs/images/init-database-1.png)

---

#### Step 6: Initialize Metadata Tables

Click the button to initialize the tables:

<img src="docs/images/init-database-2.png" alt="Initialize the tables" width="500"/>

Then:

1. **Hard refresh** your browser: `Cmd+Shift+R` (Mac) or `Ctrl+Shift+R` (Windows)
2. Navigate back to the **Setup** tab (bottom-left sidebar)

<img src="docs/images/return-to-setup.png" alt="Return to setup" width="300"/>

---

#### Step 7: Start the Job Sentinel

This background job processes the validation queue:

```bash
databricks bundle run job_sentinel --no-wait -t <your-target>
```

---

#### Step 8: Run Your First Validation

You're all set! Here's an overview of next steps:

1. **Create Systems** (Systems view)
   - Add your source database (e.g., Netezza production)
   - Add your target database (e.g., Databricks lakehouse)
   - Test connections

2. **Configure Type Transformations** (Type Mappings view) - Optional but recommended
   - Select the system pair
   - Load default transformations or customize
   - Save configuration

3. **Add a Table** (Tables view)
   - Specify source and target table names
   - Define primary key columns
   - Set include/exclude columns if needed
   - Add tags for organization

4. **Run Validation** (Tables view)
   - Click "▶ Run" on your table card
   - Monitor in Queue view
   - Review results in Validation Results view

5. **Schedule It** (Schedules view) - Optional
   - Create a cron schedule
   - Bind your table to the schedule
   - Automated validations will run on schedule

## 🏗️ Architecture

### Component Overview

LiveValidator consists of four main components:

1. **FastAPI Backend** - REST API serving validation configuration and history
2. **React Frontend** - Modern web UI built with Vite and Tailwind CSS
3. **PostgreSQL Control Database** - Stores configuration, queue, and 7-day history
4. **Databricks Spark Engine** - Executes validation jobs at scale

### Data Flow

```
User Action (Manual/Scheduled)
    ↓
Create Trigger (status='queued')
    ↓
Job Sentinel Claims Trigger (status='running')
    ↓
Launch Databricks Workflow
    ↓
Spark Job Executes Validation
    ↓
Write Results to History
    ↓
Delete Trigger from Queue
    ↓
Display in UI
```

### File Structure

```
LiveValidator/
├── src/app/                        # Deployed to Databricks Apps
│   ├── app.yaml                    # Databricks App configuration
│   ├── backend/
│   │   ├── app.py                  # FastAPI application & endpoints
│   │   ├── db.py                   # PostgreSQL connection pool
│   │   ├── models.py               # Pydantic models
│   │   ├── default_transformations.py  # Type mapping defaults
│   │   └── sql/
│   │       ├── ddl.sql             # Database schema
│   │       └── grants.sql          # Permission grants
│   ├── frontend/
│   │   ├── src/
│   │   │   ├── App.jsx             # Main application
│   │   │   ├── components/         # Reusable UI components
│   │   │   ├── views/              # Page-level views
│   │   │   ├── services/           # API client
│   │   │   └── utils/              # Helper functions
│   │   └── dist/                   # Production build (committed)
│   └── resources/
│       ├── run_validation.yml      # Databricks job definition
│       └── job_sentinel.yml        # Worker job definition
├── jobs/
│   ├── run_validation.py           # Spark validation logic
│   └── job_sentinel.py             # Queue worker process
├── resources/
│   ├── run_validation.yml          # Job config (DAB reference)
│   └── job_sentinel.yml            # Worker config (DAB reference)
├── docs/
│   └── images/                     # Documentation images
├── databricks.yml.example          # Bundle config template (copy to databricks.yml)
└── README.md
```

## 🌍 Environment Variables

### Local Development (.env file)
Create a `.env` file in the project root:
```bash
DB_DSN=postgresql://postgres:postgres@localhost:5432/livevalidator
DB_USE_SSL=false
```

## 📦 Features

### Setting Up

**System Management**
- Register source and target database systems
- Support for Databricks, Netezza, Teradata, SQL Server, MySQL, Postgres, Snowflake
- JDBC connection strings or Unity Catalog integration
- Secret-based credential management via Databricks
- Connection testing and validation

**Type Transformations**
- Define Python functions to handle type differences per system pair
- Pre-built defaults for common database combinations
- Real-time syntax and type hint validation
- Side-by-side editors for bidirectional transformations

### Configuring Validations

**Tables**
- Configure table-to-table comparisons
- Define primary key columns for row identification
- Include/exclude specific columns from comparison
- Tag-based organization
- Version conflict detection with optimistic locking

**Queries**
- Define custom SQL queries for complex validations
- Run same query on both systems or different queries
- Full filtering and comparison options

**Comparison Options**
- EXCEPT ALL set-based comparison
- Unicode normalization (downgrade ü→u, ñ→n, etc.)
- Special character replacement (configurable hex ranges)
- Diacritics removal
- Non-breaking space normalization
- System-level max row limits for large datasets

### Running Validations

**Manual Execution**
- One-click validation runs from UI
- Immediate queue priority
- Real-time status updates

**Scheduled Execution**
- Cron-based scheduling (e.g., `0 2 * * *`)
- Flexible schedule bindings (many-to-many)
- Bind multiple tables/queries to one schedule
- Bind one table to multiple schedules

**Bulk Operations**
- CSV upload for mass table/query configuration
- Multi-select bulk deletion
- Batch tagging and organization

**Queue Management**
- Priority-based execution queue
- View queued and running jobs
- Cancel queued validations
- Worker process with atomic job claiming (SKIP LOCKED)

### Monitoring & Results

**Validation History**
- 7-day retention in UI (with archival support)
- Comprehensive filtering:
  - By entity name, type, status
  - By system pair
  - By tags (AND logic)
  - By time range (presets: 1h, 3h, 6h, 12h, 24h, 7d)
- Tag-based organization with autocomplete
- Sortable columns (name, type, status, duration, etc.)
- Drill-down to sample differences (up to 10 rows)

**Results Details**
- Schema match status (missing/extra columns)
- Row count comparison
- Row-level difference count and percentage
- Sample mismatched rows (JSON format)
- Direct links to Databricks run logs
- Duration tracking and statistics

**Dashboard Stats**
- Total validations count
- Success/failure breakdown
- Average duration
- Queue status (queued, running, recent completions)

## 🔧 Advanced Configuration

### Type Transformation Functions

Define custom Python functions to handle data type differences between systems:

```python
def transform_columns(column_name: str, data_type: str) -> str:
    """
    Returns SQL expression for column transformation.
    
    Example for Netezza:
    - CHAR types → RTRIM to remove padding
    - NUMERIC → pass through
    - Others → CAST to VARCHAR(250)
    """
    if 'CHAR' in data_type:
        return f"RTRIM({column_name})"
    if data_type.startswith('NUMERIC'):
        return column_name
    return f"CAST({column_name} AS VARCHAR(250))"
```

**Features:**
- System-specific defaults included
- Syntax validation on save
- MyPy type checking (optional)
- Executed dynamically in Databricks

### Unicode Normalization Options

Control how string differences are handled:

**Downgrade Unicode** (`downgrade_unicode: true`)
- Converts unicode characters to ASCII equivalents
- ü → u, ñ → n, ç → c
- Removes diacritics and accents
- Normalizes non-breaking spaces

**Replace Special Characters** (`replace_special_char: ["7F", "?"]`)
- Replace characters above hex threshold with substitute
- Example: `["7F", "?"]` replaces chars above 0x7F with `?`
- Useful for systems with different character set support

**Extra Regex Replacement** (`extra_replace_regex: "\\.\\.\\."`)
- Additional pattern-based replacements
- Applied after special character replacement

### System Configuration

**Max Rows** (`max_rows: 1000000`)
- Limit rows read per system for validation
- Applied after count validation, before row-level comparison
- Useful for very large tables where full comparison isn't needed

**Tags**
- Organize validations by project, environment, criticality
- Multi-tag support with AND filtering
- Autocomplete suggestions from existing tags

### Schedule Bindings

**Many-to-Many Relationships:**
- One schedule can trigger multiple tables/queries
- One table/query can be bound to multiple schedules
- Example: Daily schedule + weekly schedule for critical tables

## 📚 Additional Documentation

For detailed technical documentation:

- **[Type Mappings Feature](TYPE_MAPPINGS_FEATURE.md)** - Type transformation implementation details
- **[Queue & History System](QUEUE_AND_HISTORY_IMPLEMENTATION.md)** - Queue architecture and API endpoints
- **[Databricks Deployment](DATABRICKS_DEPLOYMENT.md)** - Asset bundle configuration and deployment guide
- **[Contributing Guidelines](contributing.md)** - Development workflow and standards

## 🔒 Security Notes

- Never commit `.env` files or `.pem` certificates to git
- Use environment variables in Databricks for production credentials
- SSL is enabled by default for remote databases
- Local development can disable SSL for simplicity
- Type transformation functions run in Databricks environment - validate code before saving

## 🛠️ Development

### Running Tests
```bash
# Backend tests
pytest backend/tests/

# Frontend tests
cd frontend
npm test
```

### Code Quality
```bash
# Python linting
ruff check backend/

# JavaScript linting
cd frontend
npm run lint
```

## 📝 Contributing

See [CONTRIBUTING.md](contributing.md) for guidelines.

## 📄 License

MIT License - see [LICENSE](LICENSE) for details

## 🔄 Updating

To pull the latest updates:

```bash
git pull origin main
databricks bundle deploy -t <your-target>
```

Your `databricks.yml` is gitignored, so there won't be merge conflicts with your local configuration.

## 🐛 Troubleshooting

**"Database Not Initialized" banner won't go away:**
- Ensure you ran `grants.sql` to create the `apprunner` role
- Ensure you clicked "Initialize Database" in the Setup tab
- Hard refresh the browser (`Cmd+Shift+R` / `Ctrl+Shift+R`)

**Database connection fails:**
- Verify LakeBase is attached to the app in Databricks (provides `PGHOST`, `PGPORT`, `PGDATABASE`)
- Check that the `apprunner` role exists and has correct password


**"Role apprunner does not exist":**
- Run `grants.sql` in Databricks SQL Editor connected to the LakeBase instance
- Must be run by the LakeBase owner (whoever deployed the DAB)

**Validation jobs stuck in queue:**
- Check that `job_sentinel` workflow is running in Databricks

- Check Databricks job logs for errors

**Frontend changes not appearing:**
- Rebuild frontend: `cd src/app/frontend && npm run build`
- Redeploy the bundle: `databricks bundle deploy -t <your-target>`
- Redeploy the app: `databricks apps deploy live-validator -t <your-target>`
- Hard refresh browser to clear cache

