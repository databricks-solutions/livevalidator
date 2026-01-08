# LiveValidator

## Overview

**LiveValidator** is a data validation platform designed to ensure data integrity across heterogeneous database systems. It automates the comparison of tables and query results between source and target databases, detecting schema mismatches, row count discrepancies, and row-level differences.

### What It Does

LiveValidator performs three-tier validation between any two database systems:

1. **Schema Validation** - Compares column names and identifies missing or extra columns
2. **Row Count Validation** - Compares total row counts between source and target
3. **Row-Level Validation** - Detects actual data differences using either set-based comparison (EXCEPT ALL), or Primary Key.

When differences are found, LiveValidator captures sample records and provides detailed reports through a modern web interface.

### Architecture

![Architecture](docs/images/LiveValidator_arch_color.png)

**Workflow:**
1. **Configure** systems, tables, and queries through the web UI, bulk uploads supported
2. **Schedule** automated validations or trigger them manually
3. **Queue** manages job execution on Databricks Spark clusters
4. **Execute** validation logic compares data between systems
5. **Review** results in the validation history with filtering and tagging

### Key Capabilities

- **Snappy LakeBase Backend**: Handle 
- **Tagging**: Handle 
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
- Setup Databricks CLI to point to your target workspace
- Lakehouse Apps enabled on your workspace
- Lakebase enabled on your workspace
- Elevated/admin priveleges for the deployer

### Setup

#### 1. Deploy DAB

```bash
databricks bundle deploy -t <your-target>
```
Note: this may take 10+ minutes as it spins up Databricks App, LakeBase, etc.

#### 2. Start App

```bash
# start the app's compute
databricks apps start live-validator -t <your-target>
# deploy the app
databricks apps deploy live-validator --source-code-path /Workspace/LiveValidator/files/src/app -t migr-dev
```
Or simply navigate to Compute -> Apps and start it from the UI

#### 3. Navigate to the app
Inspect the app configuration page under Compute -> Apps

![Compute Apps](docs/images/find-apps.png)

Inspect the configuration and ensure the app has been properly deployed and is running. 

Now navigate the app's url. The URL should look like:
```
https://live-validator-<workspace-id>.aws.databricksapps.com/
```

On first launch, you'll see a banner prompting you to go to the Setup page.

#### 3. Setup Database

The rest of the setup instructions are located on the app's Setup page, but are included here as well.

Click **"Go to Setup →"** to proceed with database initialization.

![Database Not Initialized Banner](docs/images/setup-banner.png)

The owner of the LakeBase instance, who deployed the DAB initially, must run the following in Databricks SQL Editor attached to the live-validator Postgres compute.

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

![Database Not Initialized Banner](docs/images/init-database-1.png)

#### 4. Initialize Metadata Tables

a. Click the button to initialize the tables:

<img src="docs/images/init-database-2.png" alt="Initialize the tables" width="600"/>

b. Before you move on, hard-refresh the app using `Cmd+Shift+R` (Mac) or `Ctrl+Shift+R` (Windows). 

c. Navigate back to the Setup tab on bottom left.

<img src="docs/images/return-to-setup.png" alt="Return to setup" width="400"/>

#### 5. Start the `Job Sentinel`
```
databricks bundle run job_sentinel --no-wait -t <your target>
```

#### 6. Follow remaining instructions in app

As an overview, once you've completed the above steps, follow these steps to run your first validation:

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
Worker Claims Trigger (status='running')
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
├── backend/
│   ├── app.py                      # FastAPI application & endpoints
│   ├── db.py                       # PostgreSQL connection pool
│   ├── models.py                   # Pydantic models
│   ├── default_transformations.py  # Type mapping defaults
│   └── sql/
│       ├── ddl.sql                 # Database schema
│       ├── grants.sql              # Permission grants
│       └── migrate_*.sql           # Schema migrations
├── frontend/
│   ├── src/
│   │   ├── App.jsx                 # Main application
│   │   ├── components/             # Reusable UI components
│   │   ├── views/                  # Page-level views
│   │   ├── services/               # API client
│   │   └── utils/                  # Helper functions
│   └── dist/                       # Production build
├── jobs/
│   ├── run_validation.py           # Databricks validation notebook
│   └── job_sentinel.py             # Worker process
├── resources/
│   ├── run_validation.yml          # Databricks job definition
│   └── job_sentinel.yml            # Worker job definition
└── .env                            # Environment config (local)
```

## 🌍 Environment Variables

### Local Development (.env file)
Create a `.env` file in the project root:
```bash
DB_DSN=postgresql://postgres:postgres@localhost:5432/livevalidator
DB_USE_SSL=false
```

### Databricks Deployment (app.yaml)
Configure in `app.yaml` under the `env` section or via Databricks UI:

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DB_DSN` | PostgreSQL connection string | Databricks Lakebase | Yes |
| `DB_USE_SSL` | Enable SSL for database | `true` | No |
| `DB_SSL_CA_FILE` | Path to SSL CA certificate | `backend/databricks-ca.pem` | No |

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

Proprietary - NXP Semiconductors

## 🐛 Troubleshooting

**Database connection fails:**
- Check your `DB_DSN` is correct
- Verify SSL settings match your database (local = no SSL, Databricks = SSL)
- Ensure `databricks-ca.pem` exists if using SSL

**Frontend can't reach backend:**
- Check VITE_API environment variable
- Verify CORS settings in `backend/app.py`
- Ensure backend is running on expected port

**Build errors:**
- Clear node_modules: `rm -rf node_modules && npm install`
- Clear Python cache: `rm -rf backend/__pycache__`
- Rebuild: `npm run build`

