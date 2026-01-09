# Contributing

## Dev set-up

### 1. Setup Postgres
To set up your environment on local, you will need to first set up your local postgres database.

Assuming Mac,
```bash
brew install postgresql@16
brew services start postgresql@16
```

Test using:
```bash
psql postgres
```

Now we gotta install the tables and do the grants. We have a script for it:
```bash
psql -d postgres -a -f backend/sql/ddl.sql
psql -d postgres -a -f backend/sql/grants.sql
```

### 2. Configure Environment Variables

#### For Local Development
Create a `.env` file in the project root (copy from template):
```bash
cp .env.template .env
```

Edit `.env` with your local database credentials:
```bash
# .env
DB_DSN=postgresql://postgres:postgres@localhost:5432/postgres
DB_USE_SSL=false
```

**Note:** The `.env` file is gitignored and will not be committed.

#### For Databricks Deployment
Environment variables are configured in `app.yaml`:
```yaml
env:
  - name: DB_DSN
    value: "postgresql://user:pass@instance.database.cloud.databricks.com:5432/databricks_postgres"
  - name: DB_USE_SSL
    value: "true"
  - name: DB_SSL_CA_FILE
    value: "backend/databricks-ca.pem"
```

You can also override these in the Databricks UI under **App Configuration → Environment Variables**.

### 3. Install Backend Dependencies
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install package in editable mode
pip install -e .
```

### 4. Install Frontend Dependencies
```bash
cd frontend
npm install
```

### 5. Run the Application

#### Development Mode (Local)
```bash
# Terminal 1: Start backend
source venv/bin/activate
uvicorn backend.app:app --reload --port 8000

# Terminal 2: Start frontend dev server
cd frontend
npm run dev
```

The frontend will be available at `http://localhost:5173` and will proxy API requests to the backend at `http://localhost:8000`.

#### Production Build (Databricks)
```bash
# Build frontend
cd frontend
npm run build

# Deploy to Databricks
# The backend will serve both API and frontend from /dist
databricks apps deploy
```

## Environment Configuration Details

### How It Works
- **Local**: `backend/db.py` loads `.env` using `python-dotenv`
- **Databricks**: `app.yaml` env variables are loaded as OS environment variables
- **Code**: Uses `os.getenv()` which works in both environments

### Supported Environment Variables
| Variable | Description | Default | Local | Databricks |
|----------|-------------|---------|-------|------------|
| `DB_DSN` | PostgreSQL connection string | Lakebase fallback | `.env` | `app.yaml` |
| `DB_USE_SSL` | Enable SSL for database | `true` | `.env` | `app.yaml` |
| `DB_SSL_CA_FILE` | Path to SSL CA certificate | `backend/databricks-ca.pem` | `.env` | `app.yaml` |

### Security Notes
- Never commit `.env` files to git (already in `.gitignore`)
- Never commit `.pem` certificates to git (already in `.gitignore`)
- Use Databricks secrets for sensitive credentials in production
- Local development can disable SSL for simplicity