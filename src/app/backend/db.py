import os
import ssl
import asyncpg
from dotenv import load_dotenv

# Load environment variables from .env file (for local development)
# On Databricks, environment variables are set via app.yaml
load_dotenv()

# Database connection configuration
# Local: Set in .env file
# Databricks: Set in the DABs by adding postgres as a App Resource
PGDATABASE = os.getenv("PGDATABASE")
PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT")
# DB_DSN = os.getenv("DB_DSN")
DB_DSN = f"postgresql://apprunner:beepboop123@{PGHOST}:{PGPORT}/{PGDATABASE}"

DB_USE_SSL = os.getenv("PGSSLMODE")== "require"
DB_SSL_CA_FILE = os.getenv("DB_SSL_CA_FILE", "backend/databricks-ca.pem")

# Fallback to Databricks Lakebase if DB_DSN not explicitly set
# (This is a safety fallback; production should always set DB_DSN)
if not DB_DSN:
    print("⚠️  Warning: DB_DSN not set, please ensure a LakeBase backend is set up and accessible in app.yml")

pool: asyncpg.Pool | None = None


def _ssl_ctx():
    """
    Create SSL context for secure database connections.
    If a CA file is provided and exists, use it. Otherwise, use system default certificates.
    """
    if DB_SSL_CA_FILE and os.path.exists(DB_SSL_CA_FILE):
        # Use custom CA certificate file if provided
        ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=DB_SSL_CA_FILE)
        print(f"🔐 Using custom CA certificate: {DB_SSL_CA_FILE}")
    else:
        # Use system default CA certificates (works for most cloud providers including Databricks)
        ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        if DB_SSL_CA_FILE:
            print(f"ℹ️  Custom CA file not found ({DB_SSL_CA_FILE}), using system defaults")
    
    # Keep hostname verification and certificate validation enabled for security
    return ctx


async def init_pool():
    """Initialize the database connection pool."""
    global pool
    if pool is None:
        print(f"🔌 Connecting to database: {DB_DSN.split('@')[1] if '@' in DB_DSN else DB_DSN}")
        print(f"🔒 SSL enabled: {DB_USE_SSL}")
        
        if DB_USE_SSL:
            pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=10, ssl=_ssl_ctx())
        else:
            pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=10)
    return pool


async def fetchrow(sql: str, *args):
    p = await init_pool()
    async with p.acquire() as conn:
        return await conn.fetchrow(sql, *args)


async def fetch(sql: str, *args):
    p = await init_pool()
    async with p.acquire() as conn:
        return await conn.fetch(sql, *args)


async def execute(sql: str, *args):
    p = await init_pool()
    async with p.acquire() as conn:
        return await conn.execute(sql, *args)


async def fetchval(sql: str, *args):
    p = await init_pool()
    async with p.acquire() as conn:
        return await conn.fetchval(sql, *args)
