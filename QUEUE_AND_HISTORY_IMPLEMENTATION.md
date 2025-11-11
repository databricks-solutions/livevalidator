# Queue & Validation History Implementation

## Summary

This implementation provides a complete queue and validation history system for tracking validation jobs from trigger to completion.

---

## Database Schema Changes

### 1. Enhanced `control.triggers` Table

**Added columns:**
- `databricks_run_id` TEXT - Tracks the Databricks workflow run ID
- `databricks_run_url` TEXT - URL to the Databricks run for easy access

**Updated:**
- `entity_type` now uses `'table'` instead of `'dataset'`
- Status is limited to `'queued'` and `'running'` (active jobs only)
- Completed jobs are deleted and moved to `validation_history`

### 2. New `control.validation_history` Table

**Purpose:** Stores completed validation results for UI display (30-day retention before archival)

**Key Features:**
- Denormalized data (entity names, system names) for query performance
- Generated columns for `duration_seconds` and `difference_pct`
- Comprehensive validation results:
  - Schema validation details
  - Row count comparison
  - Row-level differences with samples
  - Error messages and full result payload
- Indexed for common query patterns

**Indexes:**
- `entity_type, entity_id` - Find all validations for a table/query
- `finished_at DESC` - Recent validations
- `status` - Filter by success/failure
- `schedule_id` - Find all validations for a schedule
- `created_at DESC` - Time-based queries

---

## API Endpoints Implemented

### Queue Management

#### `GET /triggers?status={status}`
List active triggers with entity names. Optional status filter ('queued' or 'running').

**Response:**
```json
[
  {
    "id": 123,
    "entity_type": "table",
    "entity_id": 456,
    "entity_name": "my_table",
    "status": "queued",
    "priority": 100,
    "requested_by": "user@company.com",
    "requested_at": "2025-01-01T12:00:00Z"
  }
]
```

#### `POST /triggers`
Create a new validation trigger. Validates that entity exists and no duplicate active trigger.

**Request:**
```json
{
  "source": "manual",
  "entity_type": "table",
  "entity_id": 123,
  "requested_by": "user@company.com",
  "priority": 100,
  "params": {}
}
```

**Response:** Created trigger object

**Error 409:** If validation already queued/running for this entity
**Error 404:** If entity not found

#### `DELETE /triggers/{id}`
Cancel a queued trigger. Cannot cancel running triggers.

**Error 400:** If trigger is already running

#### `GET /queue-status`
Get queue statistics for dashboard display.

**Response:**
```json
{
  "active": {
    "queued": 5,
    "running": 2,
    "total_active": 7
  },
  "recent_1h": {
    "succeeded": 12,
    "failed": 1,
    "total_completed": 13
  }
}
```

---

### Validation History

#### `GET /validation-history`
List completed validations with filters.

**Query Params:**
- `entity_type` - Filter by 'table' or 'compare_query'
- `entity_id` - Filter by specific entity ID
- `status` - Filter by 'succeeded', 'failed', or 'canceled'
- `schedule_id` - Filter by schedule
- `limit` - Max results (default: 100)
- `offset` - Pagination offset (default: 0)

**Response:**
```json
[
  {
    "id": 789,
    "trigger_id": 123,
    "entity_type": "table",
    "entity_id": 456,
    "entity_name": "my_table",
    "source": "schedule",
    "requested_by": "scheduler",
    "started_at": "2025-01-01T12:00:00Z",
    "finished_at": "2025-01-01T12:02:30Z",
    "duration_seconds": 150,
    "source_system_name": "Prod DB",
    "target_system_name": "Lakehouse",
    "status": "succeeded",
    "schema_match": true,
    "row_count_match": true,
    "row_count_source": 1000000,
    "row_count_target": 1000000,
    "rows_compared": 1000000,
    "rows_different": 50,
    "difference_pct": 0.005,
    "databricks_run_url": "https://..."
  }
]
```

#### `GET /validation-history/{id}`
Get full validation details including sample differences and full result payload.

#### `GET /validation-history/entity/{entity_type}/{entity_id}/latest`
Get most recent validation for a specific table or query. Returns `null` if no history.

**Use case:** Display "Last Run" badge on table/query cards in UI

#### `POST /validation-history`
**Called by Databricks workflow** to record completion. Deletes trigger from queue.

**Request:** Full validation result object (see schema for all fields)

**Response:**
```json
{
  "id": 789,
  "ok": true
}
```

---

### Worker Helper Endpoints

#### `GET /triggers/next?worker_id={id}`
**Called by worker process** to atomically claim next queued trigger using `SKIP LOCKED`.

**Response:**
```json
{
  "trigger": {...},
  "entity": {...},
  "source_system": {...},
  "target_system": {...}
}
```

Returns `null` if no work available.

**Note:** Automatically cleans up orphaned triggers if entity was deleted.

#### `PUT /triggers/{id}/update-run-id`
**Called by worker** after successfully launching Databricks job.

**Request:**
```json
{
  "run_id": "12345",
  "run_url": "https://databricks.com/run/12345"
}
```

#### `PUT /triggers/{id}/fail`
**Called by worker** if it fails to launch the job (before Databricks runs).

Records minimal failure entry in history and removes trigger from queue.

**Request:**
```json
{
  "error": "Failed to authenticate with Databricks API"
}
```

---

## Data Flow

```
USER ACTION (UI/Scheduler)
    ↓
POST /triggers (status='queued')
    ↓
control.triggers table
    ↓
GET /triggers/next (worker polls)
    ↓
UPDATE status='running'
    ↓
WORKER LAUNCHES DATABRICKS JOB
    ↓
PUT /triggers/{id}/update-run-id
    ↓
DATABRICKS WORKFLOW EXECUTES
    ↓
POST /validation-history (on completion)
    ↓
INSERT into validation_history + DELETE from triggers
    ↓
UI DISPLAYS RESULTS
```

---

## Important Notes

### Entity Type Naming
- **Frontend/API:** Uses `"table"` for entity_type
- **Database:** Tables are still named `control.datasets` (for historical reasons)
- **Consistency:** All new endpoints use `"table"` terminology

### Trigger Lifecycle
1. **Created:** `status='queued'`, `priority=100` (default)
2. **Claimed by worker:** `status='running'`, `worker_id` set, `locked_at` set
3. **Completed:** Deleted from `triggers`, inserted into `validation_history`

### Error Handling
- **Entity deleted:** Worker auto-cleans orphaned triggers
- **Duplicate prevention:** Cannot queue same entity twice
- **Worker failures:** Recorded in history with `status='failed'`

### Performance Considerations
- `FOR UPDATE SKIP LOCKED` prevents worker contention
- Indexes optimized for common UI queries
- Denormalized fields reduce JOINs in history queries

---

## Future UI Views

### Queue Monitor (`/queue`)
- Live view of queued/running jobs
- Cancel queued jobs button
- System concurrency usage visualization
- Retry failed jobs

### Validation History (`/history`)
- Filterable table of completed validations
- Drill-down into individual results
- Charts: success rate, duration trends
- Export validation reports

### Entity Detail Pages
- "Last Run" status badge
- Quick link to full validation history
- "Run Now" button
- Schedule bindings display

---

## Testing Your Worker

### Minimal Worker Example (Python)

```python
import asyncio
import httpx

API_URL = "http://localhost:8000/api"

async def worker_loop():
    async with httpx.AsyncClient() as client:
        while True:
            # Poll for work
            resp = await client.get(f"{API_URL}/triggers/next?worker_id=test-worker")
            work = resp.json()
            
            if not work:
                print("No work, sleeping...")
                await asyncio.sleep(5)
                continue
            
            trigger = work['trigger']
            entity = work['entity']
            
            print(f"Got trigger {trigger['id']} for {entity['name']}")
            
            # Simulate Databricks job launch
            try:
                # Your Databricks API call here
                run_id = "mock-12345"
                
                # Update with run ID
                await client.put(
                    f"{API_URL}/triggers/{trigger['id']}/update-run-id",
                    json={"run_id": run_id, "run_url": f"https://databricks.com/run/{run_id}"}
                )
                
                print(f"Launched job {run_id}")
                
            except Exception as e:
                # Mark as failed
                await client.put(
                    f"{API_URL}/triggers/{trigger['id']}/fail",
                    json={"error": str(e)}
                )
                print(f"Failed to launch: {e}")

if __name__ == "__main__":
    asyncio.run(worker_loop())
```

---

## Validation Schema Changes

### Updated Pydantic Models

**`TriggerIn`:**
```python
class TriggerIn(BaseModel):
    source: Literal['manual', 'schedule', 'bulk_job'] = 'manual'
    schedule_id: Optional[int] = None
    entity_type: Literal['table', 'compare_query']
    entity_id: int
    requested_by: str
    priority: int = 100
    params: dict = Field(default_factory=dict)
```

**`BindingIn`:**
```python
class BindingIn(BaseModel):
    schedule_id: int
    entity_type: Literal['table', 'compare_query']
    entity_id: int
```

---

## Archive Strategy (Your Nightly Job)

Your nightly Databricks job should:

1. Query `control.validation_history` for records older than 30 days
2. Write to Delta table (e.g., `lakebase.control.validation_history_archive`)
3. Delete from Postgres `control.validation_history`

**Sample SQL:**
```sql
-- In Databricks notebook
DELETE FROM control.validation_history 
WHERE created_at < CURRENT_DATE - INTERVAL 30 DAYS
RETURNING *
```

Then write those rows to Delta for long-term analytics.

---

## Summary of Changes

### Files Modified:
- ✅ `backend/sql/ddl.sql` - Added columns to triggers, created validation_history table
- ✅ `backend/app.py` - Implemented all queue and history endpoints
- ✅ Updated all entity_type references from 'dataset' to 'table'

### Endpoints Added: 11 new endpoints
- 4 queue management endpoints
- 4 validation history endpoints
- 3 worker helper endpoints

### Ready for:
- Worker process deployment (separate from FastAPI app)
- UI implementation for Queue and History views
- Databricks workflow integration
