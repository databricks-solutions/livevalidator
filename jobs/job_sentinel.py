# Databricks notebook source
# MAGIC %md
# MAGIC # JobSentinel - Orchestrator & Scheduler
# MAGIC 
# MAGIC Continuously monitors schedules and triggers, launching validation jobs as needed.
# MAGIC Respects system concurrency limits and manages the validation queue.

# COMMAND ----------
%pip install croniter

# COMMAND ----------

import json
import time
import requests
from datetime import datetime, timedelta, UTC
from zoneinfo import ZoneInfo
from databricks.sdk import WorkspaceClient
from croniter import croniter
import traceback

dbutils.widgets.text("backend_api_url", "")
backend_api_url: str = dbutils.widgets.get("backend_api_url")

dbutils.widgets.text("poll_interval", "30")
poll_interval: int = int(dbutils.widgets.get("poll_interval") or "30")

dbutils.widgets.text("validation_job_id", "")
validation_job_id: str = dbutils.widgets.get("validation_job_id")

print(f"JobSentinel starting...")
print(f"Backend: {backend_api_url}")
print(f"Validation Job ID: {validation_job_id}")
print(f"Poll Interval: {poll_interval}s")

# COMMAND ----------

_workspace_client: WorkspaceClient | None = None
_client_created_at: datetime | None = None

def get_workspace_client() -> WorkspaceClient:
    """Get or create WorkspaceClient, refreshing hourly"""
    global _workspace_client, _client_created_at
    
    if _workspace_client is None or _client_created_at is None or \
       datetime.now(UTC) - _client_created_at > timedelta(hours=1):
        _workspace_client = WorkspaceClient(
            host=spark.conf.get('spark.databricks.workspaceUrl'),
            client_id=dbutils.secrets.get(scope = "livevalidator", key = "lv-app-id"),
            client_secret=dbutils.secrets.get(scope = "livevalidator", key = "lv-app-secret")
            )
        _client_created_at = datetime.now(UTC)
    
    return _workspace_client

def api_call(method: str, endpoint: str, data: dict | list | None = None):
    """Call backend API"""
    client: WorkspaceClient = get_workspace_client()
    response: requests.Response = requests.request(
        method, f"{backend_api_url}{endpoint}", json=data, headers=client.config.authenticate(), timeout=30
    )
    response.raise_for_status()
    return response.json()


def update_schedule(schedule_id: int, version: int, **fields) -> None:
    """Update schedule fields"""
    api_call("PUT", f"/api/schedules/{schedule_id}", {
        **fields,
        "updated_by": "JobSentinel",
        "version": version
    })


def check_and_create_scheduled_triggers() -> int:
    """Check schedules and create triggers for due jobs"""
    created: int = 0

    try:
        schedules: list[dict] = api_call("GET", "/api/schedules")

        for schedule in schedules:
            if not schedule.get("enabled"):
                continue

            # Get current time in schedule's timezone
            tz: ZoneInfo = ZoneInfo(schedule.get("timezone", "UTC"))
            now: datetime = datetime.now(tz)

            next_run_at: str | None = schedule.get("next_run_at")

            # Initialize next_run_at if missing
            if not next_run_at:
                try:
                    cron: croniter = croniter(schedule["cron_expr"], now)
                    next_run: datetime = cron.get_next(datetime)
                    print(f"Initializing schedule '{schedule['name']}': next run at {next_run.isoformat()}")
                    update_schedule(schedule["id"], schedule["version"], next_run_at=next_run.isoformat())
                except Exception as e:
                    print(f"[WARN] Invalid cron expression for schedule '{schedule['name']}': {schedule['cron_expr']} - {e}")
                continue

            # Parse next_run_at (comes from DB as UTC timestamp)
            next_run_dt: datetime = datetime.fromisoformat(next_run_at.replace('Z', '+00:00'))

            # Check if schedule is due (comparison works across timezones)
            if now < next_run_dt:
                continue

            print(f"Schedule '{schedule['name']}' is due")

            # Create triggers for all bindings
            bindings: list[dict] = api_call("GET", f"/api/bindings_by_sched/{schedule['id']}")

            if bindings:
                # Build bulk trigger request
                trigger_requests: list[dict] = [
                    {
                        "source": "schedule",
                        "schedule_id": schedule["id"],
                        "entity_type": binding["entity_type"],
                        "entity_id": binding["entity_id"],
                        "priority": schedule.get("priority", 100),
                        "requested_by": "JobSentinel",
                        "params": {}
                    }
                    for binding in bindings
                ]
                
                # Bulk create triggers
                result: dict = api_call("POST", "/api/triggers/bulk", trigger_requests)
                num_created: int = len(result.get("created", []))
                created += num_created
                
                if num_created > 0:
                    print(f"Created {num_created} triggers")
                
                # Note: Duplicates (already queued/running) are silently skipped by the backend

            # Update schedule after processing (even if no bindings)
            try:
                cron = croniter(schedule["cron_expr"], now)
                next_run_dt: datetime = cron.get_next(datetime)
                update_schedule(schedule["id"], schedule["version"], last_run_at=now.isoformat(), next_run_at=next_run_dt.isoformat())
                print(f"Updated schedule: next run at {next_run_dt.isoformat()}")
            except Exception as e:
                print(f"[WARN] Failed to calculate next run for schedule '{schedule['name']}': {e}")

    except Exception:
        print(f"[ERROR] Schedule check failed: {traceback.format_exc()}")

    return created


def can_launch_job(src_system_id: int, tgt_system_id: int, running_per_system: dict) -> bool:
    """Check if job can be launched based on system concurrency limits"""
    try:
        src_system: dict = api_call("GET", f"/api/systems/{src_system_id}")
        tgt_system: dict = api_call("GET", f"/api/systems/{tgt_system_id}")

        # Get concurrency limits (-1 = unlimited)
        limits: list[int] = [
            c for c in [src_system.get("concurrency", -1), tgt_system.get("concurrency", -1)]
            if c != -1
        ]

        if not limits:
            return True

        # Check if any system exceeds its limit
        effective_limit: int = min(limits)
        max_running: int = max(
            running_per_system.get(src_system_id, 0),
            running_per_system.get(tgt_system_id, 0)
        )

        if max_running >= effective_limit:
            print(f"Concurrency limit reached: {max_running}/{effective_limit}")
            return False

        return True

    except Exception as e:
        print(f"⚠️  Concurrency check failed: {traceback.format_exc()}, allowing launch")
        return True


def process_next_trigger(running_per_system: dict[int, int]) -> bool:
    """Claim and process next queued trigger"""
    try:
        trigger: dict[str, str] = api_call("GET", "/api/triggers/next")

        if not trigger:
            return False

        print(f"Claimed trigger {trigger['id']}: {trigger['name']}")

        # Check concurrency limits
        if not can_launch_job(trigger["src_system_id"], trigger["tgt_system_id"], running_per_system):
            # Release trigger back to queue - can't launch yet
            api_call("PUT", f"/api/triggers/{trigger['id']}/release", {})
            return False

        # Fetch global validation config, then apply entity-specific overrides
        resolved_config = api_call("GET", "/api/validation-config")
        if trigger.get("config_overrides"):
            resolved_config.update(trigger.get("config_overrides"))

        # Build job parameters
        is_table: bool = trigger["entity_type"] == "table"
        params: dict = {
            "trigger_id": str(trigger["id"]),
            "name": trigger["name"],
            "source_system_name": str(trigger["src_system_name"]),
            "target_system_name": str(trigger["tgt_system_name"]),
            "backend_api_url": backend_api_url,
            "source_table": trigger.get("source_table", "") if is_table else "",
            "target_table": trigger.get("target_table", "") if is_table else "",
            "sql": trigger.get("sql", "") if not is_table else "",
            "watermark_expr": trigger.get("watermark_expr", "") or "",
            "compare_mode": trigger.get("compare_mode", "except_all"),
            "pk_columns": json.dumps(trigger.get("pk_columns") or []),
            "include_columns": json.dumps(trigger.get("include_columns") or []),
            "exclude_columns": json.dumps(trigger.get("exclude_columns") or []),
            "options": json.dumps(trigger.get("options") or {}),
            "downgrade_unicode": str(resolved_config.get("downgrade_unicode", False)).lower(),
            "replace_special_char": json.dumps(resolved_config.get("replace_special_char", [])),
            "extra_replace_regex": resolved_config.get("extra_replace_regex", "")
        }

        # Launch validation job
        print(f"Launching validation job...")
        w: WorkspaceClient = get_workspace_client()
        run = w.jobs.run_now(job_id=validation_job_id, job_parameters=params)
        run_url: str = f"{w.config.host}/jobs/{validation_job_id}/runs/{run.run_id}"

        print(f"Launched {trigger["name"]}: {run_url}")

        # Update trigger with run info
        api_call("PUT", f"/api/triggers/{trigger['id']}/update-run-id", {
            "run_id": str(run.run_id),
            "run_url": str(run_url)
        })

        # increment running countz for source and target systems
        for system_id in [trigger["src_system_id"], trigger["tgt_system_id"]]:
            if running_per_system.get(system_id):
                running_per_system[system_id] += 1
            else:
                running_per_system[system_id] = 1

        return True

    except Exception:
        print(f"[ERROR] Failed to process trigger: {traceback.format_exc()}\nPutting trigger back in queue...")
        api_call("PUT", f"/api/triggers/{trigger['id']}/release", {})
        return False


# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Loop

# COMMAND ----------

print("JobSentinel active - monitoring schedules and triggers...")

while True:
    try:
        print(f"\n--- Start Iteration {datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")} ---")

        # Check schedules and create triggers
        created: int = check_and_create_scheduled_triggers()

        # Get running jobs per system for concurrency control
        running_per_system: dict[int, int] = api_call("GET", "/api/triggers/running-per-system")

        # Process queued triggers
        processed: int = 0
        while process_next_trigger(running_per_system):
            processed += 1

        if processed > 0:
            print(f"Launched {processed} validation jobs")
        elif created == 0:
            print("No work to do")

        time.sleep(poll_interval)

    except KeyboardInterrupt:
        print("\nJobSentinel shutting down...")
        break
    except Exception as e:
        print(f"[ERROR] Unexpected error: {traceback.format_exc()}")
        print("Sleeping 60s before retry...")
        time.sleep(60)
