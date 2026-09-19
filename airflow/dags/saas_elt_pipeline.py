"""Daily production-style ELT orchestration for the SaaS analytics platform."""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from urllib import request

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path(os.environ.get("SAAS_ELT_PROJECT_ROOT", "/opt/airflow/saas_elt"))


def notify_failure(context: dict) -> None:
    webhook = os.getenv("ALERT_WEBHOOK_URL")
    if not webhook:
        return
    payload = {
        "text": (
            f"SaaS ELT failure: dag={context['dag'].dag_id}, "
            f"task={context['task_instance'].task_id}, run={context['run_id']}"
        )
    }
    req = request.Request(
        webhook,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    request.urlopen(req, timeout=10).read()


DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="enterprise_saas_elt",
    description="Ingest SaaS sources, build dbt models, and publish tested marts",
    start_date=datetime(2026, 1, 1),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["snowflake", "dbt", "saas", "governed"],
)
def enterprise_saas_elt():
    @task
    def ingest(ds: str) -> str:
        command = [
            "python",
            "-m",
            "saas_elt.cli",
            "ingest",
            "--source-root",
            str(PROJECT_ROOT / "data/source"),
            "--run-date",
            ds,
        ]
        completed = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        manifest = json.loads(completed.stdout)
        return manifest["manifest_path"]

    @task
    def enforce_quality_gate(manifest_path: str) -> dict:
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        source_rows = sum(item["source_rows"] for item in manifest["datasets"])
        rejected_rows = sum(item["rejected_rows"] for item in manifest["datasets"])
        rejection_rate = rejected_rows / source_rows if source_rows else 1.0
        if rejection_rate > 0.02:
            raise AirflowException(f"Rejection rate {rejection_rate:.2%} exceeds 2% threshold")
        return {"source_rows": source_rows, "rejected_rows": rejected_rows}

    load_raw = BashOperator(
        task_id="load_raw_to_snowflake",
        bash_command=(
            "python scripts/load_to_snowflake.py "
            "--manifest '{{ ti.xcom_pull(task_ids=\"ingest\") }}'"
        ),
        cwd=str(PROJECT_ROOT),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="dbt deps && dbt build --profiles-dir . --target prod",
        cwd=str(PROJECT_ROOT),
        env={**os.environ, "DBT_FULL_REFRESH": "false"},
    )

    dbt_docs = BashOperator(
        task_id="publish_dbt_artifacts",
        bash_command="dbt docs generate --profiles-dir . --target prod",
        cwd=str(PROJECT_ROOT),
    )

    manifest = ingest()
    gate = enforce_quality_gate(manifest)
    gate >> load_raw >> dbt_build >> dbt_docs


enterprise_saas_elt()

