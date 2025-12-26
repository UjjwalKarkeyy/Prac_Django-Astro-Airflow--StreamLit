# services/airflow.py
import requests
from requests.auth import HTTPBasicAuth
from services.exceptions import AirflowError

AIRFLOW_BASE_URL = "http://localhost:8080/api/v1"
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"

def trigger_dag(dag_id, topic, conf=None, run_id=None):
    url = f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns"

    # always send dag_id + topic in conf
    payload_conf = {"dag_id": dag_id, "topic": topic}
    # merge any extra conf the caller provides
    if conf:
        payload_conf.update(conf)

    payload = {"conf": payload_conf}

    if run_id:
        payload["dag_run_id"] = run_id

    response = requests.post(
            url,
            json=payload,
            auth=HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            timeout=15,
        )
    if not response.ok:
        raise AirflowError(
            f"Airflow error {response.status_code}: {response.text}"
        )
    response.raise_for_status()
    return response.json()
