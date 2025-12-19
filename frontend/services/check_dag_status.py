import requests

DAG_STATUS_CHECK_API = "http://127.0.0.1:8080/api/v1/dags"

def check_dag_status(dag_id, dag_run_id):
    response = requests.get(
        f"{DAG_STATUS_CHECK_API}/{dag_id}/dagRuns/{dag_run_id}",
        auth=("airflow", "airflow"),
        timeout=10
    )
    response.raise_for_status()
    return response.json()
