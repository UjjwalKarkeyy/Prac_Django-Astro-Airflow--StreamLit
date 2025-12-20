import streamlit as st
import requests
import time
import pandas as pd
from services.check_dag_status import check_dag_status
from services.retrieve_data import preview_data

DJANGO_INGEST_API = "http://127.0.0.1:8000/api/ingest"

st.title("Top 10 Movie Data Fetched from TMDB")
trigger_btn = st.button('Trigger Tasks')  


if trigger_btn:
    try:
        # Trigger DAG
        response = requests.post(DJANGO_INGEST_API, timeout=10)
        response.raise_for_status()
        data = response.json()

        dag_id = data["dag_id"]
        dag_run_id = data["dag_run_id"]
        status = data["state"]

        # Poll DAG status
        while status in ("queued", "running"):
            st.toast(f"Current status: :green[{status}]", duration=2)
            time.sleep(1.5)

            result = check_dag_status(dag_id, dag_run_id)
            status = result["state"]

        # Final state handling
        if status == "failed":
            st.error("DAG run FAILED!")
        else:
            st.toast(f":green[DAG finished with status: {status}]", duration=2)
            result = preview_data()
            df = pd.DataFrame(result)
            st.dataframe(df, use_container_width=True)

    except requests.RequestException as e:
        st.error(f"Request error: {e}")
    except KeyError as e:
        st.error(f"Missing expected response field: {e}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")
