from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from schemas.etl_schema import execute_sql, insert_sql
from collectors.youtube_collector import YouTubeNepal
from airflow.exceptions import AirflowSkipException

POSTGRES_CONN_ID = "postgres_default"

@dag(
    dag_id="genz_dag",
    start_date=datetime(2023, 10, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1},
    tags=["nepal", "genz"],
)

def start_genz_dag():
    @task
    def extract_data():
        collector = YouTubeNepal()
        # search videos
        vidIds = collector.search_videos(f"India", max_results=2)

        if not vidIds:
            print("No videos found.")
            return {'items': []}

        all_items = []
        for vidId in vidIds:
            if collector.is_already_processed(vidId):
                print(f"Skipping {vidId}: Already processed")
                continue
            
            # api call
            is_success, response = collector.fetch_data(vidId, limit=30)
            if is_success:
                collector.mark_as_processed(vidId)
                all_items.extend(response.get("items", []))
                print(f"Logged ID {vidId} to processed_log.json")
            else:
                print(f"No data retrieved for {vidId}")

        return {'items': all_items}

    @task
    def transform_data(extracted_data):
        items = extracted_data.get("items", [])

        if not items:
            raise AirflowSkipException("No comments today")
        comments = []
        for item in items:
            snippet = (
                item.get("snippet", {})
                    .get("topLevelComment", {})
                    .get("snippet")
            )
            if not snippet:
                continue
            comments.append(
                {
                    "id": item["id"],
                    "text": snippet["textDisplay"],
                    "author": snippet["authorDisplayName"],
                    "timestamp": snippet["publishedAt"],
                }
            )
        if not comments:
            raise AirflowSkipException("No valid comments after filtering")

        return comments

    @task
    def load_data(comments):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()  

        cursor.execute(execute_sql)

        values = [
            (
                val["id"],
                val["text"],
                val["author"],
                val["timestamp"],
            )
            for val in comments
        ]

        cursor.executemany(insert_sql, values)
        conn.commit()
        cursor.close()
    
    data = extract_data()
    comments = transform_data(data)
    load_data(comments)

# call the dag
start_genz_dag()