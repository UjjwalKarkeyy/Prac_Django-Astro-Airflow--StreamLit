import os
import json
from abc import ABC, abstractmethod
from services.psql_conn import psql_cursor

class BaseCollector(ABC):
    def __init__(self, platform):
        self.platform = platform
        self.base_path = f"/opt/airflow/data/{self.platform}"
        os.makedirs(self.base_path, exist_ok=True)
        self.log_file = os.path.join(self.base_path, "processed_log.json")

    @abstractmethod
    def fetch_data(self, *args, **kwargs): pass

    def is_already_processed(self, vid_id: str) -> bool:
        """
        Returns True if this video has already been processed (i.e., exists in processed_vidIds table).
        """
        with psql_cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM processed_vidIds WHERE vid_id = %s LIMIT 1;",
                (vid_id,)
            )
            exists = cursor.fetchone() is not None
            return exists

    def mark_as_processed(self, item_id):
        """Add ID to the processed list to avoid duplicates next time."""
        processed_ids = []
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, "r") as f:
                    raw = f.read().strip()
                    if raw:  # only parse if not empty
                        processed_ids = json.loads(raw)
            except json.JSONDecodeError:
                # file is corrupted/invalid JSON -> treat as empty state
                processed_ids = []

        # add id if not already there
        if item_id not in processed_ids:
            processed_ids.append(item_id)

        # write back valid JSON (overwrite)
        with open(self.log_file, "w") as f:
            json.dump(processed_ids, f)