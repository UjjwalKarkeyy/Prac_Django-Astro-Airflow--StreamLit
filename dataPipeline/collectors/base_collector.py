import os
import json
from abc import ABC, abstractmethod

class BaseCollector(ABC):
    def __init__(self, platform):
        self.platform = platform
        self.base_path = f"/opt/airflow/data/{self.platform}"
        os.makedirs(self.base_path, exist_ok=True)
        self.log_file = os.path.join(self.base_path, "processed_log.json")

    @abstractmethod
    def fetch_data(self, *args, **kwargs): pass

    def is_already_processed(self, item_id):
        """Check if we already downloaded data for this ID."""
        if not os.path.exists(self.log_file):
            return False
        try:
            with open(self.log_file, "r") as f:
                content = f.read().strip()
                if content is None:
                    return False  # empty file → nothing processed yet
                processed_ids = json.loads(content)
        except json.JSONDecodeError:
            # corrupted or partially written file
            return False

        return item_id in processed_ids

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