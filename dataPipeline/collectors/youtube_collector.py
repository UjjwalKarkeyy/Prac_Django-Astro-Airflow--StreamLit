from googleapiclient.discovery import build
from googleapiclient.errors import HttpError # For catching specific API errors
from datetime import datetime, timedelta
import os
from .base_collector import BaseCollector

class YouTubeNepal(BaseCollector):
    def __init__(self):
        super().__init__("youtube")
        self.api_key = os.getenv("YOUTUBE_API_KEY")
        self.youtube = build("youtube", "v3", developerKey=self.api_key)

    def search_videos(self, query, max_results=2):
        try:
            seven_days_ago = (datetime.utcnow() - timedelta(days=7)).isoformat() + "Z"
            print(f"api: {self.youtube}")
            print(f"[YT] Searching for: {query}")
            
            request = self.youtube.search().list(
                q=query,
                part="id,snippet",
                type="video",
                order="date",
                publishedAfter=seven_days_ago,
                maxResults=max_results,
                regionCode="NP"
            )
            response = request.execute()
            vidIds = [item['id']['videoId'] for item in response.get('items', [])]
            print(f"[YT] Found {len(vidIds)} video IDs: {vidIds}")
            return vidIds
        except Exception as e:
            print(f"[YT] SEARCH ERROR: {e}")
            return []

    def fetch_data(self, video_id, limit=20) -> tuple[bool, dict]:
        print(f"[YT] ---> STARTING FETCH FOR VIDEO: {video_id}") # LOG TEST
        try:
            request = self.youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=limit,
                textFormat="plainText"
            )
            response = request.execute()
            return (True, response)

        except HttpError as e:
            # If comments are disabled, YouTube returns a 403 error
            if e.resp.status == 403:
                print(f"[YT] SKIPPING: Comments are disabled for video {video_id}")
            else:
                print(f"[YT] API ERROR ({e.resp.status}): {e}")
            return (False, {})
        except Exception as e:
            print(f"[YT] UNKNOWN ERROR: {e}")
            return (False, {})