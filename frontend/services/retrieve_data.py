import requests

DJANGO_RETRIEVE_API = "http://127.0.0.1:8000/api/retrieve"

def preview_data():
    try:
        return requests.get(DJANGO_RETRIEVE_API).json()
    except Exception as e:
        return e