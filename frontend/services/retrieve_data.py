import requests

DJANGO_RETRIEVE_API = "http://127.0.0.1:8000/api/retrieve"

def preview_data(topic: str):
    try:
        return requests.get(f'{DJANGO_RETRIEVE_API}/{topic}').json()
    except Exception as e:
        return e