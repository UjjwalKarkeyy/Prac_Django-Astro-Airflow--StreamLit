from django.db import connection
from django.db.utils import ProgrammingError
from rest_framework.exceptions import ValidationError
from api.retrieve.serializers import CommentsSerializer

def retrieve_data(topic: str):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT c.*
                FROM airflow.topic_collector tc
                JOIN airflow.comments c
                  ON c.id = tc.id
                WHERE %s = ANY(tc.topic);
                """,
                [topic],
            )

            cols = [col[0] for col in cursor.description]
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

        # Always return a list (empty list is valid)
        serializer = CommentsSerializer(data=rows, many=True)
        serializer.is_valid(raise_exception=True)
        return serializer.data

    except ProgrammingError as e:
        return [{
            "error": "database_error",
            "detail": str(e),
        }]
    
    except ValidationError as e:
        return [{
            "error": "validation_error",
            "detail": e.detail,
        }]
    
    except Exception as e:
        return [{
            "error": "unknown_error",
            "detail": str(e),
        }]

