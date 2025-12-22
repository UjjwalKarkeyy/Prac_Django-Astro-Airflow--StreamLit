from rest_framework.views import APIView
from rest_framework.response import Response
from services.airflow import trigger_dag

class IngestView(APIView):
    def post(self, request, topic: str = 'genz', dag_id : str = 'genz_dag'):
        result = trigger_dag(dag_id, topic)
        return Response(result)
