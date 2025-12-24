from rest_framework.views import APIView
from rest_framework.response import Response
from services.retrieve_data import retrieve_data

class RetrieveView(APIView):
    def get(self, request, topic):        
        result = retrieve_data(topic)
        return Response(result)