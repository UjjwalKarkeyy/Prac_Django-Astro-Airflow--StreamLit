from django.urls import path
from .views import IngestView

urlpatterns = [
    path("ingest/<str:topic>/", IngestView.as_view()),
]
