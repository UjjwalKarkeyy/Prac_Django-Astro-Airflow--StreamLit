from django.urls import path
from .views import RetrieveView

urlpatterns = [
    path("retrieve", RetrieveView.as_view()),
]