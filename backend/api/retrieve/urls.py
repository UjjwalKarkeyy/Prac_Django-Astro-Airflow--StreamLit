from django.urls import path
from .views import RetrieveView

urlpatterns = [
    path("retrieve/<str:topic>", RetrieveView.as_view()),
]