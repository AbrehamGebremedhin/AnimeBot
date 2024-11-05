from django.urls import path
from .views import CustomUserListCreateAPIView, CustomUserDetailAPIView

urlpatterns = [
    path('users/', CustomUserListCreateAPIView.as_view(), name='user-list-create'),
    path('users/<int:pk>/', CustomUserDetailAPIView.as_view(), name='user-detail'),
]