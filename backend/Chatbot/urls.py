from django.urls import path
from .views.user_views import CustomUserListCreateAPIView, CustomUserDetailAPIView, UserProfileAPIView
from .views.chat_views import ChatBotAPIView

urlpatterns = [
    path('users/', CustomUserListCreateAPIView.as_view(), name='user-list-create'),
    path('users/<int:pk>/', CustomUserDetailAPIView.as_view(), name='user-detail'),
    path('profile/', UserProfileAPIView.as_view(), name='user-profile'),
    path('chat/', ChatBotAPIView.as_view(), name='chat'),
]