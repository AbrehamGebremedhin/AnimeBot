from rest_framework import serializers
from .models import CustomUser

class CustomUserSerializer(serializers.ModelSerializer):
    class Meta:
        model = CustomUser
        fields = ['id', 'email', 'username', 'first_name', 'is_active', 'is_staff', 'date_joined']
        read_only_fields = ['id', 'is_active', 'is_staff', 'date_joined']