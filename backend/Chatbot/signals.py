# signals.py
from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import CustomUser
from chat_processor.user_graph_management import UserService

@receiver(post_save, sender=CustomUser)
def create_user_in_neo4j(sender, instance, created, **kwargs):
    if created:
        user_service = UserService()
        user_service.create_user(instance.id, instance.email, instance.username)
        user_service.close()