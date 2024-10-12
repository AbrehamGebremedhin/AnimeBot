from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth.models import User
from utils.dataloader import Neo4JDataloader


@receiver(post_save, sender=User)
def create_neo4j_user(sender, instance, created, **kwargs):
    if created:
        neo4j_loader = Neo4JDataloader()
        user_pk = instance.pk
        user_name = instance.username
        email = instance.email
        # Add more attributes as needed
        with neo4j_loader.driver.session() as session:
            session.write_transaction(neo4j_loader._create_user, user_pk, user_name, email)
        neo4j_loader.close()
