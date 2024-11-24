from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.shortcuts import get_object_or_404
from ..models import CustomUser
from ..serializers import CustomUserSerializer
from chat_processor.chatbot import Chat
from chat_processor.user_graph_management import UserService

class CustomUserListCreateAPIView(APIView):
    """
    API view to retrieve list of users or create a new user.
    """
    def get(self, request):
        """
        Retrieve a list of all users.

        Args:
            request (Request): The request object.

        Returns:
            Response: A response object containing serialized user data.
        """
        users = CustomUser.objects.all()
        serializer = CustomUserSerializer(users, many=True)
        return Response(serializer.data)

    def post(self, request):
        """
        Create a new user.

        Args:
            request (Request): The request object containing user data.

        Returns:
            Response: A response object containing serialized user data or errors.
        """
        print(request.data)
        serializer = CustomUserSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        print(serializer.errors)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class CustomUserDetailAPIView(APIView):
    """
    API view to retrieve, update, or delete a user by ID.
    """
    def get(self, request, pk):
        """
        Retrieve a user by ID.

        Args:
            request (Request): The request object.
            pk (int): The primary key of the user.

        Returns:
            Response: A response object containing serialized user data.
        """
        user = get_object_or_404(CustomUser, pk=pk)
        serializer = CustomUserSerializer(user)
        return Response(serializer.data)

    def patch(self, request, pk):
        """
        Update a user by ID.

        Args:
            request (Request): The request object containing user data.
            pk (int): The primary key of the user.

        Returns:
            Response: A response object containing serialized user data or errors.
        """
        user = get_object_or_404(CustomUser, pk=pk)
        serializer = CustomUserSerializer(user, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    def delete(self, request, pk):
        """
        Delete a user by ID.

        Args:
            request (Request): The request object.
            pk (int): The primary key of the user.

        Returns:
            Response: A response object with no content.
        """
        user = get_object_or_404(CustomUser, pk=pk)
        user.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
    
class UserProfileAPIView(APIView):
    """
    API view to handle user profile related operations.
    """
    def get(self, request):
        """
        Retrieve questions based on user ID and category.

        Args:
            request (Request): The request object containing user ID and category.

        Returns:
            Response: A response object containing questions.
        """
        user_id = int(request.data.get('user_id'))
        chat = Chat(user_id=user_id)
        category = request.data.get('category')
        questions = chat.chat(req_user=user_id, category=category, user_req=None)
        questions = questions[category]
        return Response(questions, status=status.HTTP_200_OK)

    def post(self, request):
        """
        Update user profile fields.

        Args:
            request (Request): The request object containing user ID, category, and fields to update.

        Returns:
            Response: A response object indicating the update status.
        """
        service = UserService()
        print(request.data)
        user_id = int(request.data.get('user_id'))
        for key, value in request.data.get('fields').items():
            service.update_variable(user_id=user_id, field_name=f"{request.data.get('category')}_{key}", value=value)

        return Response(status=status.HTTP_202_ACCEPTED)
    