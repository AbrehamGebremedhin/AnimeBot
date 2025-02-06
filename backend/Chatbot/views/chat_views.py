from rest_framework import status
from chat_processor.chatbot import Chat
from chat_processor.mal_api import API_CALL
from rest_framework.views import APIView
from rest_framework.response import Response

class ChatBotAPIView(APIView):
    """
    API view to handle chat bot interactions.
    """

    def post(self, request):
        """
        Handle POST requests to the chat bot API.

        Args:
            request (Request): The request object containing user data.

        Returns:
            Response: The response object containing the chat bot's reply.
        """
        user_id = int(request.data.get('user_id'))
        user_reply = request.data.get('reply', '')

        # Initialize the chat instance with Redis-based session history
        chat = Chat(user_id=user_id)
        mal_api = API_CALL()

        # Check for /recommend command
        if user_reply == "/recommend":
            response = chat.chat(req_user=user_id, category=None, user_req=user_reply)
            
            print(user_id)
            for recommendation in response["Recommendations"]:
                url, synopsis = mal_api.anime_data(recommendation["title"])
                recommendation["image_url"] = url
                recommendation["synopsis"] = synopsis
            
            response = response["Recommendations"]
            # Reset session history after generating recommendations
            chat.reset_session_history()
        elif user_reply == "":
            # Generate response for empty user input
            response = chat.chat(req_user=user_id, category=None, user_req="Hello")

            # Save the updated session history back to Redis
            chat.save_session_history()
        else:
            # Generate response for normal user input
            response = chat.chat(req_user=user_id, category=None, user_req=user_reply)
            
            # Save the updated session history back to Redis
            chat.save_session_history()

        return Response(response, status=status.HTTP_200_OK)