import os
import json
from dotenv import load_dotenv
from django.core.cache import cache
from langchain_ollama import OllamaLLM
from .user_graph_management import UserService
from langchain_community.graphs import Neo4jGraph
from langchain_core.output_parsers import JsonOutputParser
from langchain_community.embeddings import OllamaEmbeddings

load_dotenv(r'D:\Projects\AnimeBot\config.env')

class Chat:
    def __init__(self, user_id):
        """
        Initialize the Chat class with user ID and necessary components.

        Args:
            user_id (str): The ID of the user.
        """
        self.user_id = user_id
        self.llm = OllamaLLM(model="llama3.1")
        self.parser = JsonOutputParser()
        self.questions = json.load(open(r'D:\Projects\AnimeBot\backend\chat_processor\questions.json'))
        self.user_service = UserService()
        self.embedder = OllamaEmbeddings(model="nomic-embed-text")
        self.kg = Neo4jGraph(
            os.getenv('NEO4J_URI'), 
            username=os.getenv('NEO4J_USERNAME'), 
            password=os.getenv('NEO4J_PASSWORD')
        )

        # Load session history from Redis
        self.session_history = cache.get(f"session_history_{self.user_id}", [])

    def save_session_history(self):
        """
        Save the current session history to Redis with a 1-hour timeout.
        """
        cache.set(f"session_history_{self.user_id}", self.session_history, timeout=3600)

    def reset_session_history(self):
        """
        Clear the session history from Redis and reset the local session history.
        """
        cache.delete(f"session_history_{self.user_id}")
        self.session_history = []

    def generation_questions(self, category):
        """
        Paraphrase anime user questions to build the main user profile.

        Args:
            category (str): The question category to be formatted.

        Returns:
            dict: The paraphrased questions.
        """
        preffered_response = """{
            "{category}": [
                {
                    "question": "Paraphrased question",
                    "type": "Question Type",
                    "Options": [], # options if any are available
                    "var_name": "" # variable name
                },
            ]
        }"""

        prompt_template = f"""
            <|system|> 
            You are a chatbot engine for an anime recommendation application that uses a graph database. You are provided a questions data, which category to generate questions, and you will paraphrase all the questions in the category to make the questions sound more human.
            IMPORTANT: The way the questions data is organized as different categories, and each category has an array of questions. You need to paraphrase all the questions in the given category.

            IMPORTANT: Format your response as a single-line JSON string, without line breaks or escaped characters.

            Questions Data:  
            {self.questions}  

            Category: {category}

            Preffered Response: {preffered_response}

            Contextual Instructions:
            - Paraphrase the question in the questions format to sound and feel like chatting with a human.
            - Paraphrase all the questions in the given category.
            - Use the provided questions data to generate a response.
            - Only return the requested category data. As in do not include any other category in the response.
            
            Rules for JSON:
            1. Keep the structure of the JSON question, type, options and varname.
            1. Use double quotes for all variables and values.
            2. Return ONLY the JSON string without any additional text or comments.
            3. No line breaks or backticks; respond only with JSON.
            4. Do not include any additional information in the JSON response.
            
        """

        response = self.llm.invoke(prompt_template)

        formatted_response = self.parser.parse(response)

        return formatted_response
    
    def generate_response(self, reply, user_profile):
        """
        Generate a conversational response based on previous chat history and user profile.

        Args:
            reply (str): The user's reply.
            user_profile (dict): The user's profile data.

        Returns:
            dict: The chatbot response.
        """
        prompt_template = f"""
            <|system|> 
            You are a chatbot designed for an anime recommendation application, but you are also friendly and conversational. Your goal is to engage users with questions that feel like chatting with a friend. While some questions can focus on anime, others should feel more personal and casual to build rapport with the user.

            **User Profile**:
            {user_profile}

            **Session History**:
            {self.session_history[-5:]}

            **User Reply**:
            {reply}

            **Contextual Instructions**:
            - Alternate between anime-related and personal/casual questions to make the conversation more engaging.
            - Use a friendly and casual tone, avoiding overly formal or robotic phrasing.
            - Avoid repeating questions from previous conversations or those closely related to the most recent question.
            - Frame questions as if you’re genuinely curious about the user’s likes, hobbies, and thoughts.
            - Keep the questions open-ended to encourage more detailed responses from the user.

            **Output Format**:
            {{
                "question": "A conversational, open-ended question in a friendly tone."
            }}

            **Examples of Friendly Questions**:
            - "What’s something fun you’ve been up to lately?"
            - "What’s your favorite thing about your all-time favorite anime?"
            - "If you could visit a place from an anime in real life, where would it be and why?"
            - "When you’re not watching anime, how do you usually spend your time?"
            - "Is there an anime character you’d love to hang out with in real life?"
            - "What kind of stories inspire you the most—anime or otherwise?"

            Respond with a single JSON object containing the question, formatted exactly as specified above.
        """

        response = self.llm.invoke(prompt_template)

        # Parse and append the generated question to session history
        formatted_response = self.parser.parse(response)
        self.session_history.append({
            "system": formatted_response["question"],
            "user": reply
        })

        return formatted_response

    
    def prepare_user_profile_embedding(self, user_profile, session_history):
        """
        Create a user profile string for embedding based on profile and chat history.

        Args:
            user_profile (dict): The user's profile data.
            session_history (list): The chat session history.

        Returns:
            str: The concatenated user profile string.
        """
        profile = []

        # Prioritize user preferences
        for key, value in user_profile.items():
            if key in ["preferred_genres", "favorite_anime", "themes"]:
                profile.append(f"{key}: {value}")

        # Include recent chat history with lower weight
        for chat in session_history[-5:]:  # Consider only the most recent history
            profile.append(f"Chat history: System -> {chat['system']}, User -> {chat['user']}")

        return " ".join(profile)

    def similarity_search(self, user_profile):
        """
        Perform an improved similarity search using Neo4j and embeddings.

        Args:
            user_profile (dict): The user's profile data.

        Returns:
            dict: The search results with anime recommendations.
        """
        # Prepare the user profile for embedding
        user_profile_text = self.prepare_user_profile_embedding(user_profile, self.session_history)
        user_profile_embedding = self.embedder.embed_query(user_profile_text)

        # Query the Neo4j database with additional filtering for genres
        search_query = """
            MATCH (a:Anime)-[:IN_GENRE]->(g:Genre)
            WHERE g.name IN $preferred_genres
            WITH a, gds.similarity.cosine(a.embedded_text, $queryEmbedding) AS similarity
            OPTIONAL MATCH (a)-[:RATED_AS]->(r:Rating),
                        (a)-[:HAS_TYPE]->(t:Type),
                        (a)-[:SOURCED_FROM]->(s:Source),
                        (a)-[:IN_GENRE]->(genre:Genre)
            RETURN a, similarity, 
                COLLECT(DISTINCT r.name) AS ratings, 
                COLLECT(DISTINCT t.name) AS types, 
                COLLECT(DISTINCT s.name) AS sources, 
                COLLECT(DISTINCT genre.name) AS genres
            ORDER BY similarity DESC
            LIMIT 100
        """
        results = self.kg.query(search_query, params={
            'queryEmbedding': user_profile_embedding,
            'preferred_genres': user_profile.get("preferred_genres", [])
        })

        # Refine and rank results
        recommendations = []
        for result in results:
            # Calculate final score with additional weights
            genre_match_score = len(set(user_profile.get("preferred_genres", [])) & set(result["genres"]))
            final_score = result["similarity"] * 0.7 + genre_match_score * 0.3

            recommendations.append({
                "anime_id": result["a"]["anime_id"],
                "title": result["a"]["name"],
                "similarity": result["similarity"],
                "final_score": final_score,
                "synopsis": result["a"]["synopsis"],
                "image_url": result["a"]["image_url"],
                "score": result["a"]["score"],
                "aired": result["a"]["aired"],
                "status": result["a"]["status"],
                "duration": result["a"]["duration"],
                "no_episodes": result["a"]["no_episodes"],
                "rating": result["ratings"],
                "type": result["types"],
                "sourced_from": result["sources"],
                "genres": result["genres"]
            })

        # Sort by final score and take the top 5
        recommendations = sorted(recommendations, key=lambda x: x["final_score"], reverse=True)[:10]

        # Generate JSON response
        prompt_template = f"""
            <|system|> 
            You are an anime recommendation chatbot using a graph database. Your primary task is to recommend anime based on a user profile and anime data obtained through vector similarity search and metadata analysis. But do not change or modify the original fields for anime_id, synopsis, or image_url.

            IMPORTANT RULES:
            - DO NOT PARAPHRASE, ALTER, REPLACE OR CHANGE the original fields for **anime_id**, **synopsis**, or **image_url**.

            **User Profile**:
            {user_profile_text}

            **Anime Data**:
            {recommendations}

            **Contextual Instructions**:
            - Focus on aligning the recommendations with the user's preferences, especially genres and themes.
            - Rank recommendations by both similarity score and genre match.
            - Ensure diversity in recommendations while respecting the user's dislikes (if any).

            **Output Format**:
            {{
                "Recommendations": [
                    {{
                        "anime_id": "anime_id" # do not change the original anime_id in the Anime Data,
                        "title": "Original title",
                        "similarity": "Similarity Score (0.0 to 1.0)",
                        "synopsis": "Original synopsis (DO NOT MODIFY)",
                        "image_url": "Original image_url (DO NOT MODIFY)",
                        "score": "Original Anime Score",
                        "aired": "Original Aired Date",
                        "status": "Original Status",
                        "duration": "Original Episode Duration",
                        "no_episodes": "Original Number of Episodes",
                        "rating": ["Original Rating"],
                        "type": ["Original Anime Type"],
                        "sourced_from": ["Original Source Material"],
                        "genres": ["Original List of Genres"]
                    }}
                ]
            }}

            Respond ONLY with the JSON object. Do not add extra explanations or context.
            """


        response = self.llm.invoke(prompt_template)
        
        # Parse the LLM response
        formatted_response = self.parser.parse(response)

        return formatted_response

    def chat(self, req_user, user_req, category):
        """
        Handle the chat interaction with the user.

        Args:
            req_user (str): The requesting user's ID.
            user_req (str): The user's request or message.
            category (str): The category of the request, if any.

        Returns:
            dict: The generated response based on the user's request and profile.
        """
        user_profile = self.user_service.get_user_profile(req_user)

        if user_req == "/recommend":
            return self.similarity_search(user_profile=user_profile)
        elif category is None:
            return self.generate_response(reply=user_req, user_profile=user_profile)
        else:
            return self.generation_questions(category=category)
            