import json
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from langchain_ollama import OllamaLLM
from langchain.schema import Document
from chat_processor.user_graph_management import UserService
from langchain_core.output_parsers import JsonOutputParser
from langchain_community.embeddings import OllamaEmbeddings

  
class Chat:
    def __init__(self):
        self.llm = OllamaLLM(model="llama3.1")
        self.parser = JsonOutputParser()
        self.questions = json.load(open(r'D:\Projects\AnimeBot\backend\chat_processor\questions.json'))
        self.session_history = []
        self.user_service = UserService()
        self.embedder = OllamaEmbeddings(model="nomic-embed-text")

    def embed_text(self, text):
        return self.embedder.embed_documents(text)
    
    def embed_user_profile(self, user_profile):
        pass
        
    def generation_questions(self, category):
        """
        Paraphrases anime user questions to build the main user profile.

        Args:
            query (str): The question category to be formatted.

        Returns:
            Json: The Paraphrased Questions.
        """
        preffered_response = """{
            "": [
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
        Formats the user query into the annotation service format.

        Args:
            query (str): The user query to be formatted.

        Returns:
            str: The formatted query.
        """
        prompt_template = f"""
            <|system|> 
            You are a chatbot engine for an anime recommendation application that uses a graph database. You are chat with the user mainly to build a user profile for an anime recommender system you are provided the logged in user profile, session history, the user's reply and previously asked questions. 
            So your question should be tailored towards anime, personality. All questions should be open-ended.

            IMPORTANT: Format your response as a single-line JSON string, without line breaks or escaped characters.

            User Profile:
            {user_profile}

            Session History:  
            {self.session_history[:10]}

            Previously Asked Questions:
            {self.questions}

            User Reply: 
            {reply}

            Output Format: {{
                "question": "Formatted response"
            }}  

            Contextual Instructions:
            - Phrase the question in the questions format to sound and feel like chatting with a human.
            - Do not ask questions that follow the theme of previously asked questions.
            - Do not ask the same question, and previousky asked questions twice.

            Rules for JSON:
            1. Keep the structure of the JSON question, type, options and varname.
            1. Use double quotes for all variables and values.
            2. Return ONLY the JSON string without any additional text or comments.
            3. No line breaks or backticks; respond only with JSON.
            4. Do not include any additional information in the JSON response.
            
        """

        response = self.llm.invoke(prompt_template)

        formatted_response = self.parser.parse(response)

        self.session_history.append({
            "system": formatted_response["question"],
            "user": reply
        })

        return formatted_response
    
    def similarity_search(self, user_profile):
        profile_embedding = self.embedder.embed_documents([Document(page_content=user_profile)])

        # Fetch all anime embeddings from the Neo4j database
        fetch_embeddings_query = """
            MATCH (anime:Anime)
            RETURN anime.anime_id AS animeId, anime.embedded_text AS embedded_text
        """

        results = self.kg.query(fetch_embeddings_query)

        # Prepare embeddings and metadata for similarity calculation
        embeddings = []
        for result in results:
            embeddings.append(result['embedded_text'])

        # Convert embeddings and query_embedding to numpy arrays
        embeddings = np.array(embeddings)
        query_embedding = np.array(profile_embedding).reshape(1, -1)

        # Compute cosine similarity between query embedding and chunk embeddings
        similarities = cosine_similarity(query_embedding, embeddings).flatten()

        # Sort the results by similarity score in descending order
        sorted_indices = np.argsort(-similarities)
        top_results = sorted_indices[:4]  # Get the top 4 results

        # Create response documents
        response = []
        for index in top_results:
            similarity_score = similarities[index]
            document = Document(
                metadata={
                    'similarity': similarity_score
                }
            )
            response.append(document)

        return response

    
    def chat(self, req_user, user_req, category):
        """
        Formats the user query into the annotation service format.

        Args:
            query (str): The user query to be formatted.

        Returns:
            str: The formatted query.
        """
        user_profile = self.user_service.get_user_profile(req_user)
        
        if user_req == "/recommend":
            pass
        elif category is None:
            return self.generate_response(reply=user_req, user_profile=user_profile)
        else:
            return self.generation_questions(category=category)


