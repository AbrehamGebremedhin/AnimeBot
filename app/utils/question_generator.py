import os
import json
import requests
import time
from functools import lru_cache
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.output_parsers import JsonOutputParser
from tenacity import retry, stop_after_attempt, wait_exponential
from langchain_core.messages import AIMessage

load_dotenv(r'.env')    
class QuestionGeneration:
    def __init__(self):
        api_key = os.getenv("GEMINI_API_KEY")
        os.environ["GOOGLE_API_KEY"] = api_key
        
        self.llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash-lite", temperature=0.2)
        self.parser = JsonOutputParser()
        
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    @lru_cache(maxsize=32)
    def generate_question(self) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        """
        Generates anime recommendation questionnaire using Gemini API.

        Returns:
            Dict: A dictionary containing categorized questions in JSON format.
        """
        prompt = """
        Generate a comprehensive anime recommendation questionnaire with the following categories:

        1. user_info: Basic information about the user
        2. preferences: Anime preferences and genres
        3. hobbies: User's non-anime interests
        4. story_preferences: Questions about storyline preferences
        5. art_style_and_animation: Questions about visual style
        6. character_types: Character preference questions
        7. maturity_and_content: Content restriction preferences
        8. cultural_and_thematic_interests: Cultural themes of interest
        9. mood_and_emotional_preferences: Mood and pacing preferences

        Format as JSON with these exact categories as keys. Each category should have 2-3 questions of either multiple-choice or open-ended types.

        JSON structure:
        {
          "user_info": [
            {
              "question": "How old are you?",
              "type": "multiple-choice",
              "Options": ["18-24", "25-34", "35-44", "45-54", "55+"],
              "var_name": "user_age"
            },
            {
              "question": "How would you describe your experience with anime?",
              "type": "multiple-choice",
              "Options": ["beginner", "intermediate", "advanced"],
              "var_name": "user_experience"
            }
          ],
          "preferences": [
            {
              "question": "What are some of your absolute favorite animes?",
              "type": "open-ended",
              "Options": [],
              "var_name": "favorite_animes"
            },
            {
              "question": "Which genres do you enjoy watching?",
              "type": "open-ended",
              "Options": [],
              "var_name": "favorite_genres"
            }
          ]
        }

        Requirements:
        - Use clear, direct questions
        - For multiple-choice questions, provide 3-5 options
        - For open-ended questions, use empty Options array
        - VAR_NAME GUIDELINES:
          - Always use snake_case (lowercase with underscores)
          - Make var_names descriptive of the question content
          - Follow consistent patterns:
            - For preference questions: "favorite_X", "disliked_X", "preferred_X"
            - For boolean questions: "has_X", "wants_X", "prefers_X"
            - For user info: "user_X"
          - Keep var_names concise but clear
          - Never use spaces or special characters
        - Cover all 9 categories
        - Format as valid JSON with double quotes
        - No comments or explanation outside JSON
        """

        try:
            response = self.llm.invoke(prompt)
            
            # Extract content from AIMessage
            if isinstance(response, AIMessage):
                content = response.content
            else:
                content = str(response)
                
            # Parse the content as JSON if it's not already a dict
            if isinstance(content, str):
                try:
                    # Handle the case where content might be wrapped in markdown code blocks
                    if "```json" in content:
                        json_part = content.split("```json")[1].split("```")[0].strip()
                        return json.loads(json_part)
                    elif "```" in content:
                        json_part = content.split("```")[1].split("```")[0].strip()
                        return json.loads(json_part)
                    else:
                        return json.loads(content)
                except json.JSONDecodeError:
                    # Return the raw string if JSON parsing fails
                    return {"error": "Failed to parse JSON", "raw_content": content}
            
            return content
        except Exception as e:
            print(f"Error generating questions: {e}")
            return None
