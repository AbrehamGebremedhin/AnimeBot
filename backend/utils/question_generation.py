import os
import json
import requests
from dotenv import load_dotenv
from neo4j import GraphDatabase
from langchain_ollama import OllamaLLM
from langchain_core.output_parsers import JsonOutputParser

load_dotenv(r'D:\Projects\ICog_Labs_Projects\rejuve\config.env')    
class QuestionGeneration:
    def __init__(self):
        self.llm = OllamaLLM(model="llama3.1")
        self.parser = JsonOutputParser()
        
    def annotation_service_format(self):
        """
        Formats the user query into the annotation service format.

        Args:
            query (str): The user query to be formatted.

        Returns:
            str: The formatted query.
        """
        prompt = """
            Generate questions strictly based on the following considerations, and return the result in JSON format only:

            **Considerations**:

            1. **Genre Preferences**: Focus on genres they enjoy or dislike (e.g., action, romance) and the tone they prefer (light-hearted, serious).
            2. **Favorite Anime**: Ask about anime they have enjoyed or disliked, which gives insight into their taste.
            3. **Experience Level**: Determine if they are beginners or seasoned fans, affecting recommendation types.
            4. **Story Preferences**: Ask if they prefer standalone or continuous story arcs and whether they like character-driven or plot-heavy stories.
            5. **Pacing Preferences**: Capture if they prefer fast-paced or slower-paced shows.
            6. **Maturity and Content**: Assess their comfort with mature themes (violence, gore, sexual themes).
            7. **Character and Art Style Preferences**: Address preferences for animation style (modern/classic) and character development depth.
            8. **Cultural and Thematic Interests**: Focus on cultural themes, settings (fantasy, futuristic), and mood (intense, relaxing).

            **JSON Format**:
            {
                "category": [
                    {
                        "question": "Question text",
                        "options": ["Option1", "Option2", ...],
                        "type": "Type of question" // Can be "multiple-choice", "yes/no", or "open-ended"
                    }
                ]
            }

            **Instructions**:

            1. Each question must address a specific aspect of the provided considerations.
            2. Use only double quotes for JSON keys and values, strictly following the JSON format.
            3. Provide questions in closed-ended or multiple-choice format wherever possible.
            4. Include only the JSON output - no explanations, comments, or any additional text.

            Example JSON Output:
            {
                "story_preferences": [
                    {
                        "question": "Do you prefer a continuous storyline or episodic episodes?",
                        "options": ["Continuous", "Episodic"],
                        "type": "multiple-choice"
                    },
                    {
                        "question": "How do you feel about plot complexity?",
                        "options": ["Simple", "Complex", "No Preference"],
                        "type": "multiple-choice"
                    }
                ]
            }

            Respond only with JSON.
        """

        response = self.llm.invoke(prompt)
        response = response[response.find('{'):]  # Clean response to start with JSON output
        formatted_response = self.parser.parse(response)

        return formatted_response

ques = QuestionGeneration()
print(ques.annotation_service_format())
