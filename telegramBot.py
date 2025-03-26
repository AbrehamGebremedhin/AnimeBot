import os
import json
import logging
import asyncio
import aiohttp  # Use aiohttp instead of requests for async
import requests
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters
)

# Load environment variables
load_dotenv('.env')

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# States for conversation handler
(
    SELECTING_CATEGORY,
    ANSWERING_QUESTIONS,
    SAVING_ANSWERS,
) = range(3)

API_BASE_URL = os.getenv("BACKEND_API_URL", "http://localhost:8000")

logging.info(f"BACKEND_API_URL: {os.getenv('BACKEND_API_URL')}")

logging.info(f"API_BASE_URL: {API_BASE_URL}")

# Question categories
CATEGORIES = [
    "user_info",
    "preferences",
    "hobbies", 
    "story_preferences",
    "art_style_and_animation",
    "character_types",
    "maturity_and_content",
    "cultural_and_thematic_interests",
    "mood_and_emotional_preferences"
]

# Helper Functions
async def get_questions_for_category(user_id: str, category: str) -> List[Dict[str, Any]]:
    """Fetch questions for a specific category from the API."""
    try:
        url = f"{API_BASE_URL}/profile/questions?user_id={user_id}&category={category}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error fetching questions: {e}")
        return []

async def update_user_profile(user_id: str, category: str, answers: Dict[str, Any]) -> bool:
    """Update user profile with answers to questions."""
    try:
        url = f"{API_BASE_URL}/profile/update"
        data = {
            "user_id": user_id,
            "category": category,
            "fields": answers
        }
        response = requests.post(url, json=data)
        response.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Error updating profile: {e}")
        return False

async def create_user_if_not_exists(user_id: str, username: str) -> bool:
    """Create a new user if one doesn't exist."""
    try:
        # Use aiohttp for async HTTP requests
        async with aiohttp.ClientSession() as session:
            # Check if user exists in both databases using the exists endpoint
            exists_url = f"{API_BASE_URL}/users/exists/{user_id}"
            
            logging.info(f"Checking if user {user_id} exists at URL: {exists_url}")
            
            try:
                async with session.get(exists_url) as response:
                    if response.status == 200:
                        response_json = await response.json()
                        logging.info(f"User exists check result: {response_json}")
                        
                        # Only return True if the user exists in both databases
                        if response_json.get("exists", False):
                            logging.info(f"User {user_id} already exists in both databases")
                            return True
                        else:
                            logging.info(f"User {user_id} not found in one or both databases - will create")
                    else:
                        response_text = await response.text()
                        logging.warning(f"Unexpected status code when checking user: {response.status}, response: {response_text[:100]}")
            except Exception as e:
                logging.error(f"Error checking if user exists: {e}")
            
            # Create new user with username as user_id
            url = f"{API_BASE_URL}/users/"
            data = {
                "username": username,
                "user_id": user_id  # Use the actual user_id
            }
            
            logging.info(f"Creating new user with data: {data} at URL: {url}")
            
            try:
                # Add more detailed error handling
                async with session.post(url, json=data) as response:
                    response_text = await response.text()
                    logging.info(f"User creation response status: {response.status}, body: {response_text}")
                    
                    # Consider both 200 and 201 as success, plus also handle cases where
                    # the API returns an error but the user was actually created
                    if response.status in (200, 201):
                        logging.info(f"Successfully created user {user_id} ({username})")
                        return True
                    else:
                        # Check if the user exists anyway despite the error
                        async with session.get(exists_url) as check_response:
                            if check_response.status == 200:
                                check_json = await check_response.json()
                                if check_json.get("exists", False):
                                    logging.info(f"User {user_id} exists despite API error - continuing")
                                    return True
                                
                        logging.error(f"Failed to create user. Status: {response.status}, Response: {response_text}")
                        return False
            except Exception as e:
                logging.error(f"Error creating user: {e}")
                
                # Even if there was an exception, check if the user was created
                try:
                    async with session.get(exists_url) as check_response:
                        if check_response.status == 200:
                            check_json = await check_response.json()
                            if check_json.get("exists", False):
                                logging.info(f"User {user_id} exists despite exception - continuing")
                                return True
                except Exception as check_error:
                    logging.error(f"Error checking if user exists after creation error: {check_error}")
                
                return False
    except Exception as e:
        logging.error(f"Error in create_user_if_not_exists: {e}")
        return False

# Command Handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the conversation and create user if needed."""
    user = update.effective_user
    user_id = str(user.id)  # Convert to string
    logging.info(f"User {user_id} started the conversation.")
    username = user.username or f"tg_user_{user_id}"
    
    await update.message.reply_text(
        f"Hi {user.first_name}! I'm your Anime Recommendation Bot. "
        "Let's set up your profile so I can recommend some great anime for you!"
    )
    
    # Create user if not exists
    success = await create_user_if_not_exists(user_id, username)
    if not success:
        await update.message.reply_text(
            "Sorry, I encountered an error setting up your account. Please try again later."
        )
        return ConversationHandler.END
    
    # Store user_id in context
    context.user_data["user_id"] = user_id
    context.user_data["current_category_index"] = 0
    context.user_data["category_answers"] = {}
    
    # Start with the first category
    return await show_category_questions(update, context)

async def show_category_questions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Show questions for the current category, starting with the first question."""
    user_id = context.user_data.get("user_id")
    category_index = context.user_data.get("current_category_index", 0)
    
    if category_index >= len(CATEGORIES):
        # All categories completed
        await send_message(update, context, 
            "Congratulations! You've completed your anime profile. "
            "Now I can give you personalized recommendations! Type /recommend to get started."
        )
        return ConversationHandler.END
    
    current_category = CATEGORIES[category_index]
    context.user_data["current_category"] = current_category
    context.user_data["category_answers"] = {}
    
    # Get questions for this category
    questions = await get_questions_for_category(user_id, current_category)
    context.user_data["questions"] = questions
    context.user_data["current_question_index"] = 0
    
    # Format category name for display
    formatted_category = current_category.replace("_", " ").title()
    
    await send_message(update, context, 
        f"📋 Category: {formatted_category} ({category_index + 1}/{len(CATEGORIES)})"
    )
    
    # Show only the first question
    return await show_next_question(update, context)

async def show_next_question(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Show the next question in the current category."""
    current_index = context.user_data.get("current_question_index", 0)
    questions = context.user_data.get("questions", [])
    
    # Check if there are any questions or if we've reached the end
    if not questions or current_index >= len(questions):
        # No more questions in this category, save answers and move to next category
        return await save_category_answers(update, context)
    
    # Get the current question
    question = questions[current_index]
    q_text = question["question"]
    q_type = question["type"]
    
    # Display question number out of total
    question_counter = f"Question {current_index + 1}/{len(questions)}: "
    
    if q_type == "multiple-choice":
        options = question.get("Options", [])
        if not options:  # Fallback check for lowercase "options"
            options = question.get("options", [])
        
        # Create inline keyboard with options
        keyboard = []
        for option in options:
            callback_data = f"q{current_index}:{option}"
            keyboard.append([InlineKeyboardButton(option, callback_data=callback_data)])
            
        reply_markup = InlineKeyboardMarkup(keyboard)
        await send_message(update, context, 
            f"{question_counter}{q_text}",
            reply_markup=reply_markup
        )
    else:  # open-ended
        await send_message(update, context, 
            f"{question_counter}{q_text}\n(Please type your answer)"
        )
        # Store the expected question to answer
        context.user_data["awaiting_response_for"] = current_index
    
    return ANSWERING_QUESTIONS

async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle button presses for multiple choice questions and show the next question."""
    query = update.callback_query
    await query.answer()
    
    # Parse the callback data (format: "q{index}:{answer}")
    data = query.data
    question_index, answer = data.split(":", 1)
    question_index = int(question_index[1:])  # Remove the 'q' prefix
    
    # Get the question
    questions = context.user_data.get("questions", [])
    if question_index < len(questions):
        question = questions[question_index]
        var_name = question.get("var_name")
        
        # Store the answer
        if var_name:
            context.user_data["category_answers"][var_name] = answer
            await query.edit_message_text(
                text=f"{question['question']}\nYour answer: {answer}"
            )
    
    # Move to next question
    context.user_data["current_question_index"] = question_index + 1
    return await show_next_question(update, context)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle text responses for open-ended questions and show the next question."""
    text = update.message.text
    
    # Get the question index we're waiting for
    question_index = context.user_data.get("awaiting_response_for")
    
    if question_index is not None:
        # Get the question
        questions = context.user_data.get("questions", [])
        if question_index < len(questions):
            question = questions[question_index]
            var_name = question.get("var_name")
            
            # Store the answer
            if var_name:
                context.user_data["category_answers"][var_name] = text
                await update.message.reply_text(f"Answer saved: {text}")
        
        # Clear the awaiting flag
        context.user_data["awaiting_response_for"] = None
        
        # Move to next question
        context.user_data["current_question_index"] = question_index + 1
        return await show_next_question(update, context)
    
    return ANSWERING_QUESTIONS

async def save_category_answers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Save answers for the current category and move to the next one."""
    user_id = context.user_data.get("user_id")
    current_category = context.user_data.get("current_category")
    answers = context.user_data.get("category_answers", {})
    
    # Update the profile via API
    success = await update_user_profile(user_id, current_category, answers)
    
    if success:
        # Move to the next category
        context.user_data["current_category_index"] += 1
        
        # Show a success message
        await send_message(update, context, "✅ Category completed! Moving to the next one...")
        
        # Show the next category
        return await show_category_questions(update, context)
    else:
        await send_message(update, context, 
            "Sorry, I encountered an error saving your answers. Let's try again."
        )
        # Try the same category again
        return await show_category_questions(update, context)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel the conversation."""
    await update.message.reply_text(
        "Profile setup canceled. You can restart anytime with /start"
    )
    return ConversationHandler.END

async def recommend(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the recommend command."""
    user_id = str(update.effective_user.id)  # Convert to string
    
    await update.message.reply_text("Finding the perfect anime for you...")
    
    try:
        # Call the recommendation API
        url = f"{API_BASE_URL}/chat/"
        data = {
            "user_id": user_id,
            "reply": "/recommend"
        }
        response = requests.post(url, json=data)
        response.raise_for_status()
        recommendations = response.json()
        
        # Check if the response has the expected structure
        if "Recommendations" in recommendations:
            animes = recommendations["Recommendations"]
        else:
            animes = recommendations  # Fallback to the direct response
        
        # Log the structure of the first recommendation for debugging
        if animes and len(animes) > 0:
            logger.info(f"First recommendation structure: {json.dumps(animes[0], indent=2)}")
        
        # Display up to 5 recommendations
        for i, anime in enumerate(animes[:5], 1):
            title = anime.get("title", "Unknown Anime")
            score = anime.get("score", "N/A")
            synopsis = anime.get("synopsis", "No synopsis available.")
            image_url = anime.get("image_url", "")
            
            # Get additional fields
            aired = anime.get("aired", "Unknown")
            status = anime.get("status", "Unknown Status")
            duration = anime.get("duration", "Unknown")
            episodes = anime.get("no_episodes", "?")
            
            # Get new fields
            rating = anime.get("rating", [])
            rating_text = rating[0] if rating and isinstance(rating, list) else "Not specified"
            
            anime_type = anime.get("type", [])
            type_text = anime_type[0] if anime_type and isinstance(anime_type, list) else "Not specified"
            
            source = anime.get("sourced_from", [])
            source_text = source[0] if source and isinstance(source, list) else "Not specified"
            
            # Handle different possible genre formats
            genres = []
            if "genres" in anime:
                # Could be a list of strings
                if isinstance(anime["genres"], list):
                    if all(isinstance(g, str) for g in anime["genres"]):
                        genres = anime["genres"]
                    # Could be a list of dicts with name field
                    elif all(isinstance(g, dict) and "name" in g for g in anime["genres"]):
                        genres = [g["name"] for g in anime["genres"]]
            # Fallback to genre field if exists
            elif "genre" in anime and isinstance(anime["genre"], list):
                genres = anime["genre"]
                
            # Join genres or provide default
            genres_text = ", ".join(genres) if genres else "Not specified"

            # Remove "[Written by MAL Rewrite]" from synopsis
            synopsis = synopsis.replace("[Written by MAL Rewrite]", "").strip()

            # Create detailed message with better formatting
            message = f"🎬 <b>{i}. {title}</b> ⭐ {score}\n\n"
            message += f"<b>Type:</b> {type_text} • <b>Rating:</b> {rating_text}\n"
            message += f"<b>Episodes:</b> {episodes} • <b>Duration:</b> {duration}\n"
            message += f"<b>Status:</b> {status}\n"
            message += f"<b>Source:</b> {source_text}\n\n"
            message += f"<b>Genres:</b> {genres_text}\n"
            message += f"<b>Aired:</b> {aired}\n\n"
            message += f"{synopsis}\n"
            
            # Create inline keyboard with action buttons
            keyboard = [
                [
                    InlineKeyboardButton("📋 More Details", callback_data=f"details_{i}_{anime.get('id', i)}"),
                    InlineKeyboardButton("📖 Full Synopsis", callback_data=f"synopsis_{i}_{anime.get('id', i)}")
                ],
                [
                    InlineKeyboardButton("🔍 Find Similar", callback_data=f"similar_{anime.get('id', i)}"),
                    InlineKeyboardButton("⭐ Add to Favorites", callback_data=f"favorite_{anime.get('id', i)}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Send image if available, then text
            if image_url:
                try:
                    await update.message.reply_photo(
                        photo=image_url,
                        caption=message,
                        reply_markup=reply_markup,
                        parse_mode="HTML"
                    )
                except Exception as img_error:
                    logger.error(f"Error sending image for {title}: {img_error}")
                    # Fallback to text-only if image fails
                    await update.message.reply_text(
                        message,
                        reply_markup=reply_markup,
                        parse_mode="HTML"
                    )
            else:
                await update.message.reply_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode="HTML"
                )
                
            # Store anime details in context for callback handling
            if "anime_details" not in context.user_data:
                context.user_data["anime_details"] = {}
                
            # Create a unique key for this anime
            anime_key = f"anime_{anime.get('id', i)}"
            context.user_data["anime_details"][anime_key] = {
                "title": title,
                "full_synopsis": synopsis,
                "score": score,
                "type": type_text,
                "rating": rating_text,
                "source": source_text,
                "genres": genres_text,
                "aired": aired,
                "status": status,
                "duration": duration,
                "episodes": episodes,
                "image_url": image_url
            }
            
            # Add small delay between messages
            await asyncio.sleep(0.5)
        await update.message.reply_text(
            "What do you think of these recommendations? You can always type /recommend to get more!"
        )
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        await update.message.reply_text(
            "Sorry, I had trouble finding recommendations right now. Please try again later."
        )

async def handle_anime_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle callback queries from anime recommendation buttons."""
    query = update.callback_query
    await query.answer()
    
    # Parse the callback data
    data = query.data.split("_")
    action = data[0]
    anime_id = data[1]
    index = data[2]
    
    # Retrieve stored details about the anime
    anime_key = f"anime_{anime_id}_{index}"
    if anime_key in context.user_data.get("anime_details", {}):
        details = context.user_data["anime_details"][anime_key]
        
        if action == "details":
            # Format detailed information
            details_message = f"🎬 <b>{details['title']}</b> ⭐ {details['score']}\n\n"
            details_message += f"⏱️ <b>Duration:</b> {details['duration']}\n"
            details_message += f"🔞 <b>Rating:</b> {details['rating']}\n"
            details_message += f"📚 <b>Source:</b> {details['source']}\n"
            details_message += f"🚩 <b>Status:</b> {details['status']}\n"
            details_message += f"📅 <b>Aired:</b> {details['aired']}\n"
            details_message += f"📺 <b>Episodes:</b> {details['episodes']}\n\n"
            details_message += f"📖 <b>Genres:</b> {details['genres']}\n\n"
            details_message += f"{details['full_synopsis']}\n"
            
            await query.message.reply_text(details_message, parse_mode="HTML")
        
        elif action == "synopsis":
            # Send the full synopsis
            await query.message.reply_text(details['full_synopsis'], parse_mode="HTML")
        
        elif action == "similar":
            await query.message.reply_text("Feature not implemented yet.")
        
        elif action == "favorite":
            await query.message.reply_text("Feature not implemented yet.")
        
    else:
        await query.message.reply_text("Sorry, I couldn't find the details for this anime.")

async def chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle regular chat messages."""
    user_id = str(update.effective_user.id)  # Convert to string
    message = update.message.text
    
    try:
        # Call the chat API
        url = f"{API_BASE_URL}/chat/"
        data = {
            "user_id": user_id,
            "reply": message
        }
        response = requests.post(url, json=data)
        response.raise_for_status()
        chat_response = response.json()
        
        if "question" in chat_response:
            await update.message.reply_text(chat_response["question"])
        else:
            await update.message.reply_text("I'm not sure how to respond to that.")
    except Exception as e:
        logger.error(f"Error in chat: {e}")
        await update.message.reply_text(
            "Sorry, I'm having trouble processing your message right now."
        )

# Helper to send messages that works with both callback queries and regular messages
async def send_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    """Send messages in a way that works with different update types."""
    if update.callback_query:
        await update.callback_query.message.reply_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

def main() -> None:
    """Start the bot."""
    # Get the token from the correct environment variable
    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    
    if not telegram_token:
        logger.error("Telegram bot token not found! Please set TELEGRAM_BOT_TOKEN in your .env file.")
        return
    
    logger.info(f"Starting bot with token: {telegram_token[:5]}...{telegram_token[-5:]}")
    
    # Create the Application with the correct token
    application = Application.builder().token(telegram_token).build()
    
    # Add conversation handler for profile setup
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            SELECTING_CATEGORY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, show_category_questions)
            ],
            ANSWERING_QUESTIONS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text),
                CallbackQueryHandler(handle_button),
            ],
            SAVING_ANSWERS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_category_answers)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="profile_setup",
        persistent=False,
    )
    
    # Add handlers to the application
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("recommend", recommend))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, chat))
    application.add_handler(CallbackQueryHandler(handle_anime_callback, pattern=r"^(details|synopsis|similar|favorite)_\d+_\d+$"))
    
    # Log API endpoint being used
    logger.info(f"Using API endpoint: {API_BASE_URL}")
    
    # Start the Bot
    application.run_polling()

if __name__ == "__main__":
    main()
