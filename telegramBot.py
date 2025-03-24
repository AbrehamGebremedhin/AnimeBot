import os
import logging
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
import requests
import json
import uuid

load_dotenv(".env")

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_BASE_URL = os.getenv("BACKEND_API_URL")

# User session storage
user_sessions = {}

# Command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a message when the command /start is issued."""
    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"
    
    # Check if user exists or create new one
    if str(user_id) not in user_sessions:
        try:
            # Create user in API
            payload = {"username": username}
            response = requests.post(f"{API_BASE_URL}/users/", json=payload)
            
            if response.status_code == 201:
                user_data = response.json()
                user_sessions[str(user_id)] = {
                    "api_user_id": user_data.get("id", str(user_id)),
                    "username": username,
                    "setup_complete": False
                }
                await update.message.reply_text(
                    f'Welcome to AnimeBot, {username}! I can help you discover anime based on your preferences. '
                    f'Use /help to see available commands or /profile to setup your profile.'
                )
            else:
                # Fallback if API call fails
                user_sessions[str(user_id)] = {
                    "api_user_id": str(user_id),
                    "username": username,
                    "setup_complete": False
                }
                await update.message.reply_text(
                    f'Welcome to AnimeBot, {username}! Some features might be limited. '
                    f'Use /help to see available commands.'
                )
        except Exception as e:
            logger.error(f"Error creating user: {str(e)}")
            await update.message.reply_text('Hi! I\'m having some trouble connecting to my brain right now. Please try again later.')
    else:
        await update.message.reply_text(f'Welcome back, {username}! How can I help you today? Use /help to see available commands.')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a message when the command /help is issued."""
    help_text = """
Available commands:
/start - Start the bot
/help - Show this help message
/health - Check API health status
/profile - Setup or update your anime preferences (multiple categories)
/profile <category> - Setup a specific preference category:
   • user_info
   • preferences
   • hobbies
   • story_preferences
   • art_style_and_animation
   • character_types
   • maturity_and_content
   • cultural_and_thematic_interests
   • mood_and_emotional_preferences
/recommend - Get anime recommendations based on your profile
/reset - Reset your conversation history

You can also directly send messages to chat with me about anime!
"""
    await update.message.reply_text(help_text)

async def health_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check API health status."""
    try:
        response = requests.get(f"{API_BASE_URL}/health")
        if response.status_code == 200:
            health_data = response.json()
            await update.message.reply_text(f"API is healthy! Status: {health_data['status']}, Service: {health_data['service']}")
        else:
            await update.message.reply_text(f"API returned status code: {response.status_code}")
    except Exception as e:
        await update.message.reply_text(f"Error connecting to API: {str(e)}")

async def profile_setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Setup or update user profile with anime preferences."""
    user_id = str(update.effective_user.id)
    
    if user_id not in user_sessions:
        await update.message.reply_text("Please use /start to initialize your session first.")
        return
    
    # Check if a category was specified
    if context.args and len(context.args) > 0:
        # If category is specified, directly proceed with that category
        category = context.args[0]
        await start_category_questions(update, context, category)
    else:
        # Show category selection menu
        keyboard = [
            [InlineKeyboardButton("Basic Info", callback_data="category_user_info")],
            [InlineKeyboardButton("General Preferences", callback_data="category_preferences")],
            [InlineKeyboardButton("Hobbies & Interests", callback_data="category_hobbies")],
            [InlineKeyboardButton("Story Preferences", callback_data="category_story_preferences")],
            [InlineKeyboardButton("Art Style & Animation", callback_data="category_art_style_and_animation")],
            [InlineKeyboardButton("Character Types", callback_data="category_character_types")],
            [InlineKeyboardButton("Maturity & Content", callback_data="category_maturity_and_content")],
            [InlineKeyboardButton("Cultural & Thematic", callback_data="category_cultural_and_thematic_interests")],
            [InlineKeyboardButton("Mood & Emotional", callback_data="category_mood_and_emotional_preferences")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "Please select a category to set up your anime preferences:",
            reply_markup=reply_markup
        )

async def handle_category_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle category selection for profile setup."""
    query = update.callback_query
    await query.answer()
    
    user_id = str(update.effective_user.id)
    data_parts = query.data.split("_", 1)
    
    if len(data_parts) < 2 or data_parts[0] != "category":
        return
    
    category = data_parts[1]
    
    # Edit the message to confirm category selection
    await query.edit_message_text(f"Setting up your profile for: {category.replace('_', ' ').title()}")
    
    # Start questions for the selected category
    await start_category_questions(update, context, category)

async def start_category_questions(update: Update, context: ContextTypes.DEFAULT_TYPE, category):
    """Start questions for a specific category."""
    user_id = str(update.effective_user.id)
    
    if user_id not in user_sessions:
        # Determine if this is from a callback query or direct message
        if update.callback_query:
            await update.callback_query.message.reply_text("Please use /start to initialize your session first.")
        else:
            await update.message.reply_text("Please use /start to initialize your session first.")
        return
    
    try:
        # Get profile questions from API for the selected category
        api_user_id = user_sessions[user_id]["api_user_id"]
        response = requests.get(f"{API_BASE_URL}/profile/questions?user_id={api_user_id}&category={category}")
        
        if response.status_code == 200:
            questions = response.json()
            
            if not questions:
                # Handle empty questions response
                if update.callback_query:
                    await update.callback_query.message.reply_text(f"No questions available for {category.replace('_', ' ').title()} category.")
                else:
                    await update.message.reply_text(f"No questions available for {category.replace('_', ' ').title()} category.")
                return
                
            # Store questions and category in user session
            user_sessions[user_id]["current_questions"] = questions
            user_sessions[user_id]["current_category"] = category
            user_sessions[user_id]["current_question_index"] = 0
            
            # Ask first question - pass the callback_query for context
            await ask_next_question(update, context)
        else:
            # Handle API error
            error_msg = f"Failed to get profile questions. API returned status code: {response.status_code}"
            if update.callback_query:
                await update.callback_query.message.reply_text(error_msg)
            else:
                await update.message.reply_text(error_msg)
    except Exception as e:
        logger.error(f"Error in profile setup: {str(e)}")
        # Handle exception differently based on update type
        error_msg = f"Error setting up profile: {str(e)}"
        if update.callback_query:
            await update.callback_query.message.reply_text(error_msg)
        else:
            await update.message.reply_text(error_msg)

async def ask_next_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ask the next profile question."""
    user_id = str(update.effective_user.id)
    
    if user_id not in user_sessions:
        # Handle error based on update type
        if update.callback_query:
            await update.callback_query.message.reply_text("Please use /start to initialize your session first.")
        else:
            await update.message.reply_text("Please use /start to initialize your session first.")
        return
    
    session = user_sessions[user_id]
    questions = session.get("current_questions", [])
    question_index = session.get("current_question_index", 0)
    
    if question_index >= len(questions):
        # All questions answered, update profile
        if update.callback_query:
            await update.callback_query.message.reply_text("Thanks for answering all the questions! Your profile has been updated.")
        else:
            await update.message.reply_text("Thanks for answering all the questions! Your profile has been updated.")
        user_sessions[user_id]["setup_complete"] = True
        return
    
    question = questions[question_index]
    question_text = question.get("question", "No question available")
    
    # Determine which message object to use
    message_obj = update.callback_query.message if update.callback_query else update.message
    
    if question.get("type") == "multiple_choice" and question.get("options"):
        # Create inline keyboard with options
        keyboard = []
        for option in question.get("options", []):
            keyboard.append([InlineKeyboardButton(option, callback_data=f"profile_{question_index}_{option}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await message_obj.reply_text(question_text, reply_markup=reply_markup)
    else:
        # Free text question
        context.user_data["awaiting_profile_answer"] = question_index
        await message_obj.reply_text(f"{question_text}\n\nPlease type your answer:")

async def handle_profile_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button selections for profile questions."""
    query = update.callback_query
    await query.answer()
    
    user_id = str(update.effective_user.id)
    data_parts = query.data.split("_", 2)
    
    if len(data_parts) < 3 or data_parts[0] != "profile":
        return
    
    question_index = int(data_parts[1])
    selected_option = data_parts[2]
    
    # Save answer
    if "answers" not in user_sessions[user_id]:
        user_sessions[user_id]["answers"] = {}
    
    question = user_sessions[user_id]["current_questions"][question_index]
    var_name = question.get("varname", f"question_{question_index}")
    
    user_sessions[user_id]["answers"][var_name] = selected_option
    user_sessions[user_id]["current_question_index"] = question_index + 1
    
    # Update profile in API with correct category
    try:
        api_user_id = user_sessions[user_id]["api_user_id"]
        category = user_sessions[user_id]["current_category"]
        
        payload = {
            "user_id": api_user_id,
            "category": category,
            "fields": {var_name: selected_option}
        }
        
        response = requests.post(f"{API_BASE_URL}/profile/update", json=payload)
        if response.status_code != 202:
            logger.error(f"Failed to update profile: {response.status_code}")
    except Exception as e:
        logger.error(f"Error updating profile: {str(e)}")
    
    # Move to next question
    await query.edit_message_text(f"You selected: {selected_option}")
    await ask_next_question(update, context)

async def get_recommendations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get anime recommendations based on user profile."""
    user_id = str(update.effective_user.id)
    
    if user_id not in user_sessions:
        await update.message.reply_text("Please use /start to initialize your session first.")
        return
    
    await update.message.reply_text("Searching for anime recommendations based on your profile. This might take a moment...")
    
    try:
        api_user_id = user_sessions[user_id]["api_user_id"]
        
        # Send recommendation request to the API
        payload = {
            "user_id": api_user_id,
            "reply": "/recommend"
        }
        
        response = requests.post(f"{API_BASE_URL}/chat/", json=payload)
        
        if response.status_code == 200:
            recommendations = response.json()
            
            if not recommendations:
                await update.message.reply_text("I couldn't find any recommendations for you. Try updating your profile with /profile.")
                return
                
            # Format and send recommendations
            message = "Here are some anime recommendations for you:\n\n"
            
            for i, anime in enumerate(recommendations[:5], 1):
                title = anime.get("title", "Unknown")
                synopsis = anime.get("synopsis", "No description available.")
                image_url = anime.get("image_url", "")
                genres = ", ".join(anime.get("genres", []))
                
                # Truncate synopsis if too long
                if len(synopsis) > 200:
                    synopsis = synopsis[:197] + "..."
                
                message += f"{i}. *{title}*\n"
                message += f"Genres: {genres}\n"
                message += f"Synopsis: {synopsis}\n\n"
                
                # Send image separately if available
                if image_url and i <= 3:  # Limit to first 3 to avoid flooding
                    await update.message.reply_photo(image_url, caption=f"{title}")
            
            await update.message.reply_text(message, parse_mode="Markdown")
        else:
            await update.message.reply_text(f"Failed to get recommendations. API returned status code: {response.status_code}")
    except Exception as e:
        logger.error(f"Error getting recommendations: {str(e)}")
        await update.message.reply_text(f"Error getting recommendations: {str(e)}")

async def reset_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset the chat session history."""
    user_id = str(update.effective_user.id)
    
    if user_id not in user_sessions:
        await update.message.reply_text("Please use /start to initialize your session first.")
        return
    
    try:
        api_user_id = user_sessions[user_id]["api_user_id"]
        
        # Send reset request to API
        payload = {
            "user_id": api_user_id,
            "reply": ""  # Empty reply typically resets the conversation
        }
        
        response = requests.post(f"{API_BASE_URL}/chat/", json=payload)
        
        if response.status_code == 200:
            await update.message.reply_text("Chat history has been reset. Let's start a fresh conversation!")
        else:
            await update.message.reply_text(f"Failed to reset chat. API returned status code: {response.status_code}")
    except Exception as e:
        logger.error(f"Error resetting chat: {str(e)}")
        await update.message.reply_text(f"Error resetting chat: {str(e)}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle user messages and send them to chat API."""
    user_id = str(update.effective_user.id)
    message_text = update.message.text
    
    # If we're waiting for a profile answer
    if user_id in user_sessions and context.user_data.get("awaiting_profile_answer") is not None:
        question_index = context.user_data["awaiting_profile_answer"]
        
        # Save the answer
        if "answers" not in user_sessions[user_id]:
            user_sessions[user_id]["answers"] = {}
        
        question = user_sessions[user_id]["current_questions"][question_index]
        var_name = question.get("varname", f"question_{question_index}")
        
        user_sessions[user_id]["answers"][var_name] = message_text
        user_sessions[user_id]["current_question_index"] = question_index + 1
        
        # Update profile in API with correct category
        try:
            api_user_id = user_sessions[user_id]["api_user_id"]
            category = user_sessions[user_id]["current_category"]
            
            payload = {
                "user_id": api_user_id,
                "category": category,
                "fields": {var_name: message_text}
            }
            
            response = requests.post(f"{API_BASE_URL}/profile/update", json=payload)
            if response.status_code != 202:
                logger.error(f"Failed to update profile: {response.status_code}")
        except Exception as e:
            logger.error(f"Error updating profile: {str(e)}")
        
        # Clear waiting state
        context.user_data["awaiting_profile_answer"] = None
        
        # Move to next question
        await update.message.reply_text(f"Thanks for your answer!")
        await ask_next_question(update, context)
        return
    
    # Regular chat message
    if user_id not in user_sessions:
        await start(update, context)
        return
    
    try:
        api_user_id = user_sessions[user_id]["api_user_id"]
        
        # Send message to chat API
        payload = {
            "user_id": api_user_id,
            "reply": message_text
        }
        
        await update.message.reply_chat_action("typing")
        response = requests.post(f"{API_BASE_URL}/chat/", json=payload)
        
        if response.status_code == 200:
            chat_response = response.json()
            # Extract the question from response
            if "question" in chat_response:
                await update.message.reply_text(chat_response["question"])
            else:
                await update.message.reply_text("I received your message, but I'm not sure how to respond.")
        else:
            await update.message.reply_text(f"Sorry, I'm having trouble processing your message. Error code: {response.status_code}")
    except Exception as e:
        logger.error(f"Error in chat: {str(e)}")
        await update.message.reply_text("Sorry, I'm having some technical difficulties right now.")

def main():
    """Start the bot."""
    # Create the Application
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("health", health_check))
    application.add_handler(CommandHandler("profile", profile_setup))
    application.add_handler(CommandHandler("recommend", get_recommendations))
    application.add_handler(CommandHandler("reset", reset_chat))
    
    # Add callback query handlers for both profile answers and category selection
    application.add_handler(CallbackQueryHandler(handle_category_selection, pattern="^category_"))
    application.add_handler(CallbackQueryHandler(handle_profile_callback, pattern="^profile_"))
    
    # Add message handler
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Start the Bot
    application.run_polling()

if __name__ == '__main__':
    main()