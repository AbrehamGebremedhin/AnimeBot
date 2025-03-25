# AnimeBot Backend

A comprehensive backend service for an anime recommendation system with a conversational interface through Telegram.

## Overview

AnimeBot is an AI-powered anime recommendation system that uses natural language processing to understand user preferences and provide personalized anime suggestions. The system leverages a graph database (Neo4j) to store anime relationships and user profiles, enabling rich, context-aware recommendations.

## Features

- 🤖 **Conversational UI**: Natural dialogue flow through Telegram bot
- 💬 **AI-Powered Chat**: Gemini 2.0 integration for conversational recommendations
- 📊 **User Profiling**: Comprehensive preference collection through conversational onboarding
- 🔍 **Graph-Based Recommendations**: Neo4j graph database for relationship-based recommendations
- 🎯 **Personalized Suggestions**: Content tailored to user preferences and viewing history
- 🔄 **MyAnimeList Integration**: Real anime data through MAL API

## Technologies

- **Backend**: FastAPI, Python 3.9+
- **Databases**:
  - Neo4j (graph database for anime relationships)
  - SQLite (user management)
  - Redis (caching and session management)
- **AI**: Google Generative AI (Gemini models)
- **API Integrations**: MyAnimeList API
- **Client**: Telegram Bot API

## Usage

### Telegram Bot

1. Start a chat with your bot on Telegram
2. Use `/start` to begin the onboarding process
3. Answer questions about your anime preferences
4. Use `/recommend` to get personalized anime recommendations

### API Endpoints

The backend exposes several REST endpoints:

- **User Management**:

  - `GET /users/` - List all users
  - `POST /users/` - Create a new user

- **Profile Management**:

  - `GET /profile/questions` - Get profile questions by category
  - `POST /profile/update` - Update user profile

- **Chat**:

  - `POST /chat/` - Send a message to the chatbot

- **Health Checks**:
  - `GET /health` - Simple health check
  - `GET /health/liveness` - Liveness probe
  - `GET /health/readiness` - Readiness probe with detailed status

## License

This project is licensed under the MIT License.
