FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create directory for the database with proper permissions
RUN mkdir -p /app/data && \
    chmod 777 /app/data

# Copy requirements first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Set environment to production
ENV ENV=production
ENV PYTHONUNBUFFERED=1

# Expose the port the app runs on
EXPOSE 8050

# Command to run the application
CMD ["python", "run.py"]
