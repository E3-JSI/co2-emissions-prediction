# Use the official Python image as a base
FROM python:3.9-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory
WORKDIR /app

RUN apt-get update && apt-get install -y \
    chromium-driver \
    chromium \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    libx11-xcb1 \
    && rm -rf /var/lib/apt/lists/*

# Copy only the requirements file and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application file (adjust the filename if needed)
COPY api.py /app/api.py

# Expose the port the app runs on
EXPOSE 5001

# Command to run the application
CMD ["python", "/app/api.py"]
