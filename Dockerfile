# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies for Selenium (Chromium and WebDriver)
# These are required for the CO2IntensityManager to scrape electricitymaps.com
RUN apt-get update && \
    apt-get install -y \
        chromium \
        chromium-driver \
        wget gnupg unzip \
        --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements.txt and install Python dependencies
COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt

# Copy the entire refactored application folder
# Ensure your local structure has an 'app' directory with __init__.py
COPY app/ ./app/

# Expose the port Flask runs on (default in your config is 5001)
EXPOSE 5001

# Set environment variables for the headless driver
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_BIN=/usr/bin/chromedriver

# Command to run the application as a module
CMD ["python", "-m", "app.main"]