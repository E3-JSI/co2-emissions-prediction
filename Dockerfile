FROM python:3.9-slim

WORKDIR /app

# Install system dependencies for Selenium (Chromium and WebDriver)
RUN apt-get update && \
    apt-get install -y \
        chromium \
        chromium-driver \
        wget gnupg unzip \
        --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements.txt and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY api2.py .

# Expose the port Flask runs on
EXPOSE 5001

# Command to run the application
CMD ["python", "api2.py"]
