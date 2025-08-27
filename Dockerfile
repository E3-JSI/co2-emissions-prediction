FROM python:3.9-slim-buster

# Set environment variables for non-interactive debconf
ENV DEBIAN_FRONTEND=noninteractive

# Install Chrome and dependencies
# Based on: https://github.com/puppeteer/puppeteer/blob/main/docs/troubleshooting.md#running-puppeteer-in-docker
RUN apt-get update && apt-get install -yq \
    chromium-browser \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libgcc1 \
    libgconf-2-4 \
    libgdk-pixbuf2.0-0 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libstdc++6 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    xdg-utils \
    --no-install-recommends

# Set display for Selenium
ENV DISPLAY=:99

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY sth/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY sth/api2.py ./sth/
COPY sth/db_setup.py ./sth/

# Set the Flask application entry point
ENV FLASK_APP=sth/api2.py

# Expose the port Flask runs on
EXPOSE 5001

# Run the Flask application
CMD ["python3", "-m", "flask", "run", "--host=0.0.0.0", "--port=5001"]
