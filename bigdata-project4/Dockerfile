FROM python:3.9-slim

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y default-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install PySpark and other dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . /app

# Default command
CMD ["/bin/bash"]
