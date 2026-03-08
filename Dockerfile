# Stage 1: Build dependencies
FROM python:3.12-slim AS builder

# Set environment variables for Python
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Create a working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Stage 2: Create the final image
FROM python:3.12-slim

# Install system dependencies for pytesseract and pdf2image
RUN apt-get update && apt-get install -y \
    curl \
    tesseract-ocr \
    tesseract-ocr-rus \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy installed packages and executables from builder stage
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy the application source code
COPY . /app

# Expose the port Uvicorn will listen on
EXPOSE 8001

# Run the application with Uvicorn
CMD ["sh", "-c", "yoyo apply --batch --database $DATABASE_URL ./migrations && uvicorn main:app --host 0.0.0.0 --port 8001"]
