# Use official Python image
FROM python:3.9-slim

# Set working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY app/requirements.txt .  
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY app /app 

# Expose the port Streamlit runs on
EXPOSE 8501

# Command to run the app
CMD ["streamlit", "run", "app.py"]
