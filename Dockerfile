# Use official Python image
FROM python:3.10

# Set the working directory
WORKDIR /code

# Copy your requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all your project files into the Space
COPY . .

# Expose the required Hugging Face port
EXPOSE 7860

# Start the worker application
CMD ["gunicorn", "worker:worker_app", "--bind", "0.0.0.0:7860", "--threads", "2"]
