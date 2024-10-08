# Use the official Python 3.10 image as the base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the rest of the application code into the container
COPY . .

# Expose the port that Flask will run on
EXPOSE 5000

# Install Flask
RUN pip install -r requirements.txt

# Command to run the Flask app
CMD ["flask", "run", "--host=0.0.0.0"]
