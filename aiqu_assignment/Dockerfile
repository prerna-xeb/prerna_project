# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80


# Install Databricks CLI
RUN pip install databricks-cli

# Configure Databricks CLI
RUN databricks configure --token
ENV DATABRICKS_HOST='https://adb-3822198821958806.6.azuredatabricks.net'
ENV DATABRICKS_TOKEN='dapi096a2c47e70f466a59f98fd00e7a6436-3'

# Run main.py when the container launches
CMD ["python3", "main.py"]