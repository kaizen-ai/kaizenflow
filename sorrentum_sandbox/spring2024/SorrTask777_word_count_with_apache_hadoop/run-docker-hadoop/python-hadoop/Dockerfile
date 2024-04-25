# Use a base Python image
FROM python:3.9-slim

# Set environment variables for NLTK data directory
ENV NLTK_DATA=/usr/local/share/nltk_data

# Set the working directory in the container
WORKDIR /usr/src/app

# Install NLTK and download the English stop words corpus
RUN pip install nltk && python -c "import nltk; nltk.download('stopwords')"

# Copy the mapper and reducer scripts into the container
COPY mapper.py reducer.py text_sample.txt ./

# Set the entrypoint script as executable
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the entrypoint for the container
ENTRYPOINT ["/entrypoint.sh"]
