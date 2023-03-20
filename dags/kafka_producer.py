import json
import time
from kafka import KafkaProducer
import requests

# Replace the API_KEY 
API_KEY = "Your_Own_API_Key"
MOVIE_ID_START = 100  # Change this value if you want to start from a different movie ID

REQUEST_LIMIT = 40  # TMDb API limit, adjust if necessary
TIME_WINDOW = 10  # Time window in seconds for the API limit

kafka_producer = KafkaProducer(bootstrap_servers='kafka:9092',
                               value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_movie_data(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        return None

def send_movie_data_to_kafka(movie_data):
    kafka_producer.send('tmdb_movie_data', movie_data)

def main():
    movie_id = MOVIE_ID_START
    request_count = 0
    start_time = time.time()

    # I am trying to mimic a streaming pipeline here
    while True:
        movie_data = fetch_movie_data(movie_id)
        if movie_data:
            send_movie_data_to_kafka(movie_data)
            print(f"Sent movie data for movie ID: {movie_id}")
        else:
            print(f"Failed to fetch movie data for movie ID: {movie_id}")

        movie_id += 1
        request_count += 1

        # we have to make sure we donot exceed the rate limiting setup by TMDB
        if request_count >= REQUEST_LIMIT:
            elapsed_time = time.time() - start_time
            if elapsed_time < TIME_WINDOW:
                sleep_time = TIME_WINDOW - elapsed_time
                time.sleep(sleep_time)
            start_time = time.time()
            request_count = 0

        time.sleep(1)

if __name__ == "__main__":
    main()
