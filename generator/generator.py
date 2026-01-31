import json
import time
import requests
from kafka import KafkaProducer

# ---------------- API CONFIG ---------------- #
API_KEY = "d5u9p91r01qtjet2gvb0d5u9p91r01qtjet2gvbg"
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

# ---------------- KAFKA PRODUCER ---------------- #
producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ---------------- DATA FETCH ---------------- #
def fetch_quote(symbol):
    try:
        response = requests.get(
            BASE_URL,
            params={"symbol": symbol, "token": API_KEY},
            timeout=5
        )
        response.raise_for_status()

        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())

        return data

    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

# ---------------- STREAM LOOP ---------------- #
def run_stream():
    while True:
        for symbol in SYMBOLS:
            quote = fetch_quote(symbol)

            if not quote:
                continue

            print(f"Producing: {quote}")
            producer.send("stock-quotes", value=quote)

        producer.flush()
        time.sleep(6)

# ---------------- ENTRY POINT ---------------- #
if __name__ == "__main__":
    run_stream()



























# #Import requirements
# import time
# import json
# import requests
# from kafka import KafkaProducer

# #Define variables for API
# API_KEY="d5u9p91r01qtjet2gvb0d5u9p91r01qtjet2gvbg"
# BASE_URL = "https://finnhub.io/api/v1/quote"
# SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

# #Initial Producer
# producer = KafkaProducer (
#     bootstrap_servers=["host.docker.internal:29092"],
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# #Retrive Data
# def fetch_quote(symbol):
#     url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#         data = response.json()
#         data["symbol"] = symbol
#         data["fetched_at"] = int (time.time())
#         return data
#     except Exception as e:
#         print(f"Error fetching {symbol}: {e}")
#         return None

# #Looping and Pushing to Stream
# while True:
#     for symbol in SYMBOLS:
#         quote = fetch_quote(symbol)
#         if quote:
#             print(f"Producing: {quote}")
#             producer.send("stock-quotes", value=quote)
#     time.sleep(6)