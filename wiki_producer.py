import requests
import json
from kafka import KafkaProducer

URL = "https://stream.wikimedia.org/v2/stream/recentchange"

headers = {
    "User-Agent": "Mozilla/5.0",   # IMPORTANT
    "Accept": "text/event-stream"
}

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

response = requests.get(URL, headers=headers, stream=True)

print("Connected to Wikimedia stream...")   # DEBUG

for line in response.iter_lines(decode_unicode=True):
    if line:
        # DEBUG: print raw lines (temporary)
        print("RAW:", line)

        if line.startswith("data: "):
            try:
                data = json.loads(line[6:])
                producer.send("wiki_edits", value=data)

                print("Sent:", data.get("title"))

            except Exception as e:
                print("Error:", e)
