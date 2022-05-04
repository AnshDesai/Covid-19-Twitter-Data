from pymongo import MongoClient
from kafka import KafkaConsumer
import json

topic_name = "DataStream"

consumer = KafkaConsumer(
    topic_name,
     bootstrap_servers=['127.0.0.1:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     auto_commit_interval_ms =  5000,
     fetch_max_bytes = 128,
     max_poll_records = 100,

     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Setup MongoDB client to connect to the database
client = MongoClient()
db = client['Tweets']

# Processing through each message received
for message in consumer:
    #Since we are decoding the byte string into utf-8, we need a try catch block as certain illegal characters can throw an error and stop processing
    try:
        # print(message)
        tweets = json.loads(json.dumps(message.value))
        result = db.Tweets.insert_one(tweets)
    except:
        continue