from google.cloud import bigquery
from kafka import KafkaConsumer
from json import loads
import os

consumer = KafkaConsumer(
    'twitter_info',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# PROJECT_ID = "marine-embassy-366213"
# DATASET_ID = "bigprojectbd"
# TABLE_ID = "trial_table"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"/home/chelseacastro_june/big-query_key.json"
client = bigquery.Client()

rows_to_insert = []

for event in consumer:
    data = event.value
    data = loads(data)
    
    rows_to_insert = [] 

    # json mapping
    conversation_id = data['conversation_id']
    created_at = data['created_at']
    date = data['date']
    timezone = data['timezone']
    place = data['place']
    tweet = data['tweet']
    hashtags = data['hashtags']
    user_id = data['user_id']
    username = data['username']
    name = data['name']
    link = data['link']
    nlikes = data['nlikes']
    nreplies = data['nreplies']
    nretweets = data['nretweets']
    geo = data['geo']

    new_json = {
        "conversation_id" : conversation_id,
        "created_at" : created_at,
        "date" : date,
        "timezone" : timezone,
        "place" : place,
        "tweet" : tweet,
        "hashtags" : hashtags,
        "user_id" : user_id,
        "username" : username,
        "name" : name,
        "link" : link,
        "nlikes" : nlikes,
        "nreplies" : nreplies,
        "nretweets" : nretweets,
        "geo" : geo
    }

    # inserting row
    rows_to_insert.append(new_json)
    errors = client.insert_rows_json(f"marine-embassy-366213.bigprojectsbd.twitter_info", rows_to_insert)

    if errors == []:
        print("New rows have been added.")
        print(data)
    else:
        print("Encountered errors while inserting rows: ".format(errors))
