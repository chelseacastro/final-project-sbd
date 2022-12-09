import twint
import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

twint_configuration = twint.Config()
twint_configuration.Search = "investasi OR reksadana OR \"investasi properti\" OR \"investasi emas\" OR sukuk OR saham"
twint_configuration.Since = "2022-11-12"
twint_configuration.Until = "2022-11-19"
twint_configuration.Limit = 10000
twint_configuration.Pandas = True
# twint_configuration.Hide_output = True

twint.run.Search(twint_configuration)

# prepping pandas dataframe
df = twint.storage.panda.Tweets_df
df_csv = df.to_csv(r'/home/chelseacastro_june/bigproject_twitter.csv')
df_json = df.to_json(orient='records')
df_json = json.loads(df_json)


for x in df_json:
    data_twitter = json.dumps(x)
    producer.send(topic='twitter_info', value=data_twitter)
    producer.flush()
