from elasticsearch import Elasticsearch, helpers
from elasticsearch.client import IndicesClient
import csv

# Setting connection
ELASTIC_PASSWORD = "5e-HuoR1XfI-CEEJV1vi"

client = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", ELASTIC_PASSWORD),
    verify_certs=False
)

with open('/home/warehouse-server/tweets.csv') as f:
    reader = csv.DictReader(f)
    client.indices.delete(index='big-project', ignore=[400, 404])
    client.indices.create(index='big-project', ignore=400)
    # index_client = IndicesClient(client)
    # resp = index_client.create(index='big-project')
    helpers.bulk(client, reader, index='big-project')