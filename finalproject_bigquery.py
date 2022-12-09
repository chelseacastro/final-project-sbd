from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"/home/chelseacastro_june/big-query_key.json"
client = bigquery.Client()

def create_dataset(dataset_id):
    dataset_ref = bigquery.DatasetReference.from_string(dataset_id, default_project=client.project)

    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "asia-southeast1"
    dataset = client.create_dataset(dataset)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

create_dataset(dataset_id="marine-embassy-366213.bigprojectsbd")

schema = [
    bigquery.SchemaField("conversation_id", "STRING"),
    bigquery.SchemaField("created_at", "STRING"),
    bigquery.SchemaField("date", "TIMESTAMP"),
    bigquery.SchemaField("timezone", "STRING"),
    bigquery.SchemaField("place", "STRING"),
    bigquery.SchemaField("tweet", "STRING"),
    bigquery.SchemaField("hashtags", "STRING", mode="REPEATED" ),
    bigquery.SchemaField("user_id", "INTEGER"),
    bigquery.SchemaField("username", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("link", "STRING"),
    bigquery.SchemaField("nlikes", "INTEGER"),
    bigquery.SchemaField("nreplies", "INTEGER"),
    bigquery.SchemaField("nretweets", "INTEGER"),
    bigquery.SchemaField("geo", "STRING"),
]

table = bigquery.Table(f"marine-embassy-366213.bigprojectsbd.twitter_info", schema=schema)
table = client.create_table(table)
print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
