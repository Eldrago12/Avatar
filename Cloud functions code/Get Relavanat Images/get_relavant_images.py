import os
import ast
import base64
from flask import Flask, request, jsonify
from azure.cosmos import CosmosClient, PartitionKey
from google.cloud import pubsub_v1

app = Flask(__name__)

cosmos_client = CosmosClient(os.getenv('AZURE_COSMOS_ENDPOINT'), os.getenv('AZURE_COSMOS_KEY'))
database = cosmos_client.get_database_client('ImageDatabase')
container = database.get_container_client('ImageMetadata')

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(os.getenv('GCP_PROJECT'), os.getenv('PUBSUB_TOPIC_2'))

def get_relevant_images(keywords):
    images = []
    for keyword in keywords:
        query = "SELECT * FROM c WHERE ARRAY_CONTAINS(c.tags, @keyword)"
        parameters = [{"name": "@keyword", "value": keyword}]
        items = list(container.query_items(query=query, parameters=parameters, enable_cross_partition_query=True))
        images.extend(items)
    return images

@app.route('/pubsub_callback', methods=['POST'])
def pubsub_callback():
    envelope = request.get_json()
    if not envelope:
        return ('Bad Request: no Pub/Sub message received', 400)
    if not isinstance(envelope, dict) or 'message' not in envelope:
        return ('Bad Request: invalid Pub/Sub message format', 400)
    
    pubsub_message = envelope['message']
    if isinstance(pubsub_message, dict) and 'data' in pubsub_message:
        keywords = ast.literal_eval(base64.b64decode(pubsub_message['data']).decode('utf-8'))
        images = get_relevant_images(keywords)
        publisher.publish(topic_path, data=str(images).encode('utf-8'))
    
    return ('', 204)

if __name__ == '__main__':
    app.run(debug=True)