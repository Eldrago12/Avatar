import os
import ast
import base64
from flask import Flask, request
from google.cloud import storage
from PIL import Image
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import requests
import json
from azure.identity import ClientSecretCredential
import openai

tenant_id = os.getenv('AZURE_TENANT_ID')
client_id = os.getenv('AZURE_CLIENT_ID')
client_secret = os.getenv('AZURE_CLIENT_SECRET')

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
# blob_service_client = BlobServiceClient(account_url="https://<YOUR_STORAGE_ACCOUNT_NAME>.blob.core.windows.net", credential=credential)
blob_service_client = BlobServiceClient(account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.blob.core.windows.net", credential=credential)
app = Flask(__name__)


# blob_service_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_STORAGE_CONNECTION_STRING'))
container_client = blob_service_client.get_container_client(os.getenv('AZURE_CONTAINER_NAME'))
api_gateway_url = os.getenv('API_GATEWAY_URL')
openai.api_key = os.getenv('AZURE_OPENAI_API_KEY')

def compose_image(image_urls):
    images = [Image.open(BytesIO(requests.get(url).content)) for url in image_urls]
    base_image = images[0]
    for img in images[1:]:
        base_image.paste(img, (0, 0), img)
    buffer = BytesIO()
    base_image.save(buffer, format="PNG")
    buffer.seek(0)
    return buffer

def upload_image_to_blob(buffer):
    blob_client = container_client.get_blob_client("composed_image.png")
    blob_client.upload_blob(buffer, overwrite=True)
    return blob_client.url

def publish_to_api_gateway(image_url):
    headers = {'Content-Type': 'application/json'}
    data = {'image_url': image_url}
    response = requests.post(api_gateway_url, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print(f"Failed to publish to API Gateway: {response.status_code}, {response.text}")
    else:
        print("Successfully published to API Gateway")

def generate_image_from_prompt(prompt):
    response = openai.Image.create(
        prompt=prompt,
        n=1,
        size="512x512"
    )
    image_url = response['data'][0]['url']
    image_content = requests.get(image_url).content
    buffer = BytesIO(image_content)
    return buffer

@app.route('/pubsub_callback', methods=['POST'])
def pubsub_callback():
    envelope = request.get_json()
    if not envelope:
        return ('Bad Request: no Pub/Sub message received', 400)
    if not isinstance(envelope, dict) or 'message' not in envelope:
        return ('Bad Request: invalid Pub/Sub message format', 400)
    
    pubsub_message = envelope['message']
    if isinstance(pubsub_message, dict) and 'data' in pubsub_message:
        data = ast.literal_eval(base64.b64decode(pubsub_message['data']).decode('utf-8'))
        image_urls = data.get('image_urls', [])
        prompt = data.get('prompt', '')
        
        if image_urls:
            image_buffer = compose_image(image_urls)
        else:
            image_buffer = generate_image_from_prompt(prompt)
        
        image_url = upload_image_to_blob(image_buffer)
        publish_to_api_gateway(image_url)
    
    return ('', 204)

if __name__ == '__main__':
    app.run(debug=True)