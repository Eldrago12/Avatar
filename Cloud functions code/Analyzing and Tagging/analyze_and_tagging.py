import os
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.cognitiveservices.vision.customvision.prediction import CustomVisionPredictionClient
from azure.cosmos import CosmosClient

tenant_id = os.getenv('AZURE_TENANT_ID')
client_id = os.getenv('AZURE_CLIENT_ID')
client_secret = os.getenv('AZURE_CLIENT_SECRET')
storage_account_url = f"https://{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.blob.core.windows.net"
custom_vision_endpoint = os.getenv('AZURE_CUSTOM_VISION_ENDPOINT')
custom_vision_project_id = os.getenv('AZURE_CUSTOM_VISION_PROJECT_ID')
custom_vision_prediction_key = os.getenv('AZURE_CUSTOM_VISION_PREDICTION_KEY')
cosmos_endpoint = os.getenv('AZURE_COSMOS_ENDPOINT')
cosmos_key = os.getenv('AZURE_COSMOS_KEY')


credential = ClientSecretCredential(tenant_id, client_id, client_secret)
blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=credential)
prediction_client = CustomVisionPredictionClient.from_prediction_key(custom_vision_endpoint, custom_vision_prediction_key)
cosmos_client = CosmosClient(cosmos_endpoint, cosmos_key)
database_name = 'ImageDatabase'
cosmos_container_name = 'ImageMetadata'
database = cosmos_client.get_database_client(database_name)
container = database.get_container_client(cosmos_container_name)

def analyze_image_and_store_metadata(container_name, image_url):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=image_url)
    blob_data = blob_client.download_blob().readall()
    results = prediction_client.classify_image(custom_vision_project_id, "Iteration8", blob_data)
    
    predictions = results.predictions
    if predictions:
        best_prediction = max(predictions, key=lambda p: p.probability)
        object_type = best_prediction.tag_name if best_prediction.probability > 0.5 else "Unknown"
    else:
        object_type = "Unknown"

    metadata = {
        'id': image_url, 
        'image_url': image_url,
        'tag': object_type
    }
    container.upsert_item(metadata, partition_key=metadata['tag'])

def analyze_blob_images(request):
    try:
        request_json = request.get_json()
        container_name = request_json.get('container_name')
        if not container_name:
            return {'error': 'container_name is required'}, 400

        container_client = blob_service_client.get_container_client(container_name)
        blobs = list(container_client.list_blobs())
        if not blobs:
            return {'error': 'No blobs found in the container'}, 404

        blob_name = blobs[0].name
        analyze_image_and_store_metadata(container_name, blob_name)
        return '', 204

    except Exception as e:
        return {'error': str(e)}, 500