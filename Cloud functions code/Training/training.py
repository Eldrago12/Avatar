import os
from azure.cognitiveservices.vision.customvision.training import CustomVisionTrainingClient
from msrest.authentication import ApiKeyCredentials
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
import json

tenant_id = os.getenv('AZURE_TENANT_ID')
client_id = os.getenv('AZURE_CLIENT_ID')
client_secret = os.getenv('AZURE_CLIENT_SECRET')
training_key = os.getenv('AZURE_CUSTOM_VISION_TRAINING_KEY')
endpoint = os.getenv('AZURE_CUSTOM_VISION_ENDPOINT')
project_id = os.getenv('AZURE_CUSTOM_VISION_PROJECT_ID')
azure_storage_account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
container_name = os.getenv('AZURE_CONTAINER_NAME')  

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
blob_service_client = BlobServiceClient(account_url=f"https://{azure_storage_account_name}.blob.core.windows.net", credential=credential)
credentials = ApiKeyCredentials(in_headers={"Training-key": training_key})
trainer = CustomVisionTrainingClient(endpoint, credentials)


def upload_and_tag_images_from_blob(container_name, processed_images=None):
    if processed_images is None:
        processed_images = set()

    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()

    for blob in blob_list:
        if blob.name not in processed_images and blob.name.endswith(('png', 'jpg', 'jpeg')):
            tag_name = blob.name.replace(os.sep, '_').replace('/', '_').replace('\\', '_').rsplit('.', 1)[0]

            existing_tags = {tag.name: tag.id for tag in trainer.get_tags(project_id)}
            if tag_name not in existing_tags:
                tag = trainer.create_tag(project_id, tag_name)
                tag_id = tag.id
            else:
                tag_id = existing_tags[tag_name]
                
            blob_client = container_client.get_blob_client(blob.name)
            image_contents = blob_client.download_blob().readall()

            trainer.create_images_from_data(project_id, image_contents, tag_ids=[tag_id])
            print(f"Uploaded and tagged {blob.name} with tag {tag_name}")
            
            processed_images.add(blob.name)

    for blob in blob_list:
        if blob.name.endswith('/'):
            upload_and_tag_images_from_blob(container_name, blob.name, processed_images)


def upload_and_tag_images(request):
  container_name = request.get_json().get('container_name', 'your_container_name')
  upload_and_tag_images_from_blob(container_name)
  return json.dumps({"status": "success"}), 200