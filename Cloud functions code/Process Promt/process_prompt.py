import os
import openai
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1

app = Flask(__name__)
openai.api_key = os.getenv('AZURE_OPENAI_API_KEY')

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(os.getenv('GCP_PROJECT'), os.getenv('PUBSUB_TOPIC'))

def process_prompt(prompt):
    response = openai.Completion.create(engine="davinci", prompt=f"Extract relevant keywords from this prompt: {prompt}", max_tokens=50)
    keywords = response.choices[0].text.strip().split(", ")
    return keywords

@app.route('/process_prompt', methods=['POST'])
def handle_process_prompt():
    data = request.get_json()
    prompt = data['prompt']
    keywords = process_prompt(prompt)
    publisher.publish(topic_path, data=str(keywords).encode('utf-8'))
    return jsonify({"status": "keywords published"})

if __name__ == '__main__':
    app.run(debug=True)