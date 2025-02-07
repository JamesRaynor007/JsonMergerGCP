import json
import os
from flask import Flask, jsonify
from google.cloud import storage

app = Flask(__name__)

@app.route('/merge-json', methods=['GET'])
def merge_json():
    # Configuración
    bucket_name = os.environ.get('BUCKET_NAME')
    folder_name = os.environ.get('FOLDER_NAME')

    # Inicializa el cliente de Google Cloud Storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Lista los blobs en la carpeta especificada
    blobs = bucket.list_blobs(prefix=folder_name)
    
    merged_data = []

    for blob in blobs:
        if blob.name.endswith('.json'):
            content = blob.download_as_text()
            json_data = json.loads(content)
            merged_data.append(json_data)

    return jsonify(merged_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
