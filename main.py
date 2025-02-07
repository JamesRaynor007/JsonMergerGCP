import json
import os
from flask import Flask, jsonify
from google.cloud import storage

app = Flask(__name__)

@app.route('/merge-json', methods=['GET'])
def merge_json():
    # Configuración
    bucket_name = os.environ.get('data_lake_grupo3')  # Cambia 'BUCKET_NAME' por el nombre que le diste
    folder_name = os.environ.get('metadata')  # Cambia 'FOLDER_NAME' por el nombre que le diste

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
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
