import json
import os
from flask import Flask, jsonify
from google.cloud import storage

app = Flask(__name__)

@app.route('/merge-json', methods=['GET'])
def merge_json():
    try:
        # Configuración
        bucket_name = os.environ.get('data_lake_grupo3')
        folder_name = os.environ.get('metadata')

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=folder_name)

        merged_data = []

        for blob in blobs:
            if blob.name.endswith('.json'):
                content = blob.download_as_text()
                json_data = json.loads(content)
                merged_data.append(json_data)

        # Guardar el archivo JSON combinado en el bucket
        merged_blob_name = os.path.join(folder_name, 'merged_data.json')
        merged_blob = bucket.blob(merged_blob_name)
        merged_blob.upload_from_string(json.dumps(merged_data), content_type='application/json')

        return jsonify(merged_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
