import os
import json
import logging
from flask import Flask, jsonify
from google.cloud import storage

app = Flask(__name__)

# Configuración básica del logging
logging.basicConfig(level=logging.INFO)

@app.route('/merge-json', methods=['GET'])
def merge_json():
    try:
        # Configuración de las variables de entorno
        bucket_name = os.environ.get('data_lake_grupo3')  
        folder_name = os.environ.get('google')  

        # Verificación de las variables de entorno
        if not bucket_name or not folder_name:
            logging.error("Las variables de entorno 'data_lake_grupo3' y 'google' deben estar configuradas.")
            return jsonify({"error": "Configuración inválida."}), 500

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=folder_name)

        merged_data = []
        folder_parts = folder_name.split('/')  
        directory_name = folder_parts[-1] if folder_parts else 'merged_data'  

        for blob in blobs:
            if blob.name.endswith('.json'):
                logging.info(f"Procesando el blob: {blob.name}")
                content = blob.download_as_text()
                json_data = json.loads(content)
                merged_data.append(json_data)

        if not merged_data:
            logging.warning("No se encontraron archivos JSON para combinar.")
            return jsonify({"error": "No se encontraron archivos JSON para combinar."}), 404

        # Guardar el archivo JSON combinado en el bucket
        merged_blob_name = os.path.join(folder_name, f'{directory_name}.json')  
        merged_blob = bucket.blob(merged_blob_name)
        merged_blob.upload_from_string(json.dumps(merged_data), content_type='application/json')

        logging.info(f"Archivo combinado guardado como: {merged_blob_name}")
        return jsonify(merged_data), 201

    except Exception as e:
        logging.error(f"Ocurrió un error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/status', methods=['GET'])
def status():
    return jsonify({"status": "OK", "message": "El servicio está funcionando correctamente."}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
