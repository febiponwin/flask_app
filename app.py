from flask import Flask, request, render_template, jsonify
import pandas as pd
import json
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

# Flask app setup
app = Flask(__name__)

# AWS IoT configuration
ENDPOINT = "your-aws-iot-endpoint.amazonaws.com"
CLIENT_ID = "FlaskAppClient"
TOPIC = "csvToJson"

CERT_DIR = "./certs/"
CA_PATH = CERT_DIR + "AmazonRootCA1.pem"
CERT_PATH = CERT_DIR + "device.pem.crt"
PRIVATE_KEY_PATH = CERT_DIR + "private.pem.key"

# AWS IoT MQTT Client setup
mqtt_client = AWSIoTMQTTClient(CLIENT_ID)
mqtt_client.configureEndpoint(ENDPOINT, 8883)
mqtt_client.configureCredentials(CA_PATH, PRIVATE_KEY_PATH, CERT_PATH)
mqtt_client.configureOfflinePublishQueueing(-1)
mqtt_client.configureDrainingFrequency(2)
mqtt_client.configureConnectDisconnectTimeout(10)
mqtt_client.configureMQTTOperationTimeout(5)

mqtt_client.connect()

@app.route('/')
def upload_page():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return "No file part", 400
    
    file = request.files['file']
    if file.filename == '':
        return "No selected file", 400
    
    # Convert CSV to JSON
    try:
        df = pd.read_csv(file)
        json_data = df.to_json(orient='records')
        mqtt_client.publish(TOPIC, json_data, 1)  # QoS 1
        return jsonify({"message": "Data published to AWS IoT Core", "data": json.loads(json_data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
