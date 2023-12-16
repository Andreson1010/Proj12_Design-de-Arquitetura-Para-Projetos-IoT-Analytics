# leak-detection.py
# uvicorn leak-detection:app --reload --port 8001
# http://localhost:8001/leak-status

from fastapi import FastAPI
from kafka import KafkaConsumer
import threading
import json
from joblib import load
import pandas as pd

# Modelo de previsão carregado
model = load('clf.joblib')

app = FastAPI()

# Variável global para armazenar o status do vazamento
leak_status = {"leak_detected": False}

# Configurações do consumidor Kafka
KAFKA_TOPIC = "leak-detection"
KAFKA_SERVER = "localhost:9092"

def consume_kafka_data():
    global leak_status
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data = message.value
        # Processa os dados de detecção de vazamento
        try:
            input_data = pd.DataFrame([data.values()], columns=data.keys())
            prediction = model.predict(input_data)
            if prediction[0] ==1 :
                leak_status["leak_detected"] = True
                print("Leak Detected")
            else:
                leak_status["leak_detected"] = False
                print("leak no detected")
                
        except Exception as e:
            print(f"Error processing message: {e}")

@app.on_event("startup")
def startup_event():
    # Inicia o consumidor Kafka em uma thread separada
    threading.Thread(target=consume_kafka_data, daemon=True).start()

@app.get("/leak-status")
def get_leak_status():
    return leak_status

