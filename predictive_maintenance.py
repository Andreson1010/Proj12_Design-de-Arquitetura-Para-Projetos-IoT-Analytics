# predictive_maintenance.py
#uvicorn predictive_maintenance:app --reload --port 8004
# http://localhost:8004/maintenance-status
from fastapi import FastAPI
from kafka import KafkaConsumer
import threading
import json
from joblib import load
import pandas as pd

# Modelo de Previsão carregado
model = load('clf.joblib')

app = FastAPI()

maintenance_status = {"Required maintenance": False}

# Configurações do consumidor Kafka
KAFKA_TOPIC = "predictive-maintenance"
KAFKA_SERVER = "localhost:9092"

def consume_kafka_data():
    global maintenance_status
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data = message.value
        try:
            input_data = pd.DataFrame([data.values()], columns=data.keys())
            prediction = model.predict(input_data)

            if prediction[0] in [4, 5, 6]:
                maintenance_status["Required maintenance"] = True# Supondo que estas classes indiquem necessidade de manutenção
                print("Required maintenance")
            else:
                maintenance_status["Required maintenance"] = False
                print("System operating normally")
         

        except Exception as e:
            print(f"Error processing message: {e}")

@app.on_event("startup")
def startup_event():
    # Inicia o consumidor Kafka em uma thread separada
    threading.Thread(target=consume_kafka_data, daemon=True).start()
    

@app.get("/maintenance-status")
def get_maintenance_status():
    return maintenance_status