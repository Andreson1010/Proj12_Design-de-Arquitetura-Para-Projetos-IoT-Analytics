# system-control.py
#uvicorn system-control:app --reload --port 8003
# http://localhost:8003/flow-status
from fastapi import FastAPI
from kafka import KafkaConsumer
import threading
import json
from joblib import load
import pandas as pd

# Modelo de Previsão carregado
model = load('clf.joblib')

app = FastAPI()

# Variável global para armazenar o sistema de controle
control_status = {"Adjust the flow": False}

# Configurações do consumidor Kafka
KAFKA_TOPIC = "system-control"
KAFKA_SERVER = "localhost:9092"

def consume_kafka_data():
    global control_status
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data = message.value
        # Processa os dados para regulagem de fluxo
        try:
            input_data = pd.DataFrame([data.values()], columns=data.keys())
            prediction = model.predict(input_data)
            
            if prediction[0] == 3 :
                control_status["Adjust the flow"] = True
                print("Adjust the flow to 80%")
            else:
                control_status["Adjust the flow"] = False
                print("Normal Flow")
            
        except Exception as e:
            print(f"Error processing message: {e}")

@app.on_event("startup")
def startup_event():
    # Inicia o consumidor Kafka em uma thread separada
    threading.Thread(target=consume_kafka_data, daemon=True).start()
    
@app.get("/control-status")
def get_control_status():
    return control_status