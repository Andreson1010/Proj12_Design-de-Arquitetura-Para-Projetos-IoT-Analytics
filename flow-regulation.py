# flow-regulation.py
#uvicorn flow-regulation:app --reload --port 8002
# http://localhost:8002/flow-status

from fastapi import FastAPI
from kafka import KafkaConsumer
import threading
import json
from joblib import load
import pandas as pd

# Modelo de Previsão carregado
model = load('clf.joblib')

app = FastAPI()

# Variável global para armazenar o status do fluxo
flow_status = {"stop_restart_flow": False}

# Configurações do consumidor Kafka
KAFKA_TOPIC = "flow-regulation"
KAFKA_SERVER = "localhost:9092"

def consume_kafka_data():
    global flow_status
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
            
            if prediction[0] == 2 :
                flow_status["stop_restart_flow"] = True # supondo que esta classe indique a necessecidade de regular o fluxo
                print("Stop Flow and Restart the flow")
            else:
                flow_status["stop_restart_flow"] = False
                print("Normal Flow")
            
        except Exception as e:
            print(f"Error processing message: {e}")

@app.on_event("startup")
def startup_event():
    # Inicia o consumidor Kafka em uma thread separada
    threading.Thread(target=consume_kafka_data, daemon=True).start()

@app.get("/flow-status")
def get_flow_status():
    return flow_status