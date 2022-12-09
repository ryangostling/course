from datetime import datetime
from pykafka import KafkaClient
import pandas as pd
import time
import uuid

# Set Kafka config
hostname = 'localhost'
port = '9092'
sensor_id = uuid.uuid4().hex
kafka_client = KafkaClient(hosts=f'{hostname}:{port}')
kafka_topic = kafka_client.topics['air_quality_1']

data_send_interval = 2

# make random choice
if __name__ == "__main__":
    kafka_producer = kafka_topic.get_sync_producer()
    _air = pd.read_csv('data/dataset.csv', delimiter=';')
    ids = [sensor_id for i in range(_air.shape[0])]
    _air['sensor_id'] = ids

    while True:
        air = _air
        # date;time;NO2;O3;PM10;PM25;quality
        for _index in range(0, len(air)):
            air_i = air[air.index == _index]
            air_i.at[_index, 'date'] = datetime.now().strftime('%m/%d/%Y')
            air_i.at[_index, 'time'] = datetime.now().strftime('%H:%M:%S')
            columns = ['sensor_id', 'date', 'time', 'NO2', 'O3', 'PM10', 'PM25']
            print('Receiving data from MQTT:')
            print(air_i[columns])
            json_air = air_i.to_json(orient='records')[1:-1]
            kafka_producer.produce(bytes(json_air, 'utf-8'))
            time.sleep(data_send_interval)
