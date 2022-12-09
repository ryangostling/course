import logging
import paho.mqtt.client as mqtt
from pykafka import KafkaClient
import time
import utils
from utils import load_config


class MqttKafkaBridge:

    def __init__(self, config, topic):
        self.mqtt_config = config
        self.kafka_config = load_config('kafka.json')
        self.topic = topic

    def _on_connect(self, client, userdata, flags, rc):
        print(f'Has connected to the topic {self.topic}')
        client.subscribe(self.topic)

    def _on_connect_fail(self, client, userdata, flags, rc):
        logging.info('fail')

    def _on_disconnect(self, client, userdata, rc):
        logging.info(f'Has disconnected from the topic {self.topic}')

    def _on_message(self, client, userdata, message):
        msg_payload = str(message.payload)
        print("Received MQTT message: ", msg_payload)
        self.kafka_producer.produce(msg_payload.encode('ascii'))
        print("KAFKA: Just published " + msg_payload + " to topic " + self.topic)

    def _setup_connection(self):
        self.mqtt_client = mqtt.Client()

        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_connect_fail = self._on_connect_fail
        self.mqtt_client.on_message = self._on_message
        self.mqtt_client.on_disconnect = self._on_disconnect
        self.mqtt_client.connect(self.mqtt_config['hostname'], self.mqtt_config['port'], self.mqtt_config['keepalive'])

        self.kafka_client = KafkaClient(hosts=f"{self.kafka_config['hostname']}:{self.kafka_config['port']}")
        self.kafka_topic = self.kafka_client.topics[self.topic]
        self.kafka_producer = self.kafka_topic.get_sync_producer()

    def run(self):
        self._setup_connection()

        while not utils.signaled_to_stop:
            self.mqtt_client.loop()
            time.sleep(1)

        self.mqtt_client.disconnect()
