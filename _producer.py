import json
import logging
import signal
from threading import Thread
import mqtt_kafka_bridge
import utils
from utils import load_config


def signal_handler(signum, frame):
    utils.signaled_to_stop = True
    logging.info('Stopping the producer...')


def main_loop(config, topics):
    threads = []
    for topic in topics:
        bridge = mqtt_kafka_bridge.MqttKafkaBridge(config, topic)
        thread = Thread(target=bridge.run)
        thread.start()
        threads.append(thread)

    signal.signal(signal.SIGINT, signal_handler)

    while not utils.signaled_to_stop:
        pass

    for thread in threads:
        thread.join()


def main():
    mqtt_config = load_config('mqtt.json')
    topics_config = load_config('config.json')
    topics = json.loads(json.dumps(topics_config['topics']))

    main_loop(mqtt_config, topics)


if __name__ == '__main__':
    main()
