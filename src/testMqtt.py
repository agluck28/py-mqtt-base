from py_mqtt_base.MqttComm import MqttComm, Subscription
import paho.mqtt.client as mqtt
import time

def cb(client: mqtt.Client, _, msg: mqtt.MQTTMessage):
    print(msg.topic)
    print(msg.payload)

sub = Subscription(topic='test/topic', qos=1, callback=cb)

client = MqttComm('localhost', subscriptions=[sub])

client.start()

time.sleep(1)

client.send_data('test/topic', 1, b'Hello World')

time.sleep(1)

client.stop()