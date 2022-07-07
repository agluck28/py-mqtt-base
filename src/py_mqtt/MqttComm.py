import paho.mqtt.client as mqtt
from queue import Empty, Queue
import logging
from typing import TypedDict, Any
from collections.abc import Callable
import threading
try:
    from serializers.AvroHelper import AvroHelper
except:
    from .serializers.AvroHelper import AvroHelper


class Message(TypedDict):
    topic: str
    qos: int
    payload: bytes


class Subscriber():

    def __init__(self, topic: str, qos: int,
                 deserializer: AvroHelper,
                 callback: Callable[[dict, str, int], Any]) -> None:
        self.topic = topic
        self.qos = qos
        self.deserializer = deserializer
        self.callback = callback

    def handle_subscription(self, client: mqtt.Client,
                            userdata: Any, msg: mqtt.MQTTMessage) -> None:
        # handles the data for this subscriptions
        self.callback(self.deserializer.deserialize(msg.payload),
                      msg.topic, msg.qos)


class Publisher():

    def __init__(self, topic: str, qos: int,
                 serializer: AvroHelper,
                 publisher: Callable[[Message], None]) -> None:
        self.topic = topic
        self.qos = qos
        self.serializer = serializer
        self.publisher = publisher

    def publish(self, data: dict) -> None:
        msg_bytes = self.serializer.serialize(data=data)
        self.publisher(Message(topic=self.topic,
                               qos=self.qos, payload=msg_bytes))


FORMAT = '%(asctime)s %(levelname)s: %(message)s'


class MqttComm():
    '''
    Handles communcation to MQTT broker and relaying subscriptions \n
    to users of the library and publishing data. Async implementation \n
    that uses queues to pass in data to be sent
    '''

    def __init__(self, server: str, port: int = 1883,
                 client_id: str | None = None, user_name: str | None = None,
                 password: str | None = None,
                 subscriptions: set[Subscriber] | None = None,
                 log_level: int = logging.INFO) -> None:
        self._client = mqtt.Client(
            client_id=client_id, userdata=self.send_data)
        self._server = server
        self._port = port
        # setup logging and pass to Paho MQTT
        self._logger = logging.getLogger(__name__)
        logging.basicConfig(level=log_level, format=FORMAT)
        self._client.enable_logger(self._logger)
        if password is not None and user_name is not None:
            self._client.username_pw_set(username=user_name, password=password)
        self.subscriptions = subscriptions
        self.queue = Queue()
        self.ready = False
        self._queue_ready = False
        self._queue_thread = threading.Thread(target=self._consume_queue)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

    def _on_message(self, client: mqtt.Client,
                    userdata, message: mqtt.MQTTMessage) -> None:
        '''
        Handles any messages that are received that the client did not
        subscribe to. Currently unused, future could be health monitoring
        of this library
        '''
        self._logger.warn('Unexpected topic: %s received', message.topic)
        self._logger.warn('Unexpected message: %s', message.payload)

    def _on_connect(self, client: mqtt.Client, userdata,
                    flags: dict, rc: int) -> None:
        self._logger.info('Connected')

        match(rc):
            case 0:
                # Successful connection, subscribe to any subscriptions
                if self.subscriptions is not None:
                    for sub in self.subscriptions:
                        self._logger.info('Subscribing to: %s', sub.topic)
                        self._client.subscribe(sub.topic, sub.qos)
                        self._client.message_callback_add(
                            sub.topic, sub.handle_subscription)
                self.ready = True
            case 1:
                # connection refused, invalid protocol
                self._logger.error('Unable to connect due to invalid protocol')
                raise RuntimeError(
                    'MQTT Client unable to connect due to invalid protocol')
            case 2:
                # conn refused, invalid client ID
                self._logger.warn('Unable to connect due to invalid client ID')
                raise RuntimeError(
                    'MQTT Client unable to connect due to invalid client ID')
            case 3:
                # conn refused, server not reachable
                self._logger.warn(
                    'Unable to connect due to server not being reachable, will retry')
                # add logic to delay and retry
                pass
            case 4:
                # conn refused, bad un or pw
                self._logger.warn(
                    'Unable to connect due to bad username or password')
                raise RuntimeError(
                    'MQTT Client unable to connect due to bad username or password')
            case 5:
                # conn refused, not authorized
                self._logger.warn('Unable to connect due to no authorization')
                raise RuntimeError(
                    'MQTT Client unable to connect due to no authorization')
            case _:
                # unused values not expected
                self._logger.error(
                    'Unexpected connection response %d received', rc)
                raise RuntimeError(
                    'Unexpected connection response %d received', rc)

    def _on_disconnect(self, client: mqtt.Client, userdata, rc: int) -> None:
        self._logger.info('Disconnected')
        if rc != 0:
            self._logger.warn(
                'Unexpected disconnected from client: %d', client._client_id)

    def send_data(self, message: Message) -> None:
        # add to queue with the typed message
        if self.ready:
            self.queue.put(message)
        else:
            raise RuntimeWarning('Unable to send message. Not connected to the server')

    def add_subscription(self, subscription: Subscriber) -> None:
        self._logger.info('Subscribing to: %s', subscription.topic)
        self.subscriptions.add(subscription)
        self._client.subscribe(subscription.topic,
                               subscription.qos)
        self._client.message_callback_add(
            subscription.topic, subscription.handle_subscription)

    def unsubscribe(self, subscription: Subscriber) -> None:
        self._logger.info('Unsubscribing to: %s', subscription.topic)
        self.subscriptions.remove(subscription)
        self._client.unsubscribe(subscription.topic)

    def _publish_message(self, message: Message) -> None:
        self._client.publish(topic=message['topic'],
                             payload=message['payload'],
                             qos=message['qos'])

    def _consume_queue(self) -> None:
        while self._queue_ready:
            try:
                msg: Message = self.queue.get(block=True, timeout=0.5)
                self._publish_message(msg)
            except Empty:
                # expected, handle and continue on
                pass

    def start(self) -> None:
        # start the connection and the queue consumer
        self._queue_ready = True
        self._queue_thread.start()
        self._client.connect(self._server, self._port)
        self._client.loop_start()

    def stop(self) -> None:
        self._client.disconnect()
        self._client.loop_stop()
        self.ready = False
        self._queue_ready = False
