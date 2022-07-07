from abc import ABC, abstractmethod
from typing import Callable, Any, TypedDict
try:
    from ..MqttComm import Subscriber, Publisher, Message, MqttComm
except:
    from MqttComm import Subscriber, Publisher, Message, MqttComm
import uuid
try:
    from ..serializers.AvroHelper import AvroHelper
except:
    from serializers.AvroHelper import AvroHelper
from queue import Empty, Queue
import threading


class BaseReply(ABC):

    def __init__(self, project: str, service: str,
                 method: str, qos: int,
                 deserializer: AvroHelper,
                 serializer: AvroHelper,
                 publisher: Callable[[Message], None]) -> None:
        topic = f'{project}/{service}/request/{method}/+'
        self.subscriber = Subscriber(topic, qos,
                                     deserializer,
                                     self.handle_subscription)
        self.serializer = serializer
        self.publisher = publisher

    def handle_subscription(self, data: dict, topic: str, qos: int) -> None:
        # topic structure <project-name>/<service>/<request or reply>/<method name>/<uuid>
        topic_stubs = topic.split('/')
        # replace request with reply and reform string
        topic_stubs[2] = 'reply'
        topic_reply = '/'.join(topic_stubs)
        response = self.handle_message(data)
        # respond
        Publisher(topic_reply, qos,
                  self.serializer,
                  self.publisher).publish(response)

    @abstractmethod
    def handle_message(self, data: dict) -> dict:
        pass


class QueueRequest(TypedDict):
    msg: Message
    queue: Queue
    deserializer: AvroHelper


class Request(TypedDict):
    sub: Subscriber
    queue: Queue


class BaseRequest(ABC):

    def __init__(self, project: str,
                 service: str, request: str,
                 qos: int, serializer: AvroHelper,
                 deserializer: AvroHelper,
                 request_queue: Queue,
                 timeout: int = 2) -> None:
        self.topic = f'{project}/{service}/request/{request}'
        self.qos = qos
        self.queue = Queue()
        self.deserializer = deserializer
        self.serializer = serializer
        self.request_queue = request_queue
        self.timeout = timeout
        self._request = None

    @abstractmethod
    def send_request(self, **kwargs) -> Any:
        # expects override to put data into self._request
        if self._request is not None:
            req = self.serializer.serialize(self._request)
            try:
                self.request_queue.put(QueueRequest(
                    msg=Message(topic=self.topic,
                                qos=self.qos,
                                payload=req),
                    queue=self.queue,
                    deserializer=self.deserializer))
                self._request = None
                return self._wait_for_response()
            except (TypeError, RuntimeWarning, Exception) as e:
                self._request = None
                return e
        else:
            raise RuntimeWarning('No request to send')

    def _wait_for_response(self) -> dict:
        try:
            return self.queue.get(block=True, timeout=self.timeout)
        except Empty:
            raise RuntimeWarning('Timed-out')


class RequestManager():
    '''
    Handles the sending of requests and relaying responses
    '''

    def __init__(self, comm: MqttComm) -> None:
        self.comm = comm
        self.queue = Queue()
        self._active = False
        self.requests: dict = {}
        self._logger = comm._logger
        self._queue_thread = threading.Thread(target=self._consume_queue)

    def _consume_queue(self) -> None:
        while self._active:
            try:
                self._send_request(req=self.queue.get(
                    block=True, timeout=0.5))
            except Empty:
                # expected continue
                pass
            except RuntimeWarning as e:
                # self._logger.warn(e)
                pass

    def _send_request(self, req: QueueRequest) -> None:
        # add uuid
        uid = str(uuid.uuid4())
        req['msg']['topic'] = f"{req['msg']['topic']}/{uid}"
        # topic structure <project-name>/<service>/<request or reply>/<method name>/<uuid>
        topic_stubs = req['msg']['topic'].split('/')
        topic_stubs[2] = 'reply'
        topic_reply = '/'.join(topic_stubs)
        sub = Subscriber(topic_reply,
                         req['msg']['qos'],
                         req['deserializer'],
                         self._handle_response)
        self.requests[uid] = Request(sub=sub, queue=req['queue'])
        # subscribe for response
        self.comm.add_subscription(sub)
        self._logger.info('Sending request id: %s', uid)
        try:
            self.comm.send_data(req['msg'])
        except RuntimeWarning as e:
            # server not ready. Log and bubble up warning and remove request
            self._logger.warn(
                'Unable to send request id: %s, not connected to server', uid)
            self.requests.pop(uid)
            self.comm.unsubscribe(sub)
            raise e

    def _handle_response(self, data: dict, topic: str, qos: int) -> None:
        topic_stubs = topic.split('/')
        # last stub is the uuid
        if topic_stubs[-1] in self.requests:
            self._logger.info('Received Response for: %s', topic_stubs[-1])
            rsp: Request = self.requests.pop(topic_stubs[-1])
            self.comm.unsubscribe(rsp['sub'])
            rsp['queue'].put(data)
        else:
            self._logger.warn('Received unexpected response id: %s',
                         topic_stubs[-1])

    def start(self) -> None:
        self._active = True
        self._queue_thread.start()

    def stop(self) -> None:
        self._active = False


class HelloReq(BaseRequest):

    def __init__(self, project: str,
                 service: str, qos: int, serializer: AvroHelper,
                 deserializer: AvroHelper,
                 request_queue: Queue, timeout: int = 2) -> None:
        super().__init__(project, service, 'Hello', qos,
                         serializer, deserializer, request_queue, timeout)

    def send_request(self, msg: str, **kwargs) -> Any:
        self._request = {'msg': msg}
        return super().send_request(**kwargs)


class HelloRep(BaseReply):

    def __init__(self, project: str, service: str,
                 qos: int, deserializer: AvroHelper,
                 serializer: AvroHelper,
                 publisher: Callable[[Message], None]) -> None:
        super().__init__(project, service, 'Hello',
                         qos, deserializer, serializer, publisher)

    def handle_message(self, data: dict):
        msg = data['msg']
        return {'msg': f'Hello, {msg}'}
