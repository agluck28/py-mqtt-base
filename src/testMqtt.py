from py_mqtt.MqttComm import MqttComm
from py_mqtt.serializers.AvroHelper import AvroHelper
from py_mqtt.request.PyMqttRequest import HelloRep, HelloReq, RequestManager
import time


def main(server: str, schema_file: str, msg: str):

    serializer = AvroHelper(schema_file)

    SERVER = server
    comm = MqttComm(SERVER)
    rm = RequestManager(comm)

    hello_rpy = HelloRep('pi-mqtt', 'test', 1, serializer,
                         serializer, comm.send_data)

    comm.subscriptions = {hello_rpy.subscriber}

    hello_req = HelloReq('pi-mqtt', 'test', 1, serializer,
                         serializer, rm.queue)

    comm.start()

    rm.start()
    try:
        while not comm.ready:
            pass
    except KeyboardInterrupt:
        pass
    rsp = hello_req.send_request(msg=msg)
    print(rsp)
    rm.stop()
    comm.stop()


if __name__ == '__main__':
    schema_file = './py_mqtt/request/Hello.avsc'
    SERVER = 'localhost'
    msg = 'World'
    main(SERVER, schema_file, msg)
