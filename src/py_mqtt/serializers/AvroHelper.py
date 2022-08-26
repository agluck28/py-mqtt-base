from io import BytesIO, StringIO
import fastavro.schema
from fastavro.read import schemaless_reader
from fastavro.write import schemaless_writer
from fastavro.validation import validate
from fastavro import json_reader, json_writer

class AvroHelper():
    '''
    Class to help with serializing and de-serializing the data
    '''

    def __init__(self, avro_file: str, json: bool = False) -> None:
        try:
            self.schema: fastavro.schema = fastavro.schema.load_schema(avro_file)
            self._json = json
        except (FileExistsError, FileNotFoundError):
            raise RuntimeError('Unable to Init due to not finding schema file')

    def serialize(self, data: dict) -> bytes | str:
        '''
        Serializes the passed in data and validates it matches the schema
        '''
        if validate(data, self.schema):
            stream = None
            serial = None
            if not self._json:
                stream = BytesIO()
                schemaless_writer(stream, self.schema, data)
            else:
                stream = StringIO()
                json_writer(stream, self.schema, [data])
            serial = stream.getvalue()
            stream.close()
            return serial
        else:
            raise RuntimeWarning('Passed in data does not match schema')

    def deserialize(self, data: bytes) -> dict:
        '''
        De-serialize the passed in data with the schema it was \n
        inititlaized with
        '''
        stream = None
        try:
            if not self._json:
                stream = BytesIO(data)
                return schemaless_reader(stream, self.schema)
            else:
                stream = StringIO(data)
                reader = json_reader(stream, self.schema)
                #only single record, hack solution
                for rec in reader:
                    record = rec
                return record
        except TypeError:
            #occurs when can't de-serialize
            raise RuntimeWarning('Unable to de-serialize the data')

if __name__ == '__main__':
    a = AvroHelper('../request/Hello.avsc', True)
    data = a.serialize({'msg': 'Hello'})
    ab = a.deserialize(data)
    print(ab)
    # str = BytesIO()
    # schema = fastavro.schema.load_schema('../request/Hello.avsc')
    # schemaless_writer(str, schema, {'msg': 'Hello'})
    # print(str.getvalue())
        

