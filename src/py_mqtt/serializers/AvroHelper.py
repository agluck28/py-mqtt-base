from io import BytesIO
import fastavro.schema
from fastavro.read import schemaless_reader
from fastavro.write import schemaless_writer
from fastavro.validation import validate

class AvroHelper():
    '''
    Class to help with serializing and de-serializing the data
    '''

    def __init__(self, avro_file: str) -> None:
        try:
            self.schema: fastavro.schema = fastavro.schema.load_schema(avro_file)
        except (FileExistsError, FileNotFoundError):
            raise RuntimeError('Unable to Init due to not finding schema file')

    def serialize(self, data: dict) -> bytes:
        '''
        Serializes the passed in data and validates it matches the schema
        '''
        if validate(data, self.schema):
            stream = BytesIO()
            schemaless_writer(stream, self.schema, data)
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
        stream = BytesIO(data)
        try:
            return schemaless_reader(stream, self.schema)
        except TypeError:
            #occurs when can't de-serialize
            raise RuntimeWarning('Unable to de-serialize the data')

if __name__ == '__main__':
    str = BytesIO()
    schema = fastavro.schema.load_schema('../request/Hello.avsc')
    schemaless_writer(str, schema, {'msg': 'Hello'})
    print(str.getvalue())
        

