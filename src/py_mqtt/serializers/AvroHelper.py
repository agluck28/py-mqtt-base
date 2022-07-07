import avro.schema
from avro.io import DatumReader, DatumWriter, BinaryDecoder, BinaryEncoder, validate
from io import BytesIO


class AvroHelper():
    '''
    Class to help with serializing and de-serializing the data
    '''

    def __init__(self, avro_file: str) -> None:
        try:
            with open(avro_file, 'rb') as f:
                self.schema: avro.schema.Schema = avro.schema.parse(f.read())
            self._datum_writer = DatumWriter(self.schema)
            self._datum_reader = DatumReader(self.schema)
        except (FileExistsError, FileNotFoundError):
            raise RuntimeError('Unable to Init due to not finding schema file')

    def serialize(self, data: dict) -> bytes:
        '''
        Serializes the passed in data and validates it matches the schema
        '''
        if validate(self.schema, data):
            stream = BytesIO()
            encoder = BinaryEncoder(stream)
            self._datum_writer.write(data, encoder)
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
        decoder = BinaryDecoder(stream)
        try:
            return self._datum_reader.read(decoder)
        except TypeError:
            #occurs when can't de-serialize
            raise RuntimeWarning('Unable to de-serialize the data')
        

