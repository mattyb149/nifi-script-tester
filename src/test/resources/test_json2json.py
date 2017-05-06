import json
import java.io
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

class PyStreamCallback(StreamCallback):
    def __init__(self):
        pass
    def process(self, inputStream, outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        obj = json.loads(text)
        newObj = {
            "Range": 5,
            "Rating": obj['rating']['primary']['value'],
            "SecondaryRatings": {}
        }
        for key, value in obj['rating'].iteritems():
            if key != "primary":
                newObj['SecondaryRatings'][key] = {"Id": key, "Range": 5, "Value": value['value']}

        outputStream.write(bytearray(json.dumps(newObj, indent=4).encode('utf-8')))

flowFile = session.get()
if (flowFile != None):
    flowFile = session.write(flowFile,PyStreamCallback())
    flowFile = session.putAttribute(flowFile, "filename", flowFile.getAttribute('filename').split('.')[0]+'_translated.json')
    session.transfer(flowFile, REL_SUCCESS)