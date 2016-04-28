import org.apache.nifi.processor.io.InputStreamCallback

import java.nio.file.*

flowFile = session.get()
if(!flowFile) return
session.read(flowFile, {input ->
    //log.info("Flowfile content: "+input.text)
} as InputStreamCallback)
session.transfer(flowFile, REL_SUCCESS)