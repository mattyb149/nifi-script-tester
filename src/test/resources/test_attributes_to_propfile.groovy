import org.apache.nifi.processor.io.OutputStreamCallback

flowFile = session.get()
if(!flowFile) return
session.write(flowFile, {output ->
    output.withPrintWriter { o ->
        flowFile.attributes.each { k, v -> o.println("$k=$v") }
    }
} as OutputStreamCallback)
session.transfer(flowFile, REL_SUCCESS)