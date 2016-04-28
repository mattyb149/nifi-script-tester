import org.apache.nifi.processor.io.InputStreamCallback

import java.nio.file.*

flowFile = session.create()
session.transfer(flowFile, REL_FAILURE)