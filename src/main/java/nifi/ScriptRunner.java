/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nifi;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import nifi.script.ExecuteScript;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * Created by mburgess on 4/28/16.
 */
public class ScriptRunner {

    public static String DASHED_LINE = "---------------------------------------------------------";

    private static TestRunner runner;

    private static boolean outputAttributes = false;
    private static boolean outputContent = false;
    private static boolean outputSuccess = true;
    private static boolean outputFailure = false;
    private static String inputFileDir = "";
    private static String scriptPath = "";
    private static String modulePaths = "";
    private static int numFiles = 0;

    public static void main(String[] args) {


        // Expecting a single arg with the filename, will figure out language from file extension
        if (args == null || args.length < 1) {
            System.err.println("Usage: java -jar nifi-script-tester-<version>-all.jar [options] <script file>");
            System.err.println(" Where options may include:");
            System.err.println("   -success            Output information about flow files that were transferred to the success relationship. Defaults to true");
            System.err.println("   -failure            Output information about flow files that were transferred to the failure relationship. Defaults to false");
            System.err.println("   -no-success         Do not output information about flow files that were transferred to the success relationship. Defaults to false");
            System.err.println("   -content            Output flow file contents. Defaults to false");
            System.err.println("   -attrs              Output flow file attributes. Defaults to false");
            System.err.println("   -all-rels           Output information about flow files that were transferred to any relationship. Defaults to false");
            System.err.println("   -all                Output content, attributes, etc. about flow files that were transferred to any relationship. Defaults to false");
            System.err.println("   -input=<directory>  Send each file in the specified directory as a flow file to the script");
            System.err.println("   -modules=<paths>    Comma-separated list of paths (files or directories) containing script modules/JARs");
            System.exit(1);
        }

        // Reset option flags
        outputAttributes = false;
        outputContent = false;
        outputSuccess = true;
        outputFailure = false;
        scriptPath = "";
        inputFileDir = "";
        numFiles = 0;

        for (String arg : args) {
            if ("-all".equals(arg)) {
                outputAttributes = true;
                outputContent = true;
                outputSuccess = true;
                outputFailure = true;
            } else if ("-all-rels".equals(arg)) {
                outputSuccess = true;
                outputFailure = true;
            } else if ("-success".equals(arg)) {
                outputSuccess = true;
            } else if ("-failure".equals(arg)) {
                outputFailure = true;
            } else if ("-content".equals(arg)) {
                outputContent = true;
            } else if ("-no-success".equals(arg)) {
                outputSuccess = false;
            } else if ("-attrs".equals(arg)) {
                outputAttributes = true;
            } else if (arg.startsWith("-input=")) {
                inputFileDir = arg.substring("-input=".length());
            } else if (arg.startsWith("-modules=")) {
                modulePaths = arg.substring("-modules=".length());
            } else {
                scriptPath = arg;
            }
        }
        File scriptFile = new File(scriptPath);
        if (!scriptFile.exists()) {
            System.err.println("Script file not found: " + args[0]);
            System.exit(2);
        }

        String extension = scriptPath.substring(scriptPath.lastIndexOf(".") + 1).toLowerCase();
        String scriptEngineName = "Groovy";
        if ("js".equals(extension)) {
            scriptEngineName = "ECMAScript";
        } else if ("py".equals(extension)) {
            scriptEngineName = "python";
        } else if ("rb".equals(extension)) {
            scriptEngineName = "ruby";
        } else if ("lua".equals(extension)) {
            scriptEngineName = "lua";
        }

        final ExecuteScript executeScript = new ExecuteScript();
        // Need to do something to initialize the properties, like retrieve the list of properties
        executeScript.getSupportedPropertyDescriptors();

        runner = TestRunners.newTestRunner(executeScript);

        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, scriptEngineName);
        runner.setProperty(ExecuteScript.SCRIPT_FILE, scriptPath);
        runner.setProperty(ExecuteScript.MODULES, modulePaths.isEmpty() ? "src/test/resources/modules" : modulePaths);

        runner.assertValid();
        try {
            if (inputFileDir.isEmpty()) {
                int available = System.in.available();
                if (available > 0) {
                    InputStreamReader isr = new InputStreamReader(System.in);
                    char[] input = new char[available];
                    isr.read(input);
                    runner.enqueue(new String(input));
                }
            } else {
                // Read flow files in from the folder
                Path inputFiles = Paths.get(inputFileDir);
                if (!Files.exists(inputFiles)) {
                    System.err.println("Input file directory does not exist: " + inputFileDir);
                    System.exit(3);
                }
                if (!Files.isDirectory(inputFiles)) {
                    System.err.println("Input file location is not a directory: " + inputFileDir);
                    System.exit(4);
                }
                Files.walkFileTree(inputFiles, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (attrs.isRegularFile()) {
                            runner.enqueue(Files.readAllBytes(file), new HashMap<String, String>() {{
                                put("filename", file.getFileName().toString());
                            }});
                            numFiles++;
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        if (numFiles > 1) {
            runner.run(numFiles);
        } else {
            runner.run();
        }
        if (outputSuccess) {
            outputFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        }

        if (outputFailure) {
            outputFlowFilesForRelationship(ExecuteScript.REL_FAILURE);
        }
    }

    private static void outputFlowFilesForRelationship(Relationship relationship) {

        List<MockFlowFile> files = runner.getFlowFilesForRelationship(relationship);
        if (files != null) {
            for (MockFlowFile flowFile : files) {
                if (outputAttributes) {
                    final StringBuilder message = new StringBuilder();
                    message.append("Flow file ").append(flowFile);
                    message.append("\n");
                    message.append(DASHED_LINE);
                    message.append("\nFlowFile Attributes");
                    message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "entryDate", new Date(flowFile.getEntryDate())));
                    message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "lineageStartDate", new Date(flowFile.getLineageStartDate())));
                    message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "fileSize", flowFile.getSize()));
                    message.append("\nFlowFile Attribute Map Content");
                    for (final String key : flowFile.getAttributes().keySet()) {
                        message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", key, flowFile.getAttribute(key)));
                    }
                    message.append("\n");
                    message.append(DASHED_LINE);
                    System.out.println(message.toString());
                }
                if (outputContent) {
                    System.out.println(new String(flowFile.toByteArray()));
                }
                System.out.println("");
            }
            System.out.println("Flow Files transferred to " + relationship.getName() + ": " + files.size()+ "\n");
        }
    }
}

