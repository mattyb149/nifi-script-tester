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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Unit tests for the ScriptRunner class
 */
public class ScriptRunnerTest {
    private InputStream stdin;

    @Before
    public void setUp() throws Exception {
        stdin = System.in;
    }

    @After
    public void tearDown() throws Exception {
        System.setIn(stdin);
    }

    @Test
    public void testBasicJavascript() throws Exception {
        ScriptRunner.main(new String[]{"src/test/resources/test_basic.js"});
    }

    @Test
    public void testReadInputNoInput() throws Exception {
        ScriptRunner.main(new String[]{"src/test/resources/test_read_input.groovy"});
    }

    @Test
    public void testReadInput() throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream("Hello World!".getBytes());
        System.setIn(bais);
        ScriptRunner.main(new String[]{"src/test/resources/test_read_input.groovy"});
    }

    @Test
    public void testRouteToFailure() throws Exception {
        ScriptRunner.main(new String[]{"-failure -no-success", "src/test/resources/test_route_to_fail.groovy"});
    }

    @Test
    public void testOutputAll() throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream("Hello World!".getBytes());
        System.setIn(bais);
        ScriptRunner.main(new String[]{"-all", "src/test/resources/test_read_input.groovy"});
    }

    @Test
    public void testOutputAllWithInputDir() throws Exception {

        ScriptRunner.main(new String[]{"-all", "-input=src/test/resources/input_files", "src/test/resources/test_read_input.groovy"});
    }

    @Test
    public void testReadWrite() throws Exception {
        System.setIn(new FileInputStream("src/test/resources/input_files/jolt.json"));
        ScriptRunner.main(new String[]{"-all", "src/test/resources/test_json2json.groovy"});
    }

}