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
package nifi.script;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.Relationship;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class contains variables and methods common to scripting processors
 */
abstract class AbstractScriptProcessor extends AbstractSessionFactoryProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully processed")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be processed")
            .build();

    public static PropertyDescriptor SCRIPT_ENGINE;

    public static final PropertyDescriptor SCRIPT_FILE = new PropertyDescriptor.Builder()
            .name("Script File")
            .required(false)
            .description("Path to nifi.script file to execute. Only one of Script File or Script Body may be used")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(true)
            .build();


    public static final PropertyDescriptor MODULES = new PropertyDescriptor.Builder()
            .name("Module Directory")
            .description("Comma-separated list of paths to files and/or directories which contain modules required by the nifi.script.")
            .required(false)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(false)
            .build();

    // A map from engine name to a custom configurator for that engine
    final Map<String, ScriptEngineConfigurator> scriptEngineConfiguratorMap = new ConcurrentHashMap<>();
    final AtomicBoolean isInitialized = new AtomicBoolean(false);

    private Map<String, ScriptEngineFactory> scriptEngineFactoryMap;
    String scriptEngineName;
    String scriptPath;
    String[] modules;
    List<PropertyDescriptor> descriptors;

    BlockingQueue<ScriptEngine> engineQ = null;


    /**
     * This method creates all resources needed for the nifi.script processor to function, such as nifi.script engines,
     * nifi.script file reloader threads, etc.
     */
    void createResources() {
        descriptors = new ArrayList<>();
        // The following is required for JRuby, should be transparent to everything else.
        // Note this is not done in a ScriptEngineConfigurator, as it is too early in the lifecycle. The
        // setting must be there before the factories/engines are loaded.
        System.setProperty("org.jruby.embed.localvariable.behavior", "persistent");

        // Create list of available engines
        ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        List<ScriptEngineFactory> scriptEngineFactories = scriptEngineManager.getEngineFactories();
        if (scriptEngineFactories != null) {
            scriptEngineFactoryMap = new HashMap<>(scriptEngineFactories.size());
            List<AllowableValue> engineList = new LinkedList<>();
            for (ScriptEngineFactory factory : scriptEngineFactories) {
                engineList.add(new AllowableValue(factory.getLanguageName()));
                scriptEngineFactoryMap.put(factory.getLanguageName(), factory);
            }

            // Sort the list by name so the list always looks the same.
            Collections.sort(engineList, new Comparator<AllowableValue>() {
                @Override
                public int compare(AllowableValue o1, AllowableValue o2) {
                    if (o1 == null) {
                        return o2 == null ? 0 : 1;
                    }
                    if (o2 == null) {
                        return -1;
                    }
                    return o1.getValue().compareTo(o2.getValue());
                }
            });

            AllowableValue[] engines = engineList.toArray(new AllowableValue[engineList.size()]);

            SCRIPT_ENGINE = new PropertyDescriptor.Builder()
                    .name("Script Engine")
                    .required(true)
                    .description("The engine to execute scripts")
                    .allowableValues(engines)
                    .defaultValue(engines[0].getValue())
                    .required(true)
                    .addValidator(Validator.VALID)
                    .expressionLanguageSupported(false)
                    .build();
            descriptors.add(SCRIPT_ENGINE);
        }

        descriptors.add(SCRIPT_FILE);
        descriptors.add(MODULES);

        isInitialized.set(true);
    }

    /**
     * Performs common setup operations when the processor is scheduled to run. This method assumes the member
     * variables associated with properties have been filled.
     */
    void setup(int numberOfScriptEngines) {

        if (scriptEngineConfiguratorMap.isEmpty()) {
            ServiceLoader<ScriptEngineConfigurator> configuratorServiceLoader =
                    ServiceLoader.load(ScriptEngineConfigurator.class);
            for (ScriptEngineConfigurator configurator : configuratorServiceLoader) {
                scriptEngineConfiguratorMap.put(configurator.getScriptEngineName().toLowerCase(), configurator);
            }
        }
        setupEngines(numberOfScriptEngines);
    }

    /**
     * Configures the specified nifi.script engine. First, the engine is loaded and instantiated using the JSR-223
     * javax.nifi.script APIs. Then, if any nifi.script configurators have been defined for this engine, their init() method is
     * called, and the configurator is saved for future calls.
     *
     * @see nifi.script.ScriptEngineConfigurator
     */
    private void setupEngines(int numberOfScriptEngines) {
        engineQ = new LinkedBlockingQueue<>(numberOfScriptEngines);
        ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            ProcessorLog log = getLogger();

            ScriptEngineConfigurator configurator = scriptEngineConfiguratorMap.get(scriptEngineName.toLowerCase());

            // Get a list of URLs from the configurator (if present), or just convert modules from Strings to URLs
            URL[] additionalClasspathURLs = null;
            if (configurator != null) {
                additionalClasspathURLs = configurator.getModuleURLsForClasspath(modules, log);
            } else {
                if (modules != null) {
                    List<URL> urls = new LinkedList<>();
                    for (String modulePathString : modules) {
                        try {
                            urls.add(new File(modulePathString).toURI().toURL());
                        } catch (MalformedURLException mue) {
                            log.error("{} is not a valid file, ignoring", new Object[]{modulePathString}, mue);
                        }
                    }
                    additionalClasspathURLs = urls.toArray(new URL[urls.size()]);
                }
            }

            // Need the right classloader when the engine is created. This ensures the NAR's execution class loader
            // (plus the module path) becomes the parent for the nifi.script engine
            ClassLoader scriptEngineModuleClassLoader = createScriptEngineModuleClassLoader(additionalClasspathURLs);
            if (scriptEngineModuleClassLoader != null) {
                Thread.currentThread().setContextClassLoader(scriptEngineModuleClassLoader);
            }

            ScriptEngine scriptEngine = createScriptEngine();
            try {
                if (configurator != null) {
                    configurator.init(scriptEngine, modules);
                }
                engineQ.offer(scriptEngine);

            } catch (ScriptException se) {
                log.error("Error initializing nifi.script engine configurator {}", new Object[]{scriptEngineName});
                if (log.isDebugEnabled()) {
                    log.error("Error initializing nifi.script engine configurator", se);
                }
            }

        } finally {
            // Restore original context class loader
            Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        }
    }

    /**
     * Provides a ScriptEngine corresponding to the currently selected nifi.script engine name.
     * ScriptEngineManager.getEngineByName() doesn't use find ScriptEngineFactory.getName(), which
     * is what we used to populate the list. So just search the list of factories until a match is
     * found, then create and return a nifi.script engine.
     *
     * @return a Script Engine corresponding to the currently specified name, or null if none is found.
     */
    private ScriptEngine createScriptEngine() {
        //
        ScriptEngineFactory factory = scriptEngineFactoryMap.get(scriptEngineName);
        if (factory == null) {
            return null;
        }
        return factory.getScriptEngine();
    }

    /**
     * Creates a classloader to be used by the selected nifi.script engine and the provided nifi.script file. This
     * classloader has this class's classloader as a parent (versus the current thread's context
     * classloader) and also adds the specified module URLs to the classpath. This enables scripts
     * to use other scripts, modules, etc. without having to build them into the scripting NAR.
     * If the parameter is null or empty, this class's classloader is returned
     *
     * @param modules An array of URLs to add to the class loader
     */
    private ClassLoader createScriptEngineModuleClassLoader(URL[] modules) {
        ClassLoader thisClassLoader = this.getClass().getClassLoader();
        if (modules == null) {
            return thisClassLoader;
        }

        return new URLClassLoader(modules, thisClassLoader);
    }

    @OnStopped
    public void stop() {
        if (engineQ != null) {
            engineQ.clear();
        }
    }
}
