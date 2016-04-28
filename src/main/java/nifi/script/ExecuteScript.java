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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"nifi/script", "execute", "groovy", "python", "jython", "jruby", "ruby", "javascript", "js", "lua", "luaj"})
@CapabilityDescription("Experimental - Executes a nifi.script given the flow file and a process session.  The nifi.script is responsible for "
        + "handling the incoming flow file (transfer to SUCCESS or remove, e.g.) as well as any flow files created by "
        + "the nifi.script. If the handling is incomplete or incorrect, the session will be rolled back. Experimental: "
        + "Impact of sustained usage not yet verified.")
@DynamicProperty(
        name = "A nifi.script engine property to update",
        value = "The value to set it to",
        supportsExpressionLanguage = true,
        description = "Updates a nifi.script engine property specified by the Dynamic Property's key with the value "
                + "specified by the Dynamic Property's value")
public class ExecuteScript extends AbstractScriptProcessor {

    private String scriptToRun = null;

    /**
     * Returns the valid relationships for this processor.
     *
     * @return a Set of Relationships supported by this processor
     */
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return Collections.unmodifiableSet(relationships);
    }

    /**
     * Returns a list of property descriptors supported by this processor. The list always includes properties such as
     * nifi.script engine name, nifi.script file name, nifi.script body name, nifi.script arguments, and an external module path. If the
     * scripted processor also defines supported properties, those are added to the list as well.
     *
     * @return a List of PropertyDescriptor objects supported by this processor
     */
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        synchronized (isInitialized) {
            if (!isInitialized.get()) {
                createResources();
            }
        }

        return Collections.unmodifiableList(descriptors);
    }

    /**
     * Returns a PropertyDescriptor for the given name. This is for the user to be able to define their own properties
     * which will be available as variables in the nifi.script
     *
     * @param propertyDescriptorName used to lookup if any property descriptors exist for that name
     * @return a PropertyDescriptor object corresponding to the specified dynamic property name
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }


    /**
     * Performs setup operations when the processor is scheduled to run. This includes evaluating the processor's
     * properties, as well as reloading the nifi.script (from file or the "Script Body" property)
     *
     * @param context the context in which to perform the setup operations
     */
    @OnScheduled
    public void setup(final ProcessContext context) {
        scriptEngineName = context.getProperty(SCRIPT_ENGINE).getValue();
        scriptPath = context.getProperty(SCRIPT_FILE).evaluateAttributeExpressions().getValue();
        String modulePath = context.getProperty(MODULES).getValue();
        if (modulePath != null && !modulePath.isEmpty()) {
            modules = modulePath.split(",");
        } else {
            modules = new String[0];
        }
        // Create a nifi.script engine for each possible task
        int maxTasks = context.getMaxConcurrentTasks();
        super.setup(maxTasks);

        try {
            if (scriptToRun == null && scriptPath != null) {
                scriptToRun = readFile(scriptPath);

            }
        } catch (IOException ioe) {
            throw new ProcessException(ioe);
        }

    }

    /**
     * Evaluates the given nifi.script body (or file) using the current session, context, and flowfile. The nifi.script
     * evaluation expects a FlowFile to be returned, in which case it will route the FlowFile to success. If a nifi.script
     * error occurs, the original FlowFile will be routed to failure. If the nifi.script succeeds but does not return a
     * FlowFile, the original FlowFile will be routed to no-flowfile
     *
     * @param context        the current process context
     * @param sessionFactory provides access to a {@link ProcessSessionFactory}, which
     *                       can be used for accessing FlowFiles, etc.
     * @throws ProcessException if the scripted processor's onTrigger() method throws an exception
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        synchronized (isInitialized) {
            if (!isInitialized.get()) {
                createResources();
            }
        }
        ScriptEngine scriptEngine = engineQ.poll();
        if (scriptEngine == null) {
            // No engine available so nothing more to do here
            return;
        }
        ProcessorLog log = getLogger();
        ProcessSession session = sessionFactory.createSession();
        try {

            try {
                Bindings bindings = scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE);
                if (bindings == null) {
                    bindings = new SimpleBindings();
                }
                bindings.put("session", session);
                bindings.put("context", context);
                bindings.put("log", log);
                bindings.put("REL_SUCCESS", REL_SUCCESS);
                bindings.put("REL_FAILURE", REL_FAILURE);

                // Find the user-added properties and set them on the nifi.script
                for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
                    if (property.getKey().isDynamic()) {
                        // Add the dynamic property bound to its full PropertyValue to the nifi.script engine
                        if (property.getValue() != null) {
                            bindings.put(property.getKey().getName(), context.getProperty(property.getKey()));
                        }
                    }
                }

                scriptEngine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);

                // Execute any engine-specific configuration before the nifi.script is evaluated
                ScriptEngineConfigurator configurator =
                        scriptEngineConfiguratorMap.get(scriptEngineName.toLowerCase());

                // Evaluate the nifi.script with the configurator (if it exists) or the engine
                if (configurator != null) {
                    configurator.eval(scriptEngine, scriptToRun, modules);
                } else {
                    scriptEngine.eval(scriptToRun);
                }

                // Commit this session for the user. This plus the outermost catch statement mimics the behavior
                // of AbstractProcessor. This class doesn't extend AbstractProcessor in order to share a base
                // class with InvokeScriptedProcessor
                session.commit();
            } catch (ScriptException e) {
                throw new ProcessException(e);
            }
        } catch (final Throwable t) {
            // Mimic AbstractProcessor behavior here
            getLogger().error("{} failed to process due to {}; rolling back session", new Object[]{this, t});
            session.rollback(true);
            throw t;
        } finally {
            engineQ.offer(scriptEngine);
        }
    }

    private String readFile(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path)));
    }
}
