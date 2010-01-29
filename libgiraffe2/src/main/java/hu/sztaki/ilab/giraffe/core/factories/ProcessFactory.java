/*
   Copyright 2010 Computer and Automation Research Institute, Hungarian Academy of Sciences (SZTAKI)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hu.sztaki.ilab.giraffe.core.factories;

import hu.sztaki.ilab.giraffe.core.conversion.ConversionManager;
import hu.sztaki.ilab.giraffe.core.io.RecordExporter;
import hu.sztaki.ilab.giraffe.core.io.RecordImporter;
import hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessingElementBaseClasses;
import hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessMonitor;
import hu.sztaki.ilab.giraffe.core.util.FileUtils;
import org.apache.log4j.Logger;
import hu.sztaki.ilab.giraffe.schema.dataprocessing.RecordRoute;
import hu.sztaki.ilab.giraffe.core.xml.ConfigurationManager;
import hu.sztaki.ilab.giraffe.schema.dataformat.DataSink;
import hu.sztaki.ilab.giraffe.schema.dataformat.DataSource;
import hu.sztaki.ilab.giraffe.schema.dataprocessing.ProcessingNode;
import java.net.URLClassLoader;
import javax.xml.bind.JAXBElement;
import hu.sztaki.ilab.giraffe.schema.dataprocessing.ProcessingNetwork;
import hu.sztaki.ilab.giraffe.schema.process_definitions.ConversionHint;

/**
 * A ProcessFactory creates a Process object based on a process Definition.
 * There are 3 major types of tasks:
 * 1. Copy references.
 *  Certain values such as the output directory or JDBC connection identifiers, etc,
 *  are needed by the process. References to these values must be passed on to
 *  the Process object.
 * 2. Instantiation of new objects.
 *  Objects which are not generated at run-time must be instantiated. Examples
 *  of such objects include parsers/exports for stream inputs/outputs,
 *  readers/writers to CSV files/JDBC data sources,
 *  conversion objects,
 *  classes used within processing blocks.
 * THESE TYPES ARE ADDED TO THE PROCESS OBJECT BY THIS CLASS
 * 3. Generate code for dynamic classes during runtime.
 *  Dynamically generated code is created by the ProcessingNetworkGenerator
 *  class. It is compiled to java bytecode and instantiated. It is then assigned
 *  to member objects of the Process object by this class.
 *
 * @author neumark
 */
public class ProcessFactory {

    static Logger logger = Logger.getLogger(ProcessFactory.class);
    private final ConfigurationManager configurationManager;
    private final ConversionManager conversionManager;
    private String processName = null;
    private hu.sztaki.ilab.giraffe.core.processingnetwork.Process process = null;
    private hu.sztaki.ilab.giraffe.schema.process_definitions.Definitions definitions;
    private hu.sztaki.ilab.giraffe.schema.defaults.Defaults defaults;
    private ProcessingNetworkGenerator processingNetworkGenerator = null;
    private java.util.Map<String, hu.sztaki.ilab.giraffe.schema.process_definitions.Process.Terminals.Input> inputs = new java.util.HashMap<String, hu.sztaki.ilab.giraffe.schema.process_definitions.Process.Terminals.Input>();
    private java.util.Map<String, hu.sztaki.ilab.giraffe.schema.process_definitions.Process.Terminals.Output> outputs = new java.util.HashMap<String, hu.sztaki.ilab.giraffe.schema.process_definitions.Process.Terminals.Output>();
    private TerminalFactory terminalFactory;    

    public ProcessFactory(
            ConfigurationManager configurationManager,
            ConversionManager conversionManager) throws java.lang.InstantiationException {
        this.configurationManager = configurationManager;
        this.conversionManager = conversionManager;
        if (!init()) {
            throw new java.lang.InstantiationException("Could not initialize ProcessFactory!");
        }
    }

    private boolean init() {
        if (!loadProcessDescriptionFile(configurationManager.getProcessDefinitionFileLocation())) {
            logger.error("Could not load process description file " + configurationManager.getProcessDefinitionFileLocation().toString());
            return false;
        }
        if (!loadDefaultsFile(configurationManager.getDefaultsLocation())) {
            logger.error("Could not load defaults file " + configurationManager.getDefaultsLocation().toString());
            return false;
        }
        // create output dir
        try {
            if (!FileUtils.createParentDirs(new java.io.File(configurationManager.getOutputDir()))) {
                logger.error("Could not create output directory " + configurationManager.getOutputDir());
                return false;
            }
        } catch (java.io.IOException ex) {
            logger.error(ex);
            return false;
        }
        return true;
    }

    private ProcessingElementBaseClasses.ProcessingNetwork compileSource(java.io.File sourceFile) {
        java.io.StringWriter compilerOutput = new java.io.StringWriter();
        try {
            int errorCode = com.sun.tools.javac.Main.compile(new String[]{
                        "-classpath", System.getProperty("java.class.path"),
                        "-d", configurationManager.getOutputDir(), sourceFile.getAbsolutePath()}, new java.io.PrintWriter(compilerOutput));

            if (errorCode != 0) {
                logger.error("Error compiling loop: " + compilerOutput.toString());
                return null;
            } else {
                logger.info("Generated code compiled successfully.");
            }
            // Modify the classpath to load newly generated file.
            // Stolen from: http://www.javafaq.nu/java-example-code-895.html
            URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
            Class sysclass = URLClassLoader.class;
            java.lang.reflect.Method method = sysclass.getDeclaredMethod("addURL", new Class[]{java.net.URL.class});
            method.setAccessible(true);
            method.invoke(sysloader, new Object[]{new java.io.File(configurationManager.getOutputDir()).toURI().toURL()});
            Class loopClass = sysloader.loadClass(processingNetworkGenerator.networkName);
            return (ProcessingElementBaseClasses.ProcessingNetwork) loopClass.getConstructor(new Class[]{hu.sztaki.ilab.giraffe.core.processingnetwork.Process.class}).newInstance(process);
        } catch (java.lang.reflect.InvocationTargetException ex) {
            logger.error("Error loading generated class. ", ex);
        } catch (java.net.MalformedURLException ex) {
            logger.error("Error loading generated class. ", ex);
        } catch (java.lang.NoSuchMethodException ex) {
            logger.error("Error loading generated class. ", ex);
        } catch (java.lang.ClassNotFoundException ex) {
            logger.error("Failed to load class Loop: ", ex);
        } catch (java.lang.InstantiationException ex) {
            logger.error("Failed to instantiate loop class. ", ex);
        } catch (java.lang.IllegalAccessException ex) {
            logger.error("Security manager problem encountered loading loop class. ", ex);
        } catch (java.io.IOException ex) {
            logger.error("An error occured writing the source code of the processing loop.", ex);
        } catch (java.lang.NoClassDefFoundError ex) {
            logger.error("Error invoking the java compiler.", ex);
        }
        return null;
    }

    private boolean loadDefaultsFile(java.net.URL defaultsFile) {
        if (null == defaultsFile) {
            return false;
        }
        logger.info("Using defaults file " +defaultsFile.toString());
        try {
        defaults =
                hu.sztaki.ilab.giraffe.core.xml.XMLUtils.unmarshallXmlFile(
                hu.sztaki.ilab.giraffe.schema.defaults.Defaults.class,
                "hu.sztaki.ilab.giraffe.schema.defaults:" +
                "hu.sztaki.ilab.giraffe.schema.dataprocessing:" +
                "hu.sztaki.ilab.giraffe.schema.datatypes:",
                "http://info.ilab.sztaki.hu/giraffe/schema/defaults", defaultsFile);
        } catch (Throwable th) {
            logger.error("Error loading defaults file",th);
            return false;
        }
        if (null == defaults) {
            logger.error("Failed to load defaults");
            return false;
        }
        return true;
    }

    private boolean loadProcessDescriptionFile(java.net.URL defFile) {
        if (null == defFile) {
            return false;
        }
        try {
        definitions =
                hu.sztaki.ilab.giraffe.core.xml.XMLUtils.unmarshallXmlFile(
                hu.sztaki.ilab.giraffe.schema.process_definitions.Definitions.class,
                "hu.sztaki.ilab.giraffe.schema.process_definitions:" +
                "hu.sztaki.ilab.giraffe.schema.dataprocessing:" +
                "hu.sztaki.ilab.giraffe.schema.datatypes:",
                "http://info.ilab.sztaki.hu/giraffe/schema/process_definitions", defFile);
        } catch (Throwable th) {
            logger.error("Error loading process definitions file. ", th);
            return false;
        }
        if (null == definitions) {
            logger.error("Failed to load process definitions");
            return false;
        }
        return true;
    }

    private boolean initializeAllTerminals(hu.sztaki.ilab.giraffe.schema.process_definitions.Process processDesc) {
        // Read all data sources and data sinks from process definitions XML file's terminals section
        // and place them in the data[Sources|Sinks] maps.        
        this.terminalFactory = new TerminalFactory(configurationManager, processDesc.getTerminals().getInput().size(), processDesc.getTerminals().getOutput().size(), process.monitor);
        process.recordImporterThreadsLatch = this.terminalFactory.inputLatch;
        process.recordExporterThreadsLatch = this.terminalFactory.outputLatch;
        for (hu.sztaki.ilab.giraffe.schema.process_definitions.Process.Terminals.Input input : processDesc.getTerminals().getInput()) {
            // open record reader.            
            RecordImporter imp = terminalFactory.getInputTerminal(input);
            if (imp == null) {
                logger.error("Error instantiating data source " + input.getName() + ".");
                return false;
            }
            process.inputs.put(input.getName(), imp);
            process.threads.add(new Thread(imp));
            // store all conversion hints.
            for (ConversionHint hint : input.getConversionHint()) {
                conversionManager.registerConversionHint("datasource", input.getName(), hint);
            }
            this.inputs.put(input.getName(), input);
        }
        for (hu.sztaki.ilab.giraffe.schema.process_definitions.Process.Terminals.Output output : processDesc.getTerminals().getOutput()) {
            // if the data sink is not defined, use the default format.            
            if (output.getDatasink() == null) {
                output.setDatasink(defaults.getDataFormats().getDefaultDataSink());
            }
            RecordExporter exp = terminalFactory.getOutputTerminal(output);
            if (exp == null) {
                logger.error("Error intantiating data sink " + output.getName());
                return false;
            }
            process.outputs.put(output.getName(), exp);
            process.threads.add(new Thread(exp));
            for (ConversionHint hint : output.getConversionHint()) {
                conversionManager.registerConversionHint("datasink", output.getName(), hint);
            }
            this.outputs.put(output.getName(), output);
        }
        return true;
    }

    private boolean instantiateObjects() {
        for (ObjectInstantiator inst : process.conversionInstantiators.values()) if (!inst.instantiate()) return false;
        for (ObjectInstantiator inst : process.taskInstantiators.values()) if (!inst.instantiate()) return false;
        for (ObjectInstantiator inst : process.eventInstantiators.values()) if (!inst.instantiate()) return false;
        return true;
    }

    public hu.sztaki.ilab.giraffe.core.processingnetwork.Process get(String processName, ProcessMonitor processMonitor) {
        this.processName = processName;
        process = new hu.sztaki.ilab.giraffe.core.processingnetwork.Process();
        process.monitor = processMonitor;
        process.configurationManager = configurationManager;
        process.conversionManager = conversionManager;
        processingNetworkGenerator = new ProcessingNetworkGenerator(processName, defaults, process);
        hu.sztaki.ilab.giraffe.schema.process_definitions.Process processDescription = null;
        for (hu.sztaki.ilab.giraffe.schema.process_definitions.Process p : definitions.getProcess()) {
            if (p.getName() != null && p.getName().equals(processName)) {
                processDescription = p;
                break;
            }
        }
        if (null == processDescription) {
            logger.error("No process definition named '" + processName + "' found");
            return null;
        }
        if (!initializeAllTerminals(processDescription)) {
            logger.error("Error initializing terminals!");
            return null;
        }
        /* ETL process creation checklist:
         * - create data sources
         * - create data sinks
         * - instantiate stream parsers
         * - instantiate stream exporters
         * - instantiate conversion objects
         * - instantiate and initialize error handling objects, datasinks
         * - create the record object which serves as input for each processing block
         * - instantiate all objects used by a processing block
         * - create any bdb or memory-based hash maps used within processing blocks
         * - create any bdb or memory-based caches used by processing blocks
         * - instantiate and initialize event predicate objects
         *
         */
        // These functions add the data source/data sink definitions to
        // the processingNetworkGenerator object, which will add the appropriate
        // code to the generated class.
        java.io.File generatedClassSourceFile = null;
        if (addIO(processDescription) &&
                addProcessingNodes(processDescription) &&
                addRoutes(processDescription) &&
                processingNetworkGenerator.initializeNetwork()) {
            try {
                generatedClassSourceFile = new java.io.File(configurationManager.getOutputDir(), this.processingNetworkGenerator.networkName + ".java");
                java.io.BufferedWriter w = new java.io.BufferedWriter(new java.io.FileWriter(generatedClassSourceFile));
                w.write(this.processingNetworkGenerator.generateSourceCode());
                w.close();
                logger.info("Wrote generated code to file " + generatedClassSourceFile.getAbsolutePath() + ".");
            } catch (Exception ex) {
                logger.error("Error writing generated code to file. ", ex);
                return null;
            }
        } else {
            return null;
        }
        // instantiate objects stored within ObjectInstantiators.
        if (!instantiateObjects()) {
            logger.error("Error instantiating objects!");
            return null;
        }
        process.network = compileSource(generatedClassSourceFile);
        if (process.network == null) {
            logger.error("Processing network could not be compiled, exiting!");
            return null;
        }
        return process;
    }

    private boolean addIO(hu.sztaki.ilab.giraffe.schema.process_definitions.Process processDesc) {
        for (JAXBElement<?> ioElement : processDesc.getNetwork().getIo().getInputRefOrOutputRefOrAsyncPipe()) {
            // ioObject can either be a String or an asyncPipe object.
            if ("inputRef".equals(ioElement.getName().getLocalPart())) {
                String refersTo = (String) ioElement.getValue();
                hu.sztaki.ilab.giraffe.schema.process_definitions.Process.Terminals.Input inputDesc = this.inputs.get(refersTo);
                if (inputDesc == null) {
                    logger.error("Referenced dataSource '" + refersTo + "' not defined in <terminals> section!");
                    return false;
                }
                if (!this.processingNetworkGenerator.addDataSource(refersTo, inputDesc, process.inputs.get(refersTo).getRecordFormat())) {
                    logger.error("Failed to add dataSource " + refersTo + " to processing network.");
                    return false;
                }
            }
            if ("outputRef".equals(ioElement.getName().getLocalPart())) {
                String refersTo = (String) ioElement.getValue();
                hu.sztaki.ilab.giraffe.schema.process_definitions.Process.Terminals.Output outputDesc = this.outputs.get(refersTo);
                if (outputDesc == null) {
                    logger.error("Referenced dataSink '" + refersTo + "' not defined in <terminals> section!");
                    return false;
                }
                if (!this.processingNetworkGenerator.addDataSink(refersTo, outputDesc,process.outputs.get(refersTo).getRecordFormat())) {
                    logger.error("Failed to add dataSink " + refersTo + " to processing network.");
                    return false;
                }
            }
            if ("asyncPipe".equals(ioElement.getName().getLocalPart())) {
                ProcessingNetwork.Io.AsyncPipe pipe = (ProcessingNetwork.Io.AsyncPipe) ioElement.getValue();
                if (!this.processingNetworkGenerator.addAsyncPipe(pipe)) {
                    logger.error("Failed to add async pipe " + pipe.getName());
                    return false;
                }
            }
        }
        return true;
    }

    private boolean addProcessingNodes(hu.sztaki.ilab.giraffe.schema.process_definitions.Process processDesc) {
        for (ProcessingNode node : processDesc.getNetwork().getNode()) {
            if (!this.processingNetworkGenerator.addNode(node.getName(), node)) {
                logger.error("Failed to add processing node " + node.getName());
                return false;
            }
        }
        return true;
    }

    private boolean addRoutes(hu.sztaki.ilab.giraffe.schema.process_definitions.Process processDesc) {
        for (RecordRoute r : processDesc.getNetwork().getRoute()) {
            if (!this.processingNetworkGenerator.addRoute(r)) {
                logger.error("Failed to add route '" + r.getSource() + "' -> '" + r.getDestination() + "'.");
                return false;
            }
        }
        return true;
    }
}
