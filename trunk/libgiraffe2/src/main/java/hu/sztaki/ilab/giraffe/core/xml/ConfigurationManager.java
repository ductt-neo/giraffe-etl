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
package hu.sztaki.ilab.giraffe.core.xml;

import hu.sztaki.ilab.giraffe.schema.process_definitions.Definitions;

/**
 *
 * @author neumark
 */

public class ConfigurationManager  {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ConfigurationManager.class);
    private java.util.Map<String, String> argumentMap = new java.util.HashMap<String, String>();
    private java.util.List<String> inputFileList = new java.util.LinkedList<String>();
    private static java.util.Set<String> validArgumentNames = new java.util.HashSet<String>();
    private String outputDir = "processed_log_files/";
    private String keysetDir = outputDir + "keys/";
    private String resourceDir = "resources/";
    java.net.URL processDefinitionLocation = ConfigurationManager.class.getClassLoader().getResource("xml/process_definitions.xml");
    java.net.URL defaultsLocation = ConfigurationManager.class.getClassLoader().getResource("xml/defaults.xml");
    private String requestedProcessName;
    private boolean usecache = true;
    private boolean keepkeyset = false;
    private String nameserver;
    private ExportReader exportReader = new ExportReader();

    static {
        validArgumentNames.addAll(java.util.Arrays.asList(new String[]{
                    "process_definitions",
                    "process",
                    "usecache",
                    "keepkeyset",
                    "output",
                    "data",
                    "usekeyset",
                    "nameserver"
                }));
    }

    public java.util.Collection<String> getInputFiles() {return this.inputFileList;}

    public ConfigurationManager() {
    }

    public String getRequestedProcessName() {
        return requestedProcessName;
    }

    public ExportReader getExportReader() {return exportReader;}

    public java.net.URL getProcessDefinitionFileLocation() {
        return processDefinitionLocation;
    }

    public java.net.URL getDefaultsLocation() {
        return defaultsLocation;
    }

    public String getOutputDir() {return this.outputDir;}

    private boolean parseArgs(String[] args) {
        if (args.length == 0) {
            logger.error("Recieved no command line arguments!");
            return false;
        }
        int i = 0;
        for (; i < args.length; i++) {
            String currArg = args[i];
            if (currArg.contains("=")) {
                String[] parts = currArg.split("=", 2);
                if (!parts[0].startsWith("--")) {
                    logger.error("Command line arguments should start with '--'.");
                    return false;
                }
                parts[0] = parts[0].substring(2); // remove '--' prefix.
                if (!validArgumentNames.contains(parts[0])) {
                    logger.error("Invalid command line argument: '" + parts[0] + "'");
                    return false;
                }
                if (this.argumentMap.containsKey(parts[0])) {
                    logger.error("Command line argument: '" + parts[0] + "' specified multiple times!");
                    return false;
                }
                this.argumentMap.put(parts[0], parts[1]);
            } else {
                break;
            }
        }
        for (; i < args.length; i++) {
            if (args[i].contains("=")) {
                System.out.println("All arguments should precede input file names!");
                return false;
            }
            this.inputFileList.add(args[i]);
        }
        return true;
    }

    public boolean processCommandLineArgs(String[] args) {
        parseArgs(args);
        try {
            // --- process definition file location ---
            // Try the process_definitions command line argument first
            if (argumentMap.containsKey("process_definitions")) {
                processDefinitionLocation = new java.io.File(argumentMap.get("process_definitions")).toURI().toURL();
            }
            // If the processDefinitions system property is set, use that.
            if (null == processDefinitionLocation) {
                processDefinitionLocation = new java.io.File(System.getProperty("processDefinitions")).toURI().toURL();
            }
            // If a resource called "xml/process_definitions.xml" is available in the classpath, use that.
            if (null == processDefinitionLocation) {
                processDefinitionLocation = this.getClass().getClassLoader().getResource("xml/process_definitions.xml");
            }
            if (null == processDefinitionLocation) {
                logger.error("No process definitions file specified.");
                return false;
            }
            // --- requested process name ---
            if (argumentMap.get("process") == null) {
                logger.error("No process specified.");
                return false;
            } else {
                this.requestedProcessName = argumentMap.get("process");
            }
            // -------------- OPTIONAL PARAMETERS ---------------------
            // --- override default dirs --
            if (argumentMap.get("output") != null) {
                this.outputDir = argumentMap.get("output");
            }
            if (argumentMap.get("data") != null) {
                this.resourceDir = argumentMap.get("data");
            }
            if (argumentMap.get("usecache") != null) {
                Boolean val = parseBoolean(argumentMap.get("usecache"));
                if (val != null) {
                    this.usecache = val.booleanValue();
                }
            }
            if (argumentMap.get("keepkeyset") != null) {
                Boolean val = parseBoolean(argumentMap.get("keepkeyset"));
                if (val != null) {
                    this.keepkeyset = val.booleanValue();
                }
            }
            if (argumentMap.get("nameserver") != null) {
                this.nameserver = argumentMap.get("nameserver");
            }
        } catch (Exception ex) {
            logger.error(ex);
            return false;
        }
        logger.info("Using process definitions file at " + processDefinitionLocation);
        return true;
    }

    private Boolean parseBoolean(String value) {
        if (value.equals("t") || value.equals("true") || value.equals("True") || value.equals("TRUE")) {
            return new Boolean(true);
        }
        if (value.equals("f") || value.equals("false") || value.equals("False") || value.equals("FALSE")) {
            return new Boolean(false);
        }
        logger.warn("Cannot parse value '" + value + "' as boolean; should be [true|false].");
        return null;
    }
}
