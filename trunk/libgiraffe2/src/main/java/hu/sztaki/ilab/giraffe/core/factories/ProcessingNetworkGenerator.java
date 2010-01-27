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

import hu.sztaki.ilab.giraffe.core.io.RecordExporter;
import hu.sztaki.ilab.giraffe.schema.datatypes.VarToFieldMapping;
import hu.sztaki.ilab.giraffe.core.util.Pair;
import hu.sztaki.ilab.giraffe.schema.dataformat.NativeColumn;
import hu.sztaki.ilab.giraffe.schema.datatypes.EventCondition;
import hu.sztaki.ilab.giraffe.schema.datatypes.ObjectInstance;
import java.io.StringWriter;

/**
 * ProcessingNetworkGenerator
 * //////////////////
 * The ProcessingNetworkGenerator class is responsible for creating all the
 * generated code which runs as part of the ETL process.
 * This is accomplished in the following steps:
 * 1. All processing network nodes are added to the ProcessingNetworkGenerator by
 *    the ProcessFactoryImpl class by calling addNode, addDataSink or addDataSource.
 * 2. All routes are add by calling addRoute.
 * 3. The recievedRecord types associated with datasource and datasinks are constructed (this information is only available once the
 *    neighbors of these nodes are known.)
 * 3. The network is verified:
 *    - All nodes except dataSinks should have input/output routes
 *    - If no output route has a matching condition, then a discard sink should
 *      be specified (set using setNoRouteDiscard())
 *    - All nodes should be reachable from at least 1 data sink.
 *    - At most one directed route may link two processing nodes (no double-links).
 *    - The in/out data types of task method calls must be the same as what we assumed.
 * 4. Generate the code
 *    - Generate classes for each network node
 *    - generated code which instantiates each node.
 *
 * @author neumark
 */
public class ProcessingNetworkGenerator {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ProcessingNetworkGenerator.class);
    // Contains import instructions.
    final static String nodeClassPrefix = "ProcessingNetworkNode";
    final static String runTaskFunction = "runTask";
    final static int queueSize = 5000;
    final static java.util.Map<String, RecordDefinition> sharedRecordDefinitions = new java.util.HashMap<String, RecordDefinition>();
    // recordTypeNames associates record types with their java names, eg: received with receivedFields.
    static java.util.Map<String, String> recordTypeNames = new java.util.HashMap<String, String>();
    // standardIntanceNames assign an instance name to each type of record.
    static java.util.Map<String, String> standardInstanceNames = new java.util.HashMap<String, String>();
    // recordOrder specifies the order in which records are searched for fields when the mappings are established.
    final static String[] recordOrder = new String[]{"created", "received"};
    static EventCondition defaultEventCondition = new EventCondition();
    hu.sztaki.ilab.giraffe.core.processingnetwork.Process process;

    static {
        // errorRecord is the record definition for the uniform default error record
        // which is available from any inner node in the processing network if
        // and error occurs. Since we do not define what fields it contains, its code is not generated.
        sharedRecordDefinitions.put("error", new RecordDefinition("ErrorRecord"));
        sharedRecordDefinitions.put("nodedata", new RecordDefinition("NodedataRecord"));
        // Define the fields of the error record
        sharedRecordDefinitions.get("error").owner = RecordDefinition.RecordDefinitionClassOwner.NETWORK;
        sharedRecordDefinitions.get("error").record.add(new Pair<String, String>("java.lang.Throwable", "ex"));
        sharedRecordDefinitions.get("error").record.add(new Pair<String, String>("ProcessingElementBaseClasses.Record", "received"));
        sharedRecordDefinitions.get("error").record.add(new Pair<String, String>("ProcessingElementBaseClasses.Record", "created"));
        sharedRecordDefinitions.get("error").record.add(new Pair<String, String>("ProcessingElementBaseClasses.Record", "nodedata"));
        sharedRecordDefinitions.get("error").record.add(new Pair<String, String>("java.util.Set<hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType>", "events"));
        // define nodedataRecord
        sharedRecordDefinitions.get("nodedata").owner = RecordDefinition.RecordDefinitionClassOwner.NETWORK;
        sharedRecordDefinitions.get("nodedata").record.add(new Pair<String, String>("java.lang.String", "name"));
        sharedRecordDefinitions.get("nodedata").record.add(new Pair<String, String>("java.lang.String", "nodeClass"));
        // define objectrecord
        // Define the mappings between internal record names and their corresponding java object names
        recordTypeNames.put("received", "ReceivedRecord");
        recordTypeNames.put("created", "CreatedRecord");
        standardInstanceNames.put("received", "receivedRecord");
        standardInstanceNames.put("created", "createdRecord");
        standardInstanceNames.put("error", "errorRecord");
        standardInstanceNames.put("nodedata", "nodedata");
    }
    String networkName;
    private java.util.Set<String> imports = new java.util.HashSet<String>();
    // Stores instance fields associated with the generated object.
    java.util.Map<String, ProcessingNetworkNode> nodes = new java.util.HashMap<String, ProcessingNetworkNode>();
    java.util.List<RecordRoute> routes = new java.util.LinkedList<RecordRoute>();
    hu.sztaki.ilab.giraffe.schema.defaults.Defaults defaults;

    static class Field {

        public Field(String record, String type, String field) {
            this.record = record;
            this.type = type;
            this.field = field;
        }
        String record;
        String type;
        String field;

        public String toString() {
            return record + ":" + field + " (type: " + type + ")";
        }
    }

    static class MappingExpressionSource {

        boolean isJavaExpression = false;
        String javaExpression;
        Field sourceField;

        public MappingExpressionSource(String javaExpression, String type) {
            isJavaExpression = true;
            this.javaExpression = javaExpression;
            this.sourceField = new Field(null, type, null);
        }

        public MappingExpressionSource(Field sourceField) {
            this.sourceField = sourceField;
        }

        public String toString() {
            if (isJavaExpression) {
                return ((javaExpression == null) ? "null" : javaExpression);
            }
            return sourceField.toString();
        }
    }

    static class RecordMapping {

        // maps DESTINATION field names to ( (source record, source field), String javaexpression) pairs,
        // Both (record type, source field) and javaexpression should not be set!
        // where Record type refers to either createdFields or receivedFields.
        java.util.Map<String, MappingExpressionSource> mapping =
                new java.util.HashMap<String, MappingExpressionSource>();

        public String toString() {
            String ret = "";
            for (java.util.Map.Entry<String, MappingExpressionSource> me : mapping.entrySet()) {
                ret += "\n";
                ret += me.getValue().toString() + " -> " + me.getKey() + "\n";
            }
            return ret;
        }
    }

    /* A RecordDefinition is a list of (field type, field name) pairs, where
     * type is the string representation of a java type.
     */
    public static class RecordDefinition {

        // RecordDefinitionClassOwner determines where the class source is generated.
        static enum RecordDefinitionClassOwner {

            NODE, // A class is generated based on this record for each node which uses it.
            NETWORK, // The processingnetwork has a single class shared by all nodes which use this record definition.
            EXTERNAL // The class belonging to the record definition lives in another file. Don't generate any code for this class.
        };
        java.util.List<Pair<String, String>> record = new java.util.LinkedList<Pair<String, String>>();
        String className;
        RecordDefinitionClassOwner owner = RecordDefinitionClassOwner.NODE;

        public RecordDefinition() {
        }

        public RecordDefinition(String className) {
            this.className = className;
        }

        public RecordDefinition(String columnType, java.util.List<String> columnNames) {
            for (String c : columnNames) {
                record.add(new Pair<String, String>(columnType, c));
            }
        }

        public RecordDefinition(java.util.List<Pair<String, String>> rec) {
            this.record = rec;
        }

        String generateRecordClass() {
            StringWriter codeWriter = new StringWriter();
            codeWriter.write("  public static class " + this.className + " implements hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessingElementBaseClasses.Record {\n");
            for (Pair<String, String> currentField : this.record) {
                codeWriter.write("  " + currentField.first + " " + currentField.second + ";\n");
            }
            codeWriter.write("  public " + this.className + " () {}\n");
            codeWriter.write("  public " + this.className + " (");
            boolean first = true;
            for (Pair<String, String> currentField : this.record) {
                if (!first) {
                    codeWriter.write(",");
                }
                first = false;
                codeWriter.write(currentField.first + " " + currentField.second);
            }
            codeWriter.write("){\n");
            for (Pair<String, String> currentField : this.record) {
                codeWriter.write("    this." + currentField.second + " = " + currentField.second + ";\n");
            }
            codeWriter.write("  }\n");
            codeWriter.write("  public String[] getFieldNames() { return new String[]{");
            first = true;
            for (Pair<String, String> currentField : this.record) {
                if (!first) {
                    codeWriter.write(",");
                }
                first = false;
                codeWriter.write("\"" + currentField.second + "\"");
            }
            codeWriter.write("};}\n");
            codeWriter.write("  public Object[] getFields() { return new Object[]{");
            first = true;
            for (Pair<String, String> currentField : this.record) {
                if (!first) {
                    codeWriter.write(",");
                }
                first = false;
                codeWriter.write(currentField.second);
            }
            codeWriter.write("};}\n");
            codeWriter.write("  public String[] getFieldTypes() { return new String[] {");
            first = true;
            for (Pair<String, String> currentField : this.record) {
                if (!first) {
                    codeWriter.write(",");
                }
                first = false;
                codeWriter.write("\"" + currentField.first + "\"");
            }
            codeWriter.write("};}\n");
            codeWriter.write("  public Object getField(int columnNumber) {\n");
            codeWriter.write("    switch (columnNumber) {\n");
            int index = 0;
            for (Pair<String, String> currentField : this.record) {
                codeWriter.write("    case " + index + ": return " + currentField.second + ";\n");
                index++;
            }
            codeWriter.write("    default: throw new java.lang.IndexOutOfBoundsException();");
            codeWriter.write("  }}\n");
            codeWriter.write("  public void setFields(Object[] o) {\n");
            index = 0;
            for (Pair<String, String> currentField : this.record) {
                codeWriter.write("    " + currentField.second + " = (" + currentField.first + ")o[" + index + "];\n");
                index++;
            }
            codeWriter.write("  }\n");
            // close class.
            codeWriter.write("  }\n");
            return codeWriter.toString();

        }

        public String toString() {
            String ret = "";
            for (Pair<String, String> p : record) {
                ret += "\n";
                ret += p.first + " " + p.second;
            }
            return ret;
        }
    }

    /* The ProcessingBlockSource object describes the source code to a certain
     * type of processing block. It is always compiled as a static inner class
     * of the class holding the generated code.
     * Besides the class's name and base class, the input/output parameters
     * used by the block must also be specified.
     *
     */
    static class RouteCondition {

        private RouteCondition() {
        }
        String conditionEvaluationCode = "";

        static RouteCondition getRouteCondition(hu.sztaki.ilab.giraffe.schema.datatypes.EventCondition evt, EventConditionGenerator evtGen) {
            RouteCondition cond = new RouteCondition();
            if (!evtGen.build(evt)) {
                logger.error("Failed to parse event condition!");
                return null;
            }
            cond.conditionEvaluationCode = evtGen.getExpression();
            return cond;
        }

        public String getIfStatement() {
            return conditionEvaluationCode;
        }
        // currenty empty: STUB
    }

    /* A RecordRoute connects two ProcessingNetworkNodes.
     * source and destination refer to keys in the $nodes map.
     * onEvent will probably change type later.
     *
     */
    static class RecordRoute {

        ProcessingNetworkNode source;
        ProcessingNetworkNode destination;
        // Since the routes of the processing network are registered before
        // all record definitions are deduced, the routeDefinition must initially be
        // available with the record route. Once the receivedFieldsMapping and onEvent fields are
        // computed, the routeDefinition field is no longer necessary, and can be set to null.
        hu.sztaki.ilab.giraffe.schema.dataprocessing.RecordRoute routeDefinition;
        RecordMapping receivedFieldsMapping;
        RouteCondition onEvent = new RouteCondition();

        public RecordRoute(ProcessingNetworkNode source, ProcessingNetworkNode destination, hu.sztaki.ilab.giraffe.schema.dataprocessing.RecordRoute routeDefinition) {
            this.source = source;
            this.destination = destination;
            this.routeDefinition = routeDefinition;
        }
    }

    /* ProcessingNetworkNode describes an instance of a processing block.
     * Since several instances of a processing block may be present in a single
     * network, there is a many-to-1 relationship between ProcessingNetworkNode
     * and ProcessingBlockSource objects.
     * The input of a ProcessingNetworkNode may include fields not used by the
     * code within the processing block, but which needs to be forwarded for
     * use later down the line. As a result, the record expected by the
     * ProcessingNetworkNode (which is described by ProcessingNetworkNode.recievedFields)
     * needs to be mapped to the record expected by the ProcessingBlockSource object.
     * Since the latter is a subset of the former, a list of indexes does the job.
     * Once the processing block's code is finished, an output record is created which
     * has the format described in ProcessingBlockSource.outputFields.
     * Another mapping must map columns from received Fields and columns from
     * createdFields to the outgoing column format, which is incidentally the
     * same format as the recievedFields format of the destination ProcessingNetworkNode.
     *
     * Example:
     * Incoming record fields (record0) : |a |b |c |d |e |f
     * BTW: this is described by the nodes receivedFields object.
     *  --- we need to map fields record0:1 -> record1:1 ; record0:3 -> record1:2 : [1,3]
     * Record used by the processing block's code (record1) : |a |c
     * Record created by the processing block's code: |g |h |i
     *  --- we need to map fields record0:2 -> record2:1 ; ... ; record1:1 -> record2:5; ...
     * Outgoing record fields (record 2): |b |d |e |f |g |h |i
     *
     */
    interface ProcessingNodeSource {

        public String getSource();

        public EventConditionGenerator getEventConditionGenerator();
        // intput and output are lists of (variable name, (field type, field name ))

        public boolean addTask(java.util.List<Pair<String, Pair<String, String>>> input, java.util.List<Pair<String, Pair<String, String>>> output, ObjectInstance call);

        public String getClassName();

        public String instantiate();

        public ObjectInstantiator getTaskInstantiator();

        public ObjectInstantiator getEventInstantiator();
    }

    // The ProcessingNetworkNode class describes any processing network node.
    class ProcessingNetworkNode {
        // The most important properties of a processing network node are:
        // The name of the node.

        protected String nodeName;
        // The source code of the classes which contain data and perform functions for the node.
        protected ProcessingNodeSource src;
        // The definitions of the records used by this node. Most important are
        // the created record and the received record.
        protected java.util.Map<String, ProcessingNetworkGenerator.RecordDefinition> records = new java.util.HashMap<String, ProcessingNetworkGenerator.RecordDefinition>();
        // Incoming routes from other nodes.
        protected java.util.List<ProcessingNetworkGenerator.RecordRoute> incomingRoutes = new java.util.LinkedList<ProcessingNetworkGenerator.RecordRoute>();
        // Outgoing routes to other nodes.
        protected java.util.List<ProcessingNetworkGenerator.RecordRoute> outgoingRoutes = new java.util.LinkedList<ProcessingNetworkGenerator.RecordRoute>();

        public ProcessingNetworkNode(String nodeName) {
            this.nodeName = nodeName;
        }

        public void init() {
            initStandardRecords();
            initIORecords();
            initSource();
        }

        public boolean isDataSource() {
            return false;
        }

        public boolean isDataSink() {
            return false;
        }

        public boolean isVirtual() {return false;}

        protected void initStandardRecords() {
            this.records.put("error", ProcessingNetworkGenerator.sharedRecordDefinitions.get("error"));
            this.records.put("nodedata", ProcessingNetworkGenerator.sharedRecordDefinitions.get("nodedata"));
        }

        protected void initIORecords() {
            this.records.put("received", new RecordDefinition());
            this.records.put("created", new RecordDefinition());
        }

        protected void initSource() {
            this.src = new InnerNodeSource(this, ProcessingNetworkGenerator.this.networkName);
        }

        public String getName() {
            return nodeName;
        }

        public ProcessingNetworkGenerator.ProcessingNodeSource getSource() {
            return src;
        }

        public java.util.Map<String, ProcessingNetworkGenerator.RecordDefinition> getRecords() {
            return records;
        }

        public java.util.List<ProcessingNetworkGenerator.RecordRoute> getIncomingRoutes() {
            return incomingRoutes;
        }

        public java.util.List<ProcessingNetworkGenerator.RecordRoute> getOutgoingRoutes() {
            return outgoingRoutes;
        }
    }

    // a virtual io node can be a virtual data sink or a virtual data source.
    class VirtualIONode extends ProcessingNetworkNode {

        boolean isDataSource;
        // STUB

        public VirtualIONode(String nodeName, boolean isDataSource) {
            super(nodeName);
            this.isDataSource = isDataSource;
        }

        public boolean isDataSource() {
            return this.isDataSource;
        }

        public boolean isDataSink() {
            return !this.isDataSource;
        }

        protected void initIORecords() {
            RecordDefinition singleRecordType = new RecordDefinition(); // both created and received have this same format.
            this.records.put("received", singleRecordType);
            this.records.put("created", singleRecordType);
        }
    }

    // a DataSourceNode represents a "read data source" (threaded data source).
    class DataSourceNode extends ProcessingNetworkNode {
        // Received fields is of type ObjectRecord, but the
        // elements of the Object[] array have types (and names)
        // which are stored in receivedFieldDefinition.

        RecordDefinition receivedFieldDefinition;

        class SourceCode extends InnerNodeSource {

            ObjectInstantiator conversionObjectInstantiator = new ObjectInstantiator("conv");

            public SourceCode() {
                super(DataSourceNode.this, ProcessingNetworkGenerator.this.networkName);
            }

            @Override
            public void writeClassBody(StringWriter codeWriter) {
                // import conversion.
                writeImportConversion(codeWriter);
                // constructor
                codeWriter.write("  public " + this.getClassName() + "(java.util.concurrent.CountDownLatch latch, hu.sztaki.ilab.giraffe.core.factories.ObjectInstantiator convObjects) {\n");
                codeWriter.write("    super(latch);\n");
                codeWriter.write("    " + ProcessingNetworkGenerator.this.networkName + ".this.process.inputs.get(\"" + DataSourceNode.this.getName() + "\").setQueueWriter(this);\n");
                codeWriter.write(conversionObjectInstantiator.generateGetInstancesCode("convObjects"));
                codeWriter.write("  }\n");                
                // send() stub
                // write class conversion fields (if any).
                codeWriter.write(conversionObjectInstantiator.generateFieldDeclarations());
                codeWriter.write("  public void send() {/*STUB*/ ;}\n");
                codeWriter.write("  public boolean append(Object[] row) throws InterruptedException {\n" + "    " + this.node.records.get("received").className + " newRow = new " + this.node.records.get("received").className + "();\n" + "    newRow.setFields(row);\n" + "    queue.put(newRow);return true;\n" + "  }\n");
            }

            private void writeImportConversion(StringWriter codeWriter) {
                // importConversion stub               
                codeWriter.write("  public " + DataSourceNode.this.records.get("created").className + " importConversion(" + DataSourceNode.this.records.get("received").className + " record) { \n");
                codeWriter.write("  try { // place conversion within a try... block so potential exceptions are caught.\n");
                codeWriter.write("    return new "+DataSourceNode.this.records.get("created").className+"(");
                boolean first = true;
                for (Pair<String, String> col : DataSourceNode.this.records.get("created").record) {
                    if (!first) codeWriter.write(", ");
                    Pair<String, String> srcCol = ProcessingNetworkGenerator.getField(DataSourceNode.this.records.get("received"), col.second);
                    codeWriter.write(process.conversionManager.getConversionCode(
                            "datasource",
                            DataSourceNode.this.getName(),
                            col.second,
                            srcCol.first,
                            col.first,
                            "record." + col.second,
                            conversionObjectInstantiator));
                    first = false;
                }
                codeWriter.write(");\n");
                codeWriter.write("  } catch (Exception ex) {\n");
                codeWriter.write("    this.events.remove(hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType.META_OK); // Everything is not OK anymore\n");
                codeWriter.write("    this.events.add(hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType.ERROR_CONVERSION_FAILED); // Specifically, our task reportedly failed.\n");
                codeWriter.write("    updateErrorRecord(ex);\n");
                codeWriter.write("    return null;\n");
                codeWriter.write("  }\n");
                codeWriter.write("  }\n");
                if (!conversionObjectInstantiator.isEmpty()) {
                    ProcessingNetworkGenerator.this.process.conversionInstantiators.put(nodeName, conversionObjectInstantiator);
                }
            }

            public String instantiate() {
                return " new " + getClassName() + "(this.process.dataSourcesThreadsLatch,"+
                        "this.process.conversionInstantiators.get(\"" + this.node.nodeName + "\"))";
            }

            @Override
            public String getBaseClass() {
                String type = "<" + DataSourceNode.this.records.get("received").className + ", " + DataSourceNode.this.records.get("created").className + ">";
                return "ProcessingElementBaseClasses.ThreadedDataSource" + type;
            }
        }

        public DataSourceNode(String nodeName, RecordDefinition receivedFieldDefinition) {
            super(nodeName);
            this.receivedFieldDefinition = receivedFieldDefinition;
        }

        public boolean isDataSource() {
            return true;
        }

        protected void initSource() {
            this.src = new SourceCode();
        }

        protected void initStandardRecords() {
            super.initStandardRecords();
        }

        protected void initIORecords() {
            this.records.put("received", this.receivedFieldDefinition);
            this.records.put("created", new RecordDefinition());
        }
    }

    // a DataSinkNode represents a "real" (threaded) data sink.
    class DataSinkNode extends ProcessingNetworkNode {
        // Created fields are of type ObjectRecord, but the
        // elements of the Object[] array have types (and names)
        // which are stored in createdFieldDefinition.

        RecordDefinition createdFieldDefinition;

        class SourceCode extends InnerNodeSource {

            ObjectInstantiator conversionObjectInstantiator = new ObjectInstantiator("conv");

            public SourceCode() {
                super(DataSinkNode.this, ProcessingNetworkGenerator.this.networkName);
            }


            private void writeExportConversion(StringWriter codeWriter) {
                // importConversion stub
                codeWriter.write("  public " + DataSinkNode.this.records.get("created").className + " exportConversion(" + DataSinkNode.this.records.get("received").className + " record) throws Throwable { \n");
                codeWriter.write("    return new "+DataSinkNode.this.records.get("created").className+"(");
                boolean first = true;
                for (Pair<String, String> col : DataSinkNode.this.records.get("created").record) {
                    if (!first) codeWriter.write(", ");
                    Pair<String, String> srcCol = ProcessingNetworkGenerator.getField(DataSinkNode.this.records.get("received"), col.second);
                    codeWriter.write(process.conversionManager.getConversionCode(
                            "datasink",
                            DataSinkNode.this.getName(),
                            col.second,
                            srcCol.first,
                            col.first,
                            "record." + col.second,
                            conversionObjectInstantiator));
                    first = false;
                }
                codeWriter.write(");\n");
                codeWriter.write("  }\n");
                if (!conversionObjectInstantiator.isEmpty()) {
                    ProcessingNetworkGenerator.this.process.conversionInstantiators.put(nodeName, conversionObjectInstantiator);
                }
            }

              public void writeClassBody(StringWriter codeWriter) {
                // import conversion.
                writeExportConversion(codeWriter);
                // constructor
                codeWriter.write("  public " + this.getClassName() + "(hu.sztaki.ilab.giraffe.core.factories.ObjectInstantiator convObjects) {\n");
                codeWriter.write("    " + ProcessingNetworkGenerator.this.networkName + ".this.process.outputs.get(\"" + DataSinkNode.this.getName() + "\").setQueue(this.queue);\n");
                codeWriter.write(conversionObjectInstantiator.generateGetInstancesCode("convObjects"));
                codeWriter.write("  }\n");
                // send() stub
                // write class conversion fields (if any).
                codeWriter.write(conversionObjectInstantiator.generateFieldDeclarations());
            }

            public String instantiate() {
                return " new " + getClassName() + "("+
                        "this.process.conversionInstantiators.get(\"" + this.node.nodeName + "\"))";
            }


            @Override
            public String getBaseClass() {
                String type = "<" + DataSinkNode.this.records.get("received").className + ", " + DataSinkNode.this.records.get("created").className + ">";
                return "ProcessingElementBaseClasses.ThreadedDataSink" + type;
            }
        }

        public DataSinkNode(String nodeName, RecordDefinition createdFieldDefinition) {
            super(nodeName);
            this.createdFieldDefinition = createdFieldDefinition;
        }

        public boolean isDataSink() {
            return true;
        }

        protected void initSource() {
            this.src = new SourceCode();
        }

        protected void initIORecords() {
            this.records.put("received", new RecordDefinition());
            this.records.put("created", this.createdFieldDefinition);
        }
    }

    // an AsyncPipeNode represents both the sink & source belonging to an async pipe.
    class AsyncPipeNode extends ProcessingNetworkNode {

        class SourceCode extends InnerNodeSource {

            public SourceCode() {
                super(AsyncPipeNode.this, ProcessingNetworkGenerator.this.networkName);
            }

            @Override
            public void writeClassBody(StringWriter codeWriter) {
            }

            @Override
            public String getBaseClass() {
                String type = "<" + AsyncPipeNode.this.records.get("received").className + ">";
                return "ProcessingElementBaseClasses.AsyncPipe" + type;
            }
        }

        public AsyncPipeNode(String nodeName) {
            super(nodeName);
        }

        public boolean isDataSink() {
            return true;
        }

        public boolean isDataSource() {
            return true;
        }

        @Override
        protected void initSource() {
            this.src = new SourceCode();
        }

        protected void initIORecords() {
            RecordDefinition singleRecordType = new RecordDefinition(); // both created and received have this same format.
            this.records.put("received", singleRecordType);
            this.records.put("created", singleRecordType);
        }
    }

    public ProcessingNetworkGenerator(String networkName, hu.sztaki.ilab.giraffe.schema.defaults.Defaults defaults, hu.sztaki.ilab.giraffe.core.processingnetwork.Process process) {
        imports.add("hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessingElementBaseClasses");
        this.networkName = InnerNodeSource.toClassName(networkName);
        this.defaults = defaults;
        this.process = process;
    }

    public boolean addDataSource(String name, RecordDefinition ReceivedRecord) {
        ProcessingNetworkNode dsNode = new DataSourceNode(name, ReceivedRecord);
        dsNode.init();
        nodes.put(name, dsNode);
        return true;
    }

    public boolean addDataSink(String name, RecordDefinition dsDefinition) {
        // --- create associated processing node ---
        ProcessingNetworkNode dsNode = new DataSinkNode(name, dsDefinition);
        dsNode.init();
        nodes.put(name, dsNode);
        return true;
    }

    public boolean addAsyncPipe(hu.sztaki.ilab.giraffe.schema.dataprocessing.ProcessingNetwork.Io.AsyncPipe pipe) {
        ProcessingNetworkNode node = new AsyncPipeNode(pipe.getName());
        nodes.put(pipe.getName(), node);
        return true;
    }

    public boolean addNode(String nodeName, hu.sztaki.ilab.giraffe.schema.dataprocessing.ProcessingNode nodeDef) {
        ProcessingNetworkNode node = new ProcessingNetworkNode(nodeDef.getName());
        node.init();
        // Save receievedFields:
        for (NativeColumn col : nodeDef.getReceivedFields().getColumn()) {
            node.getRecords().get("received").record.add(new Pair<String, String>(col.getJavaType(), col.getName()));
        }
        // Save createdFields:
        for (NativeColumn col : nodeDef.getCreatedFields().getColumn()) {
            node.getRecords().get("created").record.add(new Pair<String, String>(col.getJavaType(), col.getName()));
        }
        java.util.Set<String> mappedOutputColumns = new java.util.HashSet<String>();
        for (Object t : nodeDef.getTasks().getNetworkOrCall()) {
            try {
                hu.sztaki.ilab.giraffe.schema.dataprocessing.ProcessingNode.Tasks.Network net = (hu.sztaki.ilab.giraffe.schema.dataprocessing.ProcessingNode.Tasks.Network) t;
                logger.error("Using a processing network as a ProcessingNode Task is not yet supported!");
                return false;
            } catch (ClassCastException ex) {
            }
            ObjectInstance call = (ObjectInstance) t;
            // taskInputs and taskOutputs are lists of (variable name, (field type, field name pairs))
            java.util.List<Pair<String, Pair<String, String>>> taskInputs = new java.util.LinkedList<Pair<String, Pair<String, String>>>();
            java.util.List<Pair<String, Pair<String, String>>> taskOutputs = new java.util.LinkedList<Pair<String, Pair<String, String>>>();
            // Map the <work><task> nodes' input fields to receivedFields.
            for (VarToFieldMapping m : call.getInput()) {
                Pair<String, Pair<String, String>> inputField = new Pair<String, Pair<String, String>>(m.getVariable(), null);
                for (Pair<String, String> p : node.getRecords().get("received").record) {
                    if (p.second.equals(m.getField())) {
                        inputField.second = p;
                        break;
                    }
                }
                if (inputField.second == null) {
                    logger.error("A task needs an input variable named '" + m.getVariable() + "' mapping to receivedFields." + m.getField() + " but no such field is present in receivedFields of node " + node.nodeName);
                    return false;
                }
                taskInputs.add(inputField);
            }
            // Map the <work><task> nodes' output fields to createdFields.
            for (VarToFieldMapping m : call.getOutput()) {
                if (mappedOutputColumns.contains(m.getField())) {
                    logger.error("A task defines output field '" + m.getField() + "' but it has already been defined by a previous task in node " + node.nodeName);
                    return false;
                }
                Pair<String, Pair<String, String>> outputField = new Pair<String, Pair<String, String>>(m.getVariable(), null);
                for (Pair<String, String> p : node.getRecords().get("created").record) {
                    if (p.second.equals(m.getField())) {
                        outputField.second = p;
                        break;
                    }
                }
                if (outputField.second == null) {
                    logger.error("Task produces variable " + m.getVariable() + " which is mapped to createFields." + m.getField() + " but createdFields of node " + node.nodeName + " does not contain such field.");
                    return false;
                }
                taskOutputs.add(outputField);
                mappedOutputColumns.add(m.getField());
            }
            // Now that we know the inputs and outputs of this task, add task to node's source.
            if (!node.src.addTask(taskInputs, taskOutputs, call)) {
                return false;
            }
            nodes.put(node.nodeName, node);
        }
        return true;
    }

    static boolean fieldsEquivalent(Pair<String, String> sourceField, Pair<String, String> destinationField) {
        return sourceField != null && destinationField != null && (sourceField.first == null || destinationField.first == null || sourceField.first.equals(destinationField.first)) && sourceField.second.equals(destinationField.second);
    }

    /* mapFields creates a RecordMapping between a source and destination RecordDefinition.
     * DataSource and DataSink nodes are special cases, because the type of fields for these records is null.
     * In these cases if the field name matches, then the null value is replaced with the source/desintation field's type.
     */
    // list is a list of (field type, field name) pairs.
    static Pair<String, String> getField(RecordDefinition record, String name) {
        if (name == null) {
            return null;
        }
        if (record == null || record.record == null || record.record.size() < 1) {
            return null;
        }
        for (Pair<String, String> sourceField : record.record) {
            if (sourceField.second != null && name.equals(sourceField.second)) {
                return sourceField;
            }
        }
        return null;
    }

    static String unqualifyFieldName(String fn) {
        if (fn.contains(":")) {
            String[] parts = fn.split(":", 2);
            return parts[1];
        }
        return fn;
    }

    /* finds a field with a given name in the specified record if potentiallyQualifiedFieldName is a qualified field name, or
     * in any of the records passed as input in the order defined by recordOrder
     *
     */
    static Field bindField(java.util.Map<String, RecordDefinition> records, String potentiallyQualifiedFieldName, String fieldType) {
        String fieldName = potentiallyQualifiedFieldName;
        String[] recordCandidates = ProcessingNetworkGenerator.recordOrder;
        if (fieldName.contains(":")) {
            String[] parts = fieldName.split(":", 2);
            fieldName = parts[1];
            recordCandidates = new String[]{parts[0]};
        }
        return bindField(recordCandidates, records, new Pair<String, String>(fieldType, fieldName));
    }

    /* bindField finds a field with the given name and type in a set of records, using the search order defined in candidates.
     * Returns null if no such field can be found.
     */
    static Field bindField(String[] candidates, java.util.Map<String, RecordDefinition> records, Pair<String, String> field) {
        for (String r : candidates) {
            RecordDefinition recDef = records.get(r);
            if (recDef == null) {
                logger.debug("Record definition '" + r + "' is null.");
                continue;
            }
            for (Pair<String, String> f : recDef.record) {
                if (fieldsEquivalent(f, field)) {
                    return new Field(r, f.first, f.second);
                }
            }
        }
        return null;
    }

    boolean interpretMapping(ProcessingNetworkNode source, ProcessingNetworkNode destination, hu.sztaki.ilab.giraffe.schema.dataprocessing.RecordRoute.MapField mf, RecordMapping currentMappings, boolean destinationReceivedRecordDefined) {
        // describes the field in the destination node's received record which is defined by this mapping.
        Pair<String, String> currentDestinationField = null;
        // describes the source field which is copied to the destination field.
        Field sourceField = null;
        if (destinationReceivedRecordDefined) { // The destination's receivedRecord is already defined.
            currentDestinationField = getField(destination.getRecords().get("received"), mf.getDestination());
            if (null == currentDestinationField) {
                logger.error("Mapping specifies destination field '" + mf.getDestination() + "', which cannot be found in the receivedFields record of " + destination.nodeName);
                return false;
            }
            sourceField = bindField(source.getRecords(), mf.getSource(), currentDestinationField.first);
            if (null == sourceField) {
                // Since we have search every source record without finding a candidate field, we report this error and return false.
                logger.error("No source field could be bound to " + mf.getSource());
                return false;
            }
        } else { // The destination's receivedRecord is not yet defined, so add the source record to the destination.
            sourceField = bindField(source.getRecords(), mf.getSource(), null);
            if (null == sourceField) {
                logger.error("No source field named '" + mf.getSource() + " found in node " + source.getName());
                return false;
            }
            currentDestinationField = new Pair<String, String>(sourceField.type, ((mf.getDestination() == null) ? mf.getSource() : mf.getDestination()));
            logger.debug("Adding field " + currentDestinationField.toString() + " to " + destination.getName() + ".receivedFields.");
            destination.records.get("received").record.add(currentDestinationField);
        }
        if (currentMappings.mapping.containsKey(currentDestinationField.second)) {
            logger.info("Mapping overrides source of receivedFields." + currentDestinationField.second + " in " + destination.nodeName);
        }
        currentMappings.mapping.put(currentDestinationField.second, new MappingExpressionSource(sourceField));
        return true;
    }

    boolean interpretMapping(ProcessingNetworkNode source, ProcessingNetworkNode destination, hu.sztaki.ilab.giraffe.schema.dataprocessing.RecordRoute.MapExpression me, RecordMapping currentMappings, boolean destinationReceivedRecordDefined) {
        // This version of interpretMapping works with mapping expressions.
        return ExpressionParsers.parseMappingExpression(me.getExpression(), source.getRecords(), destination.getRecords().get("received"), currentMappings, destinationReceivedRecordDefined);
    }

    // We must establish a type-valid mapping for each destination field.
    // First, we must check if there is an explicit mapping for the given field.
    // In case there is a type mismatch we give up and return null.
    // The explicit mapping may name the source (received or created) for the field.
    // If it does not, default to 'created', and warn if such a field exists in both.
    // If not, we try to map the source field with the same name. Again, in case of a type
    // mismatch, we give up.
    /* //////////////////////////
     * Record type derivation rules
     * //////////////////////////
     * For regular processing network nodes, the input of the node (the received fields record) and the output of the node
     * (the created fields record) are explicitly declared, therefore no type derivation is required when a route connects
     * two regular processing nodes. Type derivation is only used in cases involving data sinks, data sources and pipes.
     * - Data sources:
     *  For real (threaded) data sources, the names of the fields are known, as they are specified in <column name="$name"> format if the input
     *  is a stream, or the column name is available from SQL if it is a JDBC data source. Their type, however is not known. In the case of
     *  streams, every field "starts life" as a String, but a conversion may convert them to any other type. JDBC fields have a type, but they
     *  to may be converted into any type expected if the necessary conversion function is defined.
     *  Virtual data sources have identical created and received fields, thus the first connection to the virtual data source defines its record
     *  format.
     * - Data sinks:
     *  Read (threaded) data sinks must convert incoming records into Object[] arrays (where the actual members of the array may be a strings
     *  if output is sent to a stream, or some other datatype of output is sent to a JDBC resource). A data sink doesn't have to know the name
     *  of the output fields, only their order and type (in both stream and JDBC cases). As a result real data sinks must have an explicit mapping
     *  which determines which fields of the source node are passed on to the data sink.
     *  Virtual data sinks have an identical record format to the record format the parent processing network expects from the embedded network.
     * - Pipes:
     *  Pipes have identical input/output formats. Since the regular network nodes (processing nodes) have an explicitly defined received record format,
     *  the recipient of the pipe's output defines its format. If the pipe is connected to a data sink, then the format of the pipe should be determined
     *  by the input of the pipe (in this case the mapping must be explicit).
     *
     * In light of all this, the algorithm used to decide the record format of a node is the following:
     * 0. If the route connects two regular nodes then everything is explicitly defined, and we're set. In this case, deduceRecordFormats is not called.
     * 1. If the destination of a route is a threaded data sink, then the route definition must include an explicit mapping, so we just use that to define the received fields record.
     * 2. If the source of a route is a threaded data source then the destination should not be a data sink, so we can use the destination's type to determine the source node's created record type.
     * 4. If either the source or the destination is a pipe, then (since the created and recevied record formats are identical), we can use either the outgoing routes or incoming routes to deduce the record format. Incoming routes may be a better choice simply because they are probably coming from a processing node.
     * Since a node's incoming/outgoing routes potentially affect it's record formats, all routes are registered before types and mappings are.
     */
    // This is referred to as case 2 in the comment above.
    private boolean deduceDataSourceCreatedRecordFormat(ProcessingNetworkNode source, ProcessingNetworkNode destination) {
        if (destination.records.get("received").record.isEmpty()) {
            logger.error("The created record format of data source node " + source.getName() + " is determined to be the same as the received record format of node " + destination.getName() + ". However, this is not known.");
            return false;
        }
        for (Pair<String, String> field : destination.records.get("received").record) {
            source.records.get("created").record.add(field);
        }
        return true;
    }

    RecordMapping mapFields(ProcessingNetworkNode source, ProcessingNetworkNode destination,
            java.util.List<Object> mappings) {
        logger.debug("Mapping fields for route " + source.getName() + " to " + destination.getName());
        boolean destinationReceivedRecordDefined = true;
        // the record formats of the source and destination nodes must be defined for field mapping to work.
        if (source.records.get("created").record.isEmpty()) {
            if (!deduceDataSourceCreatedRecordFormat(source, destination)) {
                return null;
            }
        }
        if (destination.records.get("received").record.isEmpty()) {
            // if this is the data sink portion of a pipe, then the received record = created record, which is determined by the nodes adjacent to the data sink of the pipe.
            if (destination.isDataSink() && destination.isDataSource()) {
                if (destination.outgoingRoutes.isEmpty() || !deduceDataSourceCreatedRecordFormat(destination, destination.outgoingRoutes.get(0).destination)) {
                    logger.error("Error deducing the received record type of async data pipe '" + destination.getName() + "'. This must be the same as the created record type of the associated data source, but either the source is not connectd to any other node or the created record format of the source could not be deduced.");
                    return null;
                }
            }
            if (mappings.size() < 1) {
                logger.error("The destination node (" + destination.getName() + ") has an undefied receivedRecord, so explicit field mapping must be specified for the " + source.getName() + " -> " + destination.getName() + " route.");
                return null;
            }
            destinationReceivedRecordDefined = false;
        }
        RecordMapping m = new RecordMapping();
        /* There are two kinds of objects in mappings:
         *  - RecordRoute.MapField : This maps a single source field to a single destination field.
         *  - RecordRoute.MapExpression : This maps an arbitrary java expression to a destination field
         *  expression, which identifies one or more expressions.
         *  We interpret all explicit mappings. If no mapping information is given, for a given destination field,
         *  then the default field mapping assigns each destination field the source node's created:$fieldname (or if this doesn't exist) then received:$fieldname.
         */
        for (Object o : mappings) {
            try {
                hu.sztaki.ilab.giraffe.schema.dataprocessing.RecordRoute.MapField mf = (hu.sztaki.ilab.giraffe.schema.dataprocessing.RecordRoute.MapField) o;
                if (!interpretMapping(source, destination, mf, m, destinationReceivedRecordDefined)) {
                    return null;
                }
            } catch (ClassCastException ex1) {
                try {
                    hu.sztaki.ilab.giraffe.schema.dataprocessing.RecordRoute.MapExpression me = (hu.sztaki.ilab.giraffe.schema.dataprocessing.RecordRoute.MapExpression) o;
                    if (!interpretMapping(source, destination, me, m, destinationReceivedRecordDefined)) {
                        return null;
                    }
                } catch (ClassCastException ex2) {
                    logger.error("Mapping object could be cast to neither RecordRoute.MapField or RecordRoute.MapExpression. ", ex2);
                    return null;
                }
            }
        }
        // Fields which are not explicitly mapped should be mapped implicitly to fields of the same and and type in created:, receieved: (in that order).

        for (Pair<String, String> destField : destination.getRecords().get("received").record) {
            if (!m.mapping.containsKey(destField.second)) {
                // No explicit mapping for destField was given, let's try to find an implicit mapping!
                Field sourceField = bindField(ProcessingNetworkGenerator.recordOrder, source.records, destField);
                if (sourceField == null) {
                    logger.error("Destination field " + destField.toString() + " could not be mapped to any source field.");
                    return null;
                }
                m.mapping.put(destField.second, new MappingExpressionSource(sourceField));
            }
        }
        if (destination.isDataSink() && destination.records.get("created").record.isEmpty()) {
            // This is the case for data sinks which have no explicit record definition.
            RecordExporter ex = process.outputs.get(destination.getName());
            if (null == ex) {
                logger.error("Could not find record exporter for data sink " + destination.getName());
                return null;
            }
            for (Pair<String, String> col : destination.records.get("received").record) {
                destination.records.get("created").record.add(new Pair<String, String>(ex.getFieldType(col.second), col.second));
            }
        }
        return m;
    }

    /* initializeNetwork does the following:
     * - Evaluate field mappings in route definitions, and define record formats
     *   for nodes which lack them (sinks, sources, pipes).
     * - Count the number of data sources and initialize the corresponding CountdownLatch.
     */
    public boolean initializeNetwork() {
        // First, we analyze route definitions and evaluate mapping expressions.
        for (RecordRoute currentRoute : this.routes) {
            if (null == (currentRoute.onEvent = RouteCondition.getRouteCondition(currentRoute.routeDefinition.getCondition(), currentRoute.source.src.getEventConditionGenerator()))) {
                logger.error("Error initializing event conditions for the route connecting " + currentRoute.source.getName() + " to " + currentRoute.destination.getName());
                return false;
            }
            // Establish field mappings between the output of the source node and the input of the destination node
            if (null == (currentRoute.receivedFieldsMapping = mapFields(currentRoute.source, currentRoute.destination, currentRoute.routeDefinition.getRouteFieldMapping()))) {
                logger.error("Error initializing record mapping for the route connecting " + currentRoute.source.getName() + " to " + currentRoute.destination.getName());
                return false;
            }
            // We won't need this any longer.
            currentRoute.routeDefinition = null;
        }
        int numDataSourceThreads = 0;
        for (ProcessingNetworkNode n :this.nodes.values()) if (n.isDataSource() && !n.isVirtual()) numDataSourceThreads++;
        process.dataSourcesThreadsLatch = new java.util.concurrent.CountDownLatch(numDataSourceThreads);
        return true;
    }

    private boolean addRoute(ProcessingNetworkNode source, ProcessingNetworkNode destination, hu.sztaki.ilab.giraffe.schema.dataprocessing.RecordRoute routeDefinition) {
        RecordRoute route = new RecordRoute(source, destination, routeDefinition);
        routes.add(route);
        logger.debug("registering route from '" + source.getName() + "' to '" + destination.getName() + "'.");
        route.source.getOutgoingRoutes().add(route);
        route.destination.getIncomingRoutes().add(route);
        return true;
    }

    public boolean addRoute(hu.sztaki.ilab.giraffe.schema.dataprocessing.RecordRoute routeDefinition) {
        ProcessingNetworkNode dest = null;
        // add default route condition if none is set
        if (routeDefinition.getCondition() == null) {
            routeDefinition.setCondition(defaults.getEvents().getDefaultEventCondition());
        }
        if (null == (dest = nodes.get(routeDefinition.getDestination()))) {
            logger.error("Undefined node referred to as route destination: " + routeDefinition.getDestination());
            return false;
        }
        java.util.Set<ProcessingNetworkNode> sourceNodes = ExpressionParsers.parseNodeSetExpression(routeDefinition.getSource(), nodes);
        if (null == sourceNodes) {
            logger.error("Error parsing source node expression '" + routeDefinition.getSource() + "'.");
            return false;
        }
        if (sourceNodes.size() == 0) {
            logger.warn("Source node expression '" + routeDefinition.getSource() + "' identifies 0 nodes; no routes will be added.");
        }
        for (ProcessingNetworkNode source : sourceNodes) {
            if (!addRoute(source, dest, routeDefinition)) {
                return false;
            }
        }
        return true;
    }

    public String generateSourceCode() {
        StringWriter codeWriter = new StringWriter();
        codeWriter.write("/* Giraffe 2 generated code for processing network '" + networkName + "'.\n");
        codeWriter.write(" * This file contains generated code, modifying it does not affect the\n");
        codeWriter.write(" * ETL process. This class was generated on: " + new java.util.Date().toString() + " */\n");
        for (String i : this.imports) {
            codeWriter.write("import " + i + ";\n");
        }
        codeWriter.write("public class " + networkName + " implements ProcessingElementBaseClasses.ProcessingNetwork {\n");
        codeWriter.write("  // --- Define the classes shared by several nodes of the network --- \n");
        for (RecordDefinition rec : ProcessingNetworkGenerator.sharedRecordDefinitions.values()) {
            if (rec.owner.equals(RecordDefinition.RecordDefinitionClassOwner.NETWORK)) {
                codeWriter.write(rec.generateRecordClass());
            }
        }
        codeWriter.write("  // --- Define the classes belonging to each node of the network --- \n");
        for (ProcessingNetworkNode n : this.nodes.values()) {
            codeWriter.write(n.src.getSource() + "\n");
            // copy object instantiators for tasks and events.
            process.eventInstantiators.put(n.getName(), n.src.getEventInstantiator());
            process.taskInstantiators.put(n.getName(), n.src.getTaskInstantiator());
        }
        codeWriter.write("  // --- Declare the nodes of the network as fields of the outer class.--- \n");
        for (ProcessingNetworkNode n : this.nodes.values()) {
            codeWriter.write("  " + n.src.getClassName() + " " + n.nodeName + ";\n");
        }
        codeWriter.write("  hu.sztaki.ilab.giraffe.core.processingnetwork.Process process;\n");
        // write network constructor
        writeNetworkConstructor(codeWriter);
        writeNetworkMethods(codeWriter);
        // close class
        codeWriter.write("}\n");
        return codeWriter.toString();
    }

    private void writeNetworkConstructor(StringWriter codeWriter) {
        codeWriter.write("  public " + networkName + "(hu.sztaki.ilab.giraffe.core.processingnetwork.Process process) {\n");
        codeWriter.write("  this.process = process;\n");
        // instantiate classes representing nodes.
        for (ProcessingNetworkNode n : this.nodes.values()) {
            codeWriter.write("  " + n.nodeName + " = " + n.src.instantiate() + ";\n");
        }
        // instantiate threads for each datasource node.
        for (ProcessingNetworkNode n : this.nodes.values()) {
            if (n.isDataSource() && !n.isVirtual()) {
                codeWriter.write("  this.process.threads.add(new Thread("+n.getName()+"));\n");
            }
        }
        codeWriter.write("  }\n");
    }

    private void writeNetworkMethods(StringWriter codeWriter) {
        codeWriter.write("  public void start() {for (Thread th : this.process.threads) th.start();}\n");
        codeWriter.write("  public void dataSourcesRequestStop() {\n");
        for (ProcessingNetworkNode n : nodes.values()) if (n.isDataSource() && !n.isVirtual()){
            codeWriter.write("    "+n.getName()+".requestStop();\n");
        }
        codeWriter.write("  }\n");        
    }
}
