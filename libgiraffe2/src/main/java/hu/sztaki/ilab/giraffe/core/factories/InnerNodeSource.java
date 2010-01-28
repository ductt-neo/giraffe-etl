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

import hu.sztaki.ilab.giraffe.core.factories.ProcessingNetworkGenerator.MappingExpressionSource;
import hu.sztaki.ilab.giraffe.core.factories.ProcessingNetworkGenerator.RecordRoute;
import hu.sztaki.ilab.giraffe.core.util.Pair;
import hu.sztaki.ilab.giraffe.schema.datatypes.ObjectInstance;
import hu.sztaki.ilab.giraffe.schema.datatypes.Parameters;
import java.io.StringWriter;

/**
 *
 * @author neumark
 */
public class InnerNodeSource implements ProcessingNetworkGenerator.ProcessingNodeSource {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(InnerNodeSource.class);

    class MethodCall {

        String instantiatedClass = null;
        String instanceName = null;
        Parameters instanceParameters = null;
        String code = "";
        java.util.List<String> inputFields = new java.util.LinkedList<String>();
        String outputField = null;
    }
    ProcessingNetworkGenerator.ProcessingNetworkNode node;
    String networkName;
    java.util.List<MethodCall> tasks = new java.util.LinkedList<MethodCall>();
    ObjectInstantiator taskObjects = new ObjectInstantiator("taskObject");
    ObjectInstantiator eventObjects = new ObjectInstantiator("eventObject");
    EventConditionGenerator evtGen;
    java.util.List<String> sendFunctionNames = new java.util.LinkedList<String>();

    public ObjectInstantiator getTaskInstantiator() {
        return this.taskObjects;
    }

    public ObjectInstantiator getEventInstantiator() {
        return this.eventObjects;
    }

    public InnerNodeSource(ProcessingNetworkGenerator.ProcessingNetworkNode node, String networkName) {
        this.node = node;
        this.networkName = networkName;
        saveClassNames();
        evtGen = new EventConditionGenerator(eventObjects);
    }

    public void saveClassNames() {
        for (java.util.Map.Entry<String, ProcessingNetworkGenerator.RecordDefinition> rec : this.node.records.entrySet()) {
            if (rec.getValue().owner.equals(ProcessingNetworkGenerator.RecordDefinition.RecordDefinitionClassOwner.NODE) && this.node.records.get(rec.getKey()).className == null) {
                this.node.records.get(rec.getKey()).className = this.getClassName() + "_" + ProcessingNetworkGenerator.recordTypeNames.get(rec.getKey());
            }
        }
    }

    public String getClassName() {
        return toClassName(this.node.getName());
    }

    public String getBaseClass() {
        return "ProcessingElementBaseClasses.InnerNode";
    }

    // intput is a list of (variable name, (field type, field name ))
    // output is a String of form: variable name 1, variable name 2, ..., variable name n
    String assembleInputs(java.util.List<Pair<String, Pair<String, String>>> input) {
        String code = "";
        for (Pair<String, Pair<String, String>> p : input) {
            if (code.length() > 0) {
                code += ", ";
            }
            code += ProcessingNetworkGenerator.standardInstanceNames.get("received") + "." + p.second.second;
        }
        return code;
    }

    public EventConditionGenerator getEventConditionGenerator() {
        return this.evtGen;
    }

    String getterName(String varName) {
        return "get" + varName.substring(0, 1).toUpperCase() + varName.substring(1);
    }

    // intput and output are lists of (variable name, (field type, field name ))
    public boolean addTask(java.util.List<Pair<String, Pair<String, String>>> input, java.util.List<Pair<String, Pair<String, String>>> output, ObjectInstance call) {
        MethodCall c = new MethodCall();        
        String args = this.assembleInputs(input);
        String callingCode = taskObjects.generateCallingCode(call, args);
        if (call.getClazz() != null) {            
            c.instantiatedClass = call.getClazz();
            c.instanceName = "taskObject" + InnerNodeSource.this.tasks.size();
            c.instanceParameters = call.getParameters();
            // call the member function of the specified class.
            StringWriter codeWriter = new StringWriter();
            codeWriter.write("    if (!" + callingCode + ") {\n");
            codeWriter.write("    this.events = ProcessingElementBaseClasses.addEvent(this.events, EventType.ERROR_TASK_FAILED);\n");
            codeWriter.write("    // Copy fields regardless of task completion error.\n");
            codeWriter.write("    }\n");
            for (Pair<String, Pair<String, String>> o : output) {
                codeWriter.write("    " + ProcessingNetworkGenerator.standardInstanceNames.get("created") + "." + o.second.second + " = " + c.instanceName + "." + getterName(o.first) + "();\n");
            }
            c.code += codeWriter.toString();
        } else {            
            if (output.size() != 1) {
                logger.error("Tasks performing static method calls should return exactly 1 field!");
                return false;
            }
            c.code += "    " + ProcessingNetworkGenerator.standardInstanceNames.get("created") + "." + output.get(0).second.second + " = " + callingCode + ";\n";
        }        
        // Now we must close the try block and catch the exceptions
        tasks.add(c);
        return true;
    }

    @Override
    public String getSource() {
        StringWriter codeWriter = new StringWriter();
        // Write record classes.
        for (java.util.Map.Entry<String, ProcessingNetworkGenerator.RecordDefinition> record : this.node.getRecords().entrySet()) {
            if (record.getValue() != null && record.getValue().owner.equals(ProcessingNetworkGenerator.RecordDefinition.RecordDefinitionClassOwner.NODE)) {
                ProcessingNetworkGenerator.RecordDefinition rec = this.node.getRecords().get(record.getKey());
                if (null != rec) {
                    codeWriter.write(rec.generateRecordClass());
                    codeWriter.write("\n");
                }
            }
        }
        codeWriter.write("public class " + this.getClassName());
        if (this.getBaseClass() != null) {
            codeWriter.write(" extends " + this.getBaseClass());
        }
        codeWriter.write(" {\n");
        writeClassFields(codeWriter);
        writeClassBody(codeWriter);
        // write function called to assemble error record
        codeWriter.write(generateUpdateErrorRecordFunction());
        // close class
        codeWriter.write("}\n");
        return codeWriter.toString();
    }

    void writeClassFields(StringWriter codeWriter) {
        // write class fields
        codeWriter.write("  // Class fields\n");
        codeWriter.write(generateNodeData());
        for (java.util.Map.Entry<String, ProcessingNetworkGenerator.RecordDefinition> fieldType : this.node.getRecords().entrySet()) {
            if (fieldType.getKey().equals("nodedata")) {
                continue; // nodedata is a special case.
            }
            if (fieldType.getValue() != null) {
                codeWriter.write("  " + fieldType.getValue().className + " " + ProcessingNetworkGenerator.standardInstanceNames.get(fieldType.getKey()) + " = null;\n");
            }
        }
        // Objects instantiated for use by worker tasks or event predicates are also class fields
        codeWriter.write(this.eventObjects.generateFieldDeclarations());
        codeWriter.write(this.taskObjects.generateFieldDeclarations());
    }

    void writeClassBody(StringWriter codeWriter) {
        // Write class constructor.        
        codeWriter.write(generateConstructor());
        codeWriter.write(generateWorkerFunction());
        codeWriter.write(generateSendFunctions());
        codeWriter.write(generateReceiverFunction());
    }

    String generateConstructor() {
        StringWriter codeWriter = new StringWriter();
        codeWriter.write("  // Constructor\n");
        codeWriter.write("  public " + this.getClassName() + "(hu.sztaki.ilab.giraffe.core.factories.ObjectInstantiator taskObjects, hu.sztaki.ilab.giraffe.core.factories.ObjectInstantiator eventObjects) {\n");
        codeWriter.write("    // Assign each class field their corresponding objects.\n");
        codeWriter.write(taskObjects.generateGetInstancesCode("taskObjects"));
        codeWriter.write(eventObjects.generateGetInstancesCode("eventObjects"));
        codeWriter.write("  }\n");
        return codeWriter.toString();
    }

    public String instantiate() {
        return " new " + getClassName() + "(" +
                "this.process.taskInstantiators.get(\"" + this.node.nodeName + "\")," +
                "this.process.eventInstantiators.get(\"" + this.node.nodeName + "\"))";
    }

    String generateWorkerFunction() {
        StringWriter codeWriter = new StringWriter();
        // gets input and output as parameters.
        codeWriter.write("  protected void work()  throws java.lang.Throwable {\n");
        codeWriter.write("    this.events = ProcessingElementBaseClasses.defaultEvents;\n");
        codeWriter.write("    " + ProcessingNetworkGenerator.standardInstanceNames.get("created") + " = new " + this.node.getRecords().get("created").className + "();\n");
        codeWriter.write("    try {\n");
        for (InnerNodeSource.MethodCall m : tasks) {
            codeWriter.write(m.code);
        }
        codeWriter.write("    } catch(Throwable th) {\n");
        codeWriter.write("      throw th;\n");
        codeWriter.write("    }\n");
        codeWriter.write("  }\n");
        return codeWriter.toString();
    }

    String generateUpdateErrorRecordFunction() {
        StringWriter codeWriter = new StringWriter();
        codeWriter.write("  protected void updateErrorRecord(java.lang.Throwable ex) {\n");
        codeWriter.write("    " + ProcessingNetworkGenerator.standardInstanceNames.get("error") + " = new " + this.node.getRecords().get("error").className + " (ex, ");
        codeWriter.write(ProcessingNetworkGenerator.standardInstanceNames.get("received") + ", ");
        codeWriter.write(ProcessingNetworkGenerator.standardInstanceNames.get("created") + ", ");
        codeWriter.write(ProcessingNetworkGenerator.standardInstanceNames.get("nodedata") + ", ");
        codeWriter.write("events);");
        codeWriter.write("  }\n");
        return codeWriter.toString();
    }

    String generateNodeData() {
        StringWriter codeWriter = new StringWriter();
        codeWriter.write("  public final " + this.node.getRecords().get("nodedata").className + " " + ProcessingNetworkGenerator.standardInstanceNames.get("nodedata"));
        codeWriter.write(" = new " + this.node.getRecords().get("nodedata").className + "(");
        codeWriter.write("\"" + this.node.getName() + "\", ");
        codeWriter.write("\"" + this.getClassName() + "\"");
        codeWriter.write(");\n");
        return codeWriter.toString();
    }


    /* Receive calls work() and then send(). If something goes wrong, the sender is notified by the
     * return value.
     *
     */
    void writeProcessMonitorInvocation(StringWriter codeWriter) {
        // call the process monitor if the monitor frequency is defined and it has been reached.
        if (this.node.monitorFrequency > 0 && this.node.getGenerator().process != null) {
            codeWriter.write("  this.recordsRead++;\n");
            codeWriter.write("  if ((this.recordsRead % " + this.node.monitorFrequency + ") == 0) " + this.networkName + ".this.process.monitor.onNodeEntry(\"" + this.node.getName() + "\",this.recordsRead);\n");
        }
    }

    String generateReceiverFunction() {
        StringWriter codeWriter = new StringWriter();
        codeWriter.write("  public void receive(" + this.node.getRecords().get("received").className + " " + ProcessingNetworkGenerator.standardInstanceNames.get("received") + ") {\n");
        codeWriter.write("    this." + ProcessingNetworkGenerator.standardInstanceNames.get("received") + " = " + ProcessingNetworkGenerator.standardInstanceNames.get("received") + ";\n");
        writeProcessMonitorInvocation(codeWriter);
        codeWriter.write("    // Call the parent class's receive0 function.\n");
        codeWriter.write("    this.receive0();\n");
        codeWriter.write("  }\n");
        return codeWriter.toString();
    }

    String generateSendFunctions() {
        StringWriter codeWriter = new StringWriter();
        for (ProcessingNetworkGenerator.RecordRoute r : this.node.getOutgoingRoutes()) {
            codeWriter.write(generateSendToFunction(r));
        }
        codeWriter.write("  // row counters for each outgoing route. \n");
        for (ProcessingNetworkGenerator.RecordRoute r : this.node.getOutgoingRoutes()) {
            codeWriter.write("  int " + r.destination.getName() + " = 0;\n");
        }
        codeWriter.write("  protected void send() {\n");
        for (String fn : sendFunctionNames) {
            codeWriter.write("  " + fn + "();\n");
        }
        codeWriter.write("  }\n");
        return codeWriter.toString();
    }

    String generateSendToFunction(RecordRoute outgoingRoute) {
        StringWriter codeWriter = new StringWriter();
        String functionName = "sendTo_" + outgoingRoute.destination.nodeName;
        sendFunctionNames.add(functionName);
        codeWriter.write("  void " + functionName + "() {\n");
        // Evaluate condition associated with outgoing route.
        // The following generated code exits the sendTo_* function returning
        // false if the expected condition is not met.
        codeWriter.write("  if(!(" + outgoingRoute.onEvent.getIfStatement() + ")) return;\n");
        // Create the record object which will be passed to the destination node.
        codeWriter.write("  " + outgoingRoute.destination.getRecords().get("received").className + " outgoingRecord = new " + outgoingRoute.destination.getRecords().get("received").className + "();\n");
        codeWriter.write("  // Copy fields from current node's records to the newly created outgoingRecord object.\n");
        int fieldCounter = 0;
        for (java.util.Map.Entry<String, MappingExpressionSource> m : outgoingRoute.receivedFieldsMapping.mapping.entrySet()) {
            String source = null;
            if (m.getValue().isJavaExpression) {
                source = m.getValue().javaExpression;
            } else {
                source = ProcessingNetworkGenerator.standardInstanceNames.get(m.getValue().sourceField.record) + "." + m.getValue().sourceField.field;
            }
            codeWriter.write("  outgoingRecord." + m.getKey() + " = " + source + ";\n");
            fieldCounter++;
        }
        codeWriter.write("  // pass the outgoingRecord object to the destination node.\n");
        codeWriter.write("  " + networkName + ".this." + outgoingRoute.destination.nodeName + ".receive(outgoingRecord);\n");
        codeWriter.write("  this.events = ProcessingElementBaseClasses.addEvent(this.events, EventType.ROUTING_RECORD_HAS_BEEN_DELIVERED);\n");
        // call process monitor if it is set and the monitorfrequency has been reached.
        if (outgoingRoute.monitorFrequency > 0 && this.node.getGenerator().process != null) {
            codeWriter.write("  " + outgoingRoute.destination.getName() + "++;\n");
            codeWriter.write("  if ((" + outgoingRoute.destination.getName() + " % " + outgoingRoute.monitorFrequency + ") == 0) " + this.networkName + ".this.process.monitor.onRouteEntry(\"" + outgoingRoute.source.getName() + "\",\"" + outgoingRoute.destination.getName() + "\"," + outgoingRoute.destination.getName() + ");\n");
        }
        codeWriter.write("  }\n");
        return codeWriter.toString();
    }

    static String toClassName(String n) {
        return n.substring(0, 1).toUpperCase() + n.substring(1);
    }
}
