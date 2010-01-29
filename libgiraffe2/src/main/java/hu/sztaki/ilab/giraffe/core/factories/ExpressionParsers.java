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

import hu.sztaki.ilab.giraffe.core.factories.nodesets.NodeSetExpressionParser;
import hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.RecordMappingExpressionParser;
import hu.sztaki.ilab.giraffe.core.util.Pair;

/**
 *
 * @author neumark
 */
public class ExpressionParsers {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ExpressionParsers.class);
    private static RecordMappingExpressionParser recordMappingParser = new RecordMappingExpressionParser(new java.io.StringReader(""));
    private static NodeSetExpressionParser nodeSetExpressionParser = new NodeSetExpressionParser(new java.io.StringReader(""));

    static class METreeWalker {

        hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode root;
        java.util.Map<String, ProcessingNetworkGenerator.RecordDefinition> sourceRecords;
        ProcessingNetworkGenerator.RecordDefinition destination;
        ProcessingNetworkGenerator.RecordMapping currentMapping;
        boolean destinationReceivedRecordDefined;

        public METreeWalker(hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode root,
                java.util.Map<String, ProcessingNetworkGenerator.RecordDefinition> sourceRecords,
                ProcessingNetworkGenerator.RecordDefinition destination,
                ProcessingNetworkGenerator.RecordMapping currentMapping,
                boolean destinationReceivedRecordDefined) {
            this.root = root;
            this.sourceRecords = sourceRecords;
            this.destination = destination;
            this.currentMapping = currentMapping;
            this.destinationReceivedRecordDefined = destinationReceivedRecordDefined;
        }

        public boolean walk() {
            return walk0(root);
        }

        private java.util.List<ProcessingNetworkGenerator.MappingExpressionSource> evaluateRightSide(hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode expRoot) {
            String expNodeType = expRoot.toString();
            java.util.List<ProcessingNetworkGenerator.MappingExpressionSource> source = new java.util.LinkedList<ProcessingNetworkGenerator.MappingExpressionSource>();
            if ("JavaExpression".equals(expNodeType)) {
                String type = null;
                String exp = expRoot.jjtGetValue().toString();
                if (exp.startsWith(":")) {
                    // no type information given
                    exp = exp.substring(1);
                } else {
                    String[] parts = exp.split(":", 2);
                    type = parts[0];
                    exp = parts[1];
                }
                // remove '{' and '}' from java expression
                exp = exp.substring(1, exp.length() - 1);
                source.add(new ProcessingNetworkGenerator.MappingExpressionSource(exp, type));
            } else if ("FieldOrRecord".equals(expNodeType)) {
                String name = expRoot.jjtGetValue().toString().substring(1);
                char type = expRoot.jjtGetValue().toString().charAt(0);
                switch (type) {
                    case 'F': // Unqualified field name
                        Pair<String, String> destinationField = ProcessingNetworkGenerator.getField(destination, ProcessingNetworkGenerator.unqualifyFieldName(name));
                        if (destinationField == null) {
                            logger.error("No destination field with given name found.");
                            return null;
                        }
                        source.add(new ProcessingNetworkGenerator.MappingExpressionSource(ProcessingNetworkGenerator.bindField(sourceRecords, name, destinationField.first)));
                        break;
                    case 'R': // Record name
                        String recordName = name.substring(0, name.length() - 1);
                        ProcessingNetworkGenerator.RecordDefinition srcRec = this.sourceRecords.get(recordName);
                        if (srcRec == null) {
                            logger.error("Expression references source record " + name + " which could not be located");
                            return null;
                        }
                        // Add each field in the record to the source field set
                        for (Pair<String, String> field : srcRec.record) {
                            source.add(new ProcessingNetworkGenerator.MappingExpressionSource(new ProcessingNetworkGenerator.Field(recordName, field.first, field.second)));
                        }
                        break;
                    default:
                        logger.error("Error parsing expression '" + expRoot.jjtGetValue().toString() + "': no such field or record type: " + type);
                        return null;
                }
            } else {
                logger.error("Error evaluation right side of expression: choked on node " + expRoot.toString());
                return null;
            }
            return source;
        }

        private java.util.Set<String> evaluateLeftSide(hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode expRoot, java.util.Set<String> potentialDestinationFields) {
            String expNodeType = expRoot.toString();
            if ("ComplexFE".equals(expNodeType) && expRoot.jjtGetNumChildren() > 1) {
                java.util.Set<String> definedSet = new java.util.HashSet<String>();
                // This is something like fieldA + fieldB or fieldA + (fieldB - fieldC)
                boolean addTo = true; // true if the next term should be added to the sum, false otherwise.
                for (int i = 0; i < expRoot.jjtGetNumChildren(); i++) {
                    hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode currentChild = (hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode) expRoot.jjtGetChild(i);
                    String currentChildType = currentChild.toString();
                    if ("Minus".equals(currentChildType)) {
                        addTo = false;
                        continue;
                    }
                    if ("Plus".equals(currentChildType)) {
                        addTo = false;
                        continue;
                    }
                    java.util.Set<String> term = evaluateLeftSide(currentChild, potentialDestinationFields);
                    if (addTo) {
                        definedSet.addAll(term);
                    } else {
                        definedSet.removeAll(term);
                    }
                }
                return definedSet;
            }
            if ("FieldName".equals(expNodeType)) {
                String fieldName = expRoot.jjtGetValue().toString();
                java.util.Set<String> definedSet = new java.util.HashSet<String>();
                if (fieldName.equals("*")) {
                    definedSet.addAll(potentialDestinationFields);
                } else {
                    definedSet.add(fieldName);
                }
                if (definedSet.size() == 0) {
                    logger.debug("Field " + fieldName + " not found in destination record.");
                }
                return definedSet;
            }
            // For any other node type, we return the children
            if (expRoot.jjtGetNumChildren() != 1) {
                logger.error("A node of type " + expNodeType + " should have exactly one child. This is a bug in giraffe2!");
                return null;
            }
            return evaluateLeftSide((hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode) expRoot.jjtGetChild(0), potentialDestinationFields);
        }

        java.util.Set<String> getPotentialDestinationFields(java.util.List<ProcessingNetworkGenerator.MappingExpressionSource> sources) {
            java.util.Set<String> destFields = new java.util.HashSet<String>();
            if (this.destinationReceivedRecordDefined) {
                for (Pair<String, String> p : this.destination.record) {
                    destFields.add(p.second);
                }
            } else {
                for (ProcessingNetworkGenerator.MappingExpressionSource mes : sources) {
                    if (mes.isJavaExpression) {
                        continue;
                    }
                    destFields.add(mes.sourceField.field);
                }
            }
            return destFields;
        }

        private boolean walk0(hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode node) {
            // if root is a definition, then evaluate left and right parts separately:
            if (node.toString() != null && "Definition".equals(node.toString())) {
                hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode leftBranch = (hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode) node.jjtGetChild(0);
                hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode rightBranch = (hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode) node.jjtGetChild(1);
                // sources (the right hand side of the definition) must be evaluated first, because if the destination is a data sink, then the
                // fields in the destination record may not be defined yet.
                // For this reason, the source mapping expressions may be used by evaluateLeftSide.
                java.util.List<ProcessingNetworkGenerator.MappingExpressionSource> sources = evaluateRightSide(rightBranch);
                if (sources == null) {
                    return false;
                }
                java.util.Set<String> destinationFields = evaluateLeftSide(leftBranch, getPotentialDestinationFields(sources));
                if (destinationFields == null) {
                    return false;
                }
                // Pair destination fields with mapping expressions.
                // If the source is a java expression, then there can only be one item in the destination.
                if (sources.size() == 1 && sources.get(0).isJavaExpression) {
                    String destFieldName = (String) destinationFields.toArray()[0];
                    ProcessingNetworkGenerator.MappingExpressionSource source = sources.get(0);
                    if (destinationFields.size() != 1) {
                        logger.error("Mapping a java expression to several destination fields is not allowed!");
                        return false;
                    }
                    // If the destination node is a data sink, then its received record may not yet be defined. In this case,
                    // add the destinationFields item to the record definition with the type of the java expression.
                    if (!this.destinationReceivedRecordDefined) {
                        if (source.sourceField.type == null) {
                            logger.error("If a route's destination is a data sink, then any java expression used in the mapping definition must have type information.\nExample: use fieldA = java.lang.Integer{new Integer(3)} instead of fieldA = {new Integer(3)}");
                            return false;
                        }
                        this.destination.record.add(new Pair<String, String>(source.sourceField.type, destFieldName));
                    } else {
                        Pair<String,String> destinationFieldDescription = ProcessingNetworkGenerator.getField(destination, destFieldName);
                        if (null == destinationFieldDescription) {
                            logger.error("Field "+destFieldName+" could not be found in destination record.");
                            return false;
                        }
                        if (!destinationFieldDescription.first.equals(source.sourceField.type)) {
                            logger.error("Type mistmatch between source and destination fields.");
                            return false;
                        }
                    }
                    this.currentMapping.mapping.put(destFieldName, source);
                    return true;
                }
                for (ProcessingNetworkGenerator.MappingExpressionSource me : sources) {
                    if (me.isJavaExpression) {
                        logger.error("A java expression mapping maps exactly one expression to a single field.");
                        return false;
                    }
                    if (!destinationFields.contains(me.sourceField.field)) {
                        logger.debug("Source field " + me.sourceField.toString() + " not mapped to any destination field.");
                        continue;
                    }
                    if (!this.destinationReceivedRecordDefined) {
                        // add this field to the destination record
                        this.destination.record.add(new Pair<String, String>(me.sourceField.type, me.sourceField.field));
                    }
                    this.currentMapping.mapping.put(me.sourceField.field, me);
                }
                return true;
            } else {
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    if (!walk0((hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode) node.jjtGetChild(i))) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    static boolean parseMappingExpression(
            String expression,
            java.util.Map<String, ProcessingNetworkGenerator.RecordDefinition> sourceRecords,
            ProcessingNetworkGenerator.RecordDefinition destination,
            ProcessingNetworkGenerator.RecordMapping currentMapping,
            boolean destinationReceivedRecordDefined) {
        recordMappingParser.ReInit(new java.io.StringReader(expression));
        try {
            hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode root = recordMappingParser.Start();
            METreeWalker tw = new METreeWalker(root, sourceRecords, destination, currentMapping, destinationReceivedRecordDefined);
            return tw.walk();
        } catch (hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.ParseException ex) {
            logger.error("Failed to parse expression, '" + expression + "'.", ex);
            return false;
        }
    }

    static class NSETreeWalker {

        hu.sztaki.ilab.giraffe.core.factories.nodesets.SimpleNode root;
        java.util.Map<String, ProcessingNetworkGenerator.ProcessingNetworkNode> nodes;

        NSETreeWalker(
                hu.sztaki.ilab.giraffe.core.factories.nodesets.SimpleNode root,
                java.util.Map<String, ProcessingNetworkGenerator.ProcessingNetworkNode> nodes) {
            this.root = root;
            this.nodes = nodes;
        }

        interface NodeSetFilter {

            boolean keeper(ProcessingNetworkGenerator.ProcessingNetworkNode n);
        }

        java.util.Set<ProcessingNetworkGenerator.ProcessingNetworkNode> walk() {
            return walk0(root);
        }

        java.util.Set<ProcessingNetworkGenerator.ProcessingNetworkNode> evaluateConstant(String exp) {
            java.util.Set<ProcessingNetworkGenerator.ProcessingNetworkNode> nodeSet = new java.util.HashSet<ProcessingNetworkGenerator.ProcessingNetworkNode>();
            NodeSetFilter filter = null;
            if (exp.startsWith("@")) {
                if ("@all".equals(exp)) {
                    filter = new NodeSetFilter() {

                        public boolean keeper(ProcessingNetworkGenerator.ProcessingNetworkNode n) {
                            return true;
                        }
                    };
                }
                if ("@sources".equals(exp)) {
                    filter = new NodeSetFilter() {

                        public boolean keeper(ProcessingNetworkGenerator.ProcessingNetworkNode n) {
                            return n.isDataSource();
                        }
                    };
                }
                if ("@sinks".equals(exp)) {
                    filter = new NodeSetFilter() {

                        public boolean keeper(ProcessingNetworkGenerator.ProcessingNetworkNode n) {
                            return n.isDataSink();
                        }
                    };
                }
                if ("@nodes".equals(exp)) {
                    filter = new NodeSetFilter() {

                        public boolean keeper(ProcessingNetworkGenerator.ProcessingNetworkNode n) {
                            return !n.isDataSink() && !n.isDataSource();
                        }
                    };
                }
                if ("@pipes".equals(exp)) {
                    filter = new NodeSetFilter() {

                        public boolean keeper(ProcessingNetworkGenerator.ProcessingNetworkNode n) {
                            return n.isDataSink() && n.isDataSource();
                        }
                    };
                }
                if (filter == null) {
                    logger.error("Node set expression '" + exp + "' could not be interpretted.");
                    return null;
                }
                for (ProcessingNetworkGenerator.ProcessingNetworkNode n : nodes.values()) {
                    if (filter.keeper(n)) {
                        nodeSet.add(n);
                    }
                }
            } else { // this is a regular node
                ProcessingNetworkGenerator.ProcessingNetworkNode selectedNode = nodes.get(exp);
                if (null == selectedNode) {
                    logger.error("Error evaluating expression: could not find node '" + exp + "'");
                    return null;
                }
                nodeSet.add(selectedNode);
            }
            return nodeSet;
        }

        java.util.Set<ProcessingNetworkGenerator.ProcessingNetworkNode> walk0(hu.sztaki.ilab.giraffe.core.factories.nodesets.SimpleNode node) {
            String nodeType = node.toString();
            if ("Constant".equals(nodeType)) {
                return evaluateConstant(node.jjtGetValue().toString());
            }
            if ("E".equals(nodeType) && node.jjtGetNumChildren() > 1) {
                // An expression may have several children if it is a set of operations like A + B - C + D
                java.util.Set<ProcessingNetworkGenerator.ProcessingNetworkNode> nodeSet = new java.util.HashSet<ProcessingNetworkGenerator.ProcessingNetworkNode>();
                boolean addSet = true;
                for (int i = 0; i < node.jjtGetNumChildren(); ++i) {
                    hu.sztaki.ilab.giraffe.core.factories.nodesets.SimpleNode currentChild = (hu.sztaki.ilab.giraffe.core.factories.nodesets.SimpleNode) node.jjtGetChild(i);
                    if ("Minus".equals(currentChild.toString())) {
                        addSet = false;
                        continue;
                    }
                    if ("Plus".equals(currentChild.toString())) {
                        addSet = true;
                        continue;
                    }
                    java.util.Set<ProcessingNetworkGenerator.ProcessingNetworkNode> childSet = walk0(currentChild);
                    if (childSet == null) {
                        logger.error("Child set could not be evaluated.");
                        return null;
                    }
                    if (addSet) {
                        nodeSet.addAll(childSet);
                    } else {
                        nodeSet.removeAll(childSet);
                    }
                }
                return nodeSet;
            }
            // If node type is "Start" or "T" then only a single child is possible, so we just recursively
            // call walk0.
            if (node.jjtGetNumChildren() != 1) {
                logger.error("Expecting a single child from node of type " + node.toString());
                return null;
            }
            return walk0((hu.sztaki.ilab.giraffe.core.factories.nodesets.SimpleNode) node.jjtGetChild(0));
        }
    }

    static java.util.Set<ProcessingNetworkGenerator.ProcessingNetworkNode> parseNodeSetExpression(
            String expression,
            java.util.Map<String, ProcessingNetworkGenerator.ProcessingNetworkNode> nodes) {
        nodeSetExpressionParser.ReInit(new java.io.StringReader(expression));
        try {
            hu.sztaki.ilab.giraffe.core.factories.nodesets.SimpleNode root = nodeSetExpressionParser.Start();
            NSETreeWalker tw = new NSETreeWalker(root, nodes);
            return tw.walk();
        } catch (hu.sztaki.ilab.giraffe.core.factories.nodesets.ParseException ex) {
            logger.error("Failed to parse expression, '" + expression + "'.", ex);
            return null;
        }
    }

    /*
    public static void printTree(hu.sztaki.ilab.giraffe.core.factories.nodesets.SimpleNode n) {
    System.out.println(n.jjtGetValue() + "(" + n.toString() + ")");
    for (int i = 0; i < n.jjtGetNumChildren(); i++) {
    printTree((hu.sztaki.ilab.giraffe.core.factories.nodesets.SimpleNode) n.jjtGetChild(i));
    }
    }

    public static void main(String[] args) {
    String str = "@nodes - (someField + anotherField) - (errorRecord: + errorRecord:reason)";
    System.out.println("Input string: " + str);
    try {
    nodeSetExpressionParser.ReInit(new java.io.StringReader(str));
    hu.sztaki.ilab.giraffe.core.factories.nodesets.SimpleNode n = nodeSetExpressionParser.Start();
    printTree(n);
    n.dump("");
    } catch (hu.sztaki.ilab.giraffe.core.factories.nodesets.ParseException ex) {
    ex.printStackTrace();
    }
    }
     */
    public static void printTree(hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode n) {
        if (n.jjtGetValue() != null) {
            System.out.println(n.jjtGetValue() + "(" + n.toString() + ")");
        }
        for (int i = 0; i < n.jjtGetNumChildren(); i++) {
            printTree((hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode) n.jjtGetChild(i));
        }
    }

    public static void main(String[] args) {
        String[] exp = new String[]{/*
            "fieldA = fieldB",
            "fieldA = created:fieldB",
            "fieldA = java.lang.Integer {Integer.parseInt(createdFields.fieldB)}",
            "* = received:",
            "* - fieldA = received:",
            "(* - fieldA) = received:",
            "* - (fieldA + fieldB) = received:, raw = error:raw",
            "* = received:, * = created:",
            "* = error:",
             */
            "received = java.lang.Object[] {receivedFields.getRaw()}, created = java.lang.Object[] {createdFields.getRaw()}"
        };
        for (String str : exp) {
            System.out.println("Input string: " + str);
            recordMappingParser.ReInit(new java.io.StringReader(str));
            try {
                hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.SimpleNode n = recordMappingParser.Start();
                printTree(n);
                n.dump("");
            } catch (hu.sztaki.ilab.giraffe.core.factories.mappingexpressions.ParseException ex) {
                ex.printStackTrace();
            }
        }
    }
}
