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
package hu.sztaki.ilab.giraffe.core.processingnetwork;

import hu.sztaki.ilab.giraffe.core.database.NumberedKeyset;
import hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessingEngine;
import hu.sztaki.ilab.giraffe.core.transformers.Transformer;
import hu.sztaki.ilab.giraffe.core.events.EventPredicate;
import hu.sztaki.ilab.giraffe.core.io.streams.LineImporter;
import hu.sztaki.ilab.giraffe.core.util.Pair;
import org.apache.log4j.Logger;

/**
 *
 * @author neumark
 */
public abstract class ProcessingLoop {
/*
    protected static Logger logger = Logger.getLogger(ProcessingLoop.class);
    //private hu.sztaki.ilab.giraffe.core.io.CSVWriter factWriter;
    private java.io.Writer noEventDiscards;
    private java.io.Writer unparseableDiscards;
    // This array must be initialized be descendant classes!
    private java.util.Vector<java.util.Map<String, java.util.Set<ProcessingEngine>>> applicableProcessingEngines;
    private java.util.Set<ProcessingEngine> processingEngines = new java.util.HashSet<ProcessingEngine>();
    private java.util.Vector<java.util.List<Transformer>> transformers;
    private java.util.Vector<NumberedKeyset> keysets;
    //private java.util.List<StringPreProcessor> stringPreprocessors;
    //private java.util.List<RecordPreProcessor> recordPreprocessors;
    protected java.util.Vector<EventPredicate> eventPredicates;

    public ProcessingLoop(
            hu.sztaki.ilab.giraffe.core.io.CSVWriter output,
            java.io.Writer noEventDiscards,
            java.io.Writer unparseableDiscards,
            java.util.Vector<java.util.List<Transformer>> transformers,
            java.util.Vector<EventPredicate> eventPredicates,
            java.util.Vector<NumberedKeyset> keysets,
            java.util.Vector<java.util.Map<String, java.util.Set<ProcessingEngine>>> applicableProcessingEngines,
            java.util.List<StringPreProcessor> stringPreprocessors,
            java.util.List<RecordPreProcessor> recordPreprocessors) {
        this.factWriter = output;
        this.noEventDiscards = noEventDiscards;
        this.unparseableDiscards = unparseableDiscards;
        this.transformers = transformers;
        this.eventPredicates = eventPredicates;
        this.keysets = keysets;
        this.applicableProcessingEngines = applicableProcessingEngines;
        this.stringPreprocessors = stringPreprocessors;
        this.recordPreprocessors = recordPreprocessors;
        // populate the processingEngines hashmap:
        for (int i = 0; i < applicableProcessingEngines.size(); i++) {
            if (null != applicableProcessingEngines.get(i)) {
                for (java.util.Set<ProcessingEngine> peSet : applicableProcessingEngines.get(i).values()) {
                    for (ProcessingEngine pe : peSet) {
                        processingEngines.add(pe);
                    }
                }
            }
        }
    }

    protected abstract java.util.List<String> getEvents(java.util.List<String> s);

    protected java.util.List<String> transformRecord(java.util.List<String> rawRecord) {
        int columnIndex = 0;
        java.util.List<String> transformedRecord = new java.util.LinkedList<String>();
        for (String field : rawRecord) {
            String transformedField = field;
            if (null != transformers.get(columnIndex)) for (Transformer transformer : transformers.get(columnIndex)) {
                transformedField = transformer.transform(transformedField);
            }
            transformedRecord.add(transformedField);
            columnIndex++;
        }
        return transformedRecord;
    }

    // apply processing engine returns a key which refers to the given value of the given column.
    protected int applyProcessingEngines(int columnIndex, String fieldValue, java.util.List<String> eventNames) {
        // Events are functions of every field in the record.
        // Therefore, the fact that a certain value has already benn encountered
        // for a column does not mean it musn't be submitted to the processing engines.
        Pair<Integer, Boolean> id = keysets.get(columnIndex).getID(fieldValue);
        java.util.Set<ProcessingEngine> peSet = new java.util.HashSet<ProcessingEngine>();
        for (String event : eventNames) {
            if (applicableProcessingEngines.get(columnIndex).containsKey(event)) peSet.addAll(applicableProcessingEngines.get(columnIndex).get(event));
        }
        for (ProcessingEngine pe : peSet) {
            pe.process(fieldValue);
        }
        return id.first.intValue();
    }

    public int processLines(java.io.Reader inputReader, LineImporter importer) {
        java.io.BufferedReader input = new java.io.BufferedReader(inputReader);
        String rawLine = null;
        String preprocessedLine = null;
        java.util.List<String> rawRecord = null;
        int lineNumber = 0;
        try {
            while (null != (rawLine = input.readLine())) {
                lineNumber++;
                // apply preprocessors (if any)
                preprocessedLine = rawLine;
                for (StringPreProcessor preprocessor : stringPreprocessors) {
                    preprocessedLine = preprocessor.preprocess(preprocessedLine);
                }
                try {
                    rawRecord = importer.parse(preprocessedLine);
                    if (rawRecord.size() != this.keysets.size()) {
                        throw new java.text.ParseException("Received "+rawRecord.size() +" records instead of the expected "+ this.keysets.size() + ".", lineNumber);
                    }
                } catch (java.text.ParseException ex) {
                    logger.debug("Error parsing input file. " + printErr(rawLine, preprocessedLine, lineNumber), ex);
                    if (null != unparseableDiscards) {
                        try {
                            unparseableDiscards.write(rawLine + "\n");
                        } catch (java.io.IOException ex2) {
                            logger.error("Error writing unparseable line to discard stream ", ex2);
                        }
                    }
                    continue;
                }
                java.util.List<String> preprocessedRecord = rawRecord;
                for (RecordPreProcessor preprocessor : recordPreprocessors) {
                    preprocessedRecord = preprocessor.preprocess(preprocessedRecord);
                }
                java.util.List<String> transformedRecord = transformRecord(preprocessedRecord);
                java.util.List<String> events = getEvents(transformedRecord);
                if (events.size() == 0) {
                    if (null != noEventDiscards) {
                        try {
                            noEventDiscards.write(rawLine + "\n");
                        } catch (java.io.IOException e) {
                            logger.error("Error writing record discarded to discard file. The record was discarded because no matching events were found.");
                        }
                    }
                    continue;
                }
                java.util.List<String> processedRecord = new java.util.LinkedList<String>();
                int columnIndex = 0;
                for (String transformedField : transformedRecord) {
                    if (null != applicableProcessingEngines.get(columnIndex)) {
                        processedRecord.add("" + applyProcessingEngines(columnIndex, transformedField, events));
                    } else {
                        processedRecord.add(transformedField);
                    }
                    columnIndex++;
                }
                for (String event : events) {
                    factWriter.write(processedRecord, event);
                }                
            }
            input.close();
        } catch (java.io.IOException ex) {
            logger.error("Error reading input log file. " + printErr(rawLine, preprocessedLine, lineNumber), ex);
            return -1;
        }
        return lineNumber;
    }

    private String printErr(String input, String preprocessedLine, int lineNumber) {
        return "Choked on line number " + lineNumber + ": raw line: '" + input + "'; preprocessed line: '" + preprocessedLine + "'.";
    }

    public boolean close() {
        // close EventPredicate objects
        // TODO(neumark) : feed the event predicate a real Element!
        for (EventPredicate pred : eventPredicates) {
            if (!pred.close(null)) {
                logger.error("Error(s) encountered closing event predicates.");
            }
        }
        try {
            if (null != this.noEventDiscards) this.noEventDiscards.close();
            if (null != this.unparseableDiscards) this.unparseableDiscards.close();
            // close CSV writer.
            factWriter.close();
        } catch (java.io.IOException ex) {
            logger.error("Error closing CSVWriter object.", ex);
            return false;
        }
        // Wait until all ProcessingEngines stop.

        // Request each processing engine to stop.
        for (ProcessingEngine pe : processingEngines) {
            pe.requestStop();
        }
        // Now we need to wait for the threads to stop.
        for (ProcessingEngine pe : processingEngines) {
            try {
                pe.join();
            } catch (InterruptedException intrEx) {
                logger.error("Caught InterruptedException whil waiting on processing engines.", intrEx);
            }
        }
        return true;
    }
 * */
}
