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

import hu.sztaki.ilab.giraffe.core.io.FileReader;
import hu.sztaki.ilab.giraffe.core.io.FileWriter;
import hu.sztaki.ilab.giraffe.core.io.RecordExporter;
import hu.sztaki.ilab.giraffe.core.io.RecordImporter;
import hu.sztaki.ilab.giraffe.core.io.streams.StandardExporter;
import hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessMonitor;
import hu.sztaki.ilab.giraffe.core.util.Pair;
import hu.sztaki.ilab.giraffe.core.xml.ConfigurationManager;

/**
 * Instantiates and initializes input and output terminals.
 * @author neumark
 */
public class TerminalFactory {

    ConfigurationManager configurationManager;
    final java.util.concurrent.CountDownLatch inputLatch;
    final java.util.concurrent.CountDownLatch outputLatch;
    private final ProcessMonitor processMonitor;
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(TerminalFactory.class);

    public TerminalFactory(ConfigurationManager configurationManager, int numInputTerminals, int numOutputTerminals, ProcessMonitor processMonitor) {
        this.configurationManager = configurationManager;
        this.inputLatch = new java.util.concurrent.CountDownLatch(numInputTerminals);
        this.outputLatch = new java.util.concurrent.CountDownLatch(numOutputTerminals);
        this.processMonitor = processMonitor;
    }

    public static Pair<String, String> quoteConverter(hu.sztaki.ilab.giraffe.schema.dataformat.Quotes q) {
        Pair<String, String> qpair = new Pair<String, String>(q.getQuotes(), q.getQuotes());
        if ((qpair.second == null || qpair.second.length() < 1) && (qpair.first == null || qpair.second.length() < 1)) {
            qpair = new Pair<String, String>(q.getStart(), q.getEnd());
        }
        return qpair;
    }

    private java.util.Queue<java.io.File> getInputFiles() {
        java.util.Queue<java.io.File> fileQ = new java.util.LinkedList<java.io.File>();
        for (String fn : configurationManager.getInputFiles()) {
            fileQ.add(new java.io.File(fn));
        }
        return fileQ;
    }

    public RecordImporter getInputTerminal(hu.sztaki.ilab.giraffe.schema.process_definitions.Process.Terminals.Input terminalDesc) {        
        // the input comes from files (no other stream type is defined right now).
        if (terminalDesc.getDatasource().getFormat().getStreamFormat() != null) {
            try {
                hu.sztaki.ilab.giraffe.core.io.streams.LineImporter imp = createLineImporter(terminalDesc.getDatasource().getFormat().getStreamFormat());
                if (imp == null) return null;
                return new FileReader(processMonitor, inputLatch, imp, terminalDesc.getDatasource().getFormat().getStreamFormat().getEncoding(), getInputFiles());
            } catch (Exception ex) {
                logger.error("Error creating input terminal " + terminalDesc.getName() + "!", ex);
                return null;
            }
        }
        return null;
    }

    private hu.sztaki.ilab.giraffe.core.io.streams.LineExporter createLineExporter(hu.sztaki.ilab.giraffe.schema.dataformat.StreamFormat streamFormat) {
        return new StandardExporter(streamFormat);
    }

    public RecordExporter getOutputTerminal(hu.sztaki.ilab.giraffe.schema.process_definitions.Process.Terminals.Output terminalDesc) {        
        if (terminalDesc.getDatasink().getFormat().getStreamFormat() != null) {
            try {
                hu.sztaki.ilab.giraffe.core.io.streams.LineExporter exp = createLineExporter(terminalDesc.getDatasink().getFormat().getStreamFormat());
                if (exp == null) return null;
                FileWriter fw = new FileWriter(
                        processMonitor,
                        outputLatch,
                        exp,
                        terminalDesc.getDatasink().getFormat().getStreamFormat().getEncoding(),
                        terminalDesc.getDatasink().getFormat().getStreamFormat().getNewline(),
                        getOutputFile(terminalDesc.getName()));
                if (!fw.init()) {
                    logger.error("Error initializing file writer for output terminal "+terminalDesc.getName());
                    return null;
                }
                return fw;
            } catch (Exception ex) {
                logger.error("Error creating input terminal " + terminalDesc.getName() + "!", ex);
                return null;
            }
        }
        return null;
    }

    private java.io.File getOutputFile(String name) {
        return new java.io.File(name);
    }

    private hu.sztaki.ilab.giraffe.core.io.streams.LineImporter createLineImporter(
            hu.sztaki.ilab.giraffe.schema.dataformat.StreamFormat importerSpec) throws java.lang.ClassNotFoundException, java.lang.InstantiationException, java.lang.IllegalAccessException {
        /*
        if (null != importerSpec.getSplitImporter()) {
        return new hu.sztaki.ilab.giraffe.core.io.streams.SplitImporter(importerSpec);
        } */

        hu.sztaki.ilab.giraffe.core.io.streams.TokenizerImporter importer = new hu.sztaki.ilab.giraffe.core.io.streams.TokenizerImporter(importerSpec);
        if (!importer.init()) {
            logger.error("Error initializing importer!");
            return null;
        }
        return importer;
    }
}
