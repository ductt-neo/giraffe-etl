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

import hu.sztaki.ilab.giraffe.core.conversion.ConversionManager;
import hu.sztaki.ilab.giraffe.core.factories.ObjectInstantiator;
import hu.sztaki.ilab.giraffe.core.io.RecordExporter;
import hu.sztaki.ilab.giraffe.core.io.RecordImporter;
import hu.sztaki.ilab.giraffe.core.xml.ConfigurationManager;

/**
 * ProcessingJob keeps track of all information relating to the job. It also has a bunch of methods that open/close files and Berkely DB's during intialization and teardown.
 * @author neumark
 */
public class Process {

    // The following must be public, because ProcessFactory needs to modify it.
    public static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(Process.class);
    public java.util.concurrent.CountDownLatch recordImporterThreadsLatch;
    public java.util.concurrent.CountDownLatch dataSourcesThreadsLatch;
    public java.util.concurrent.CountDownLatch recordExporterThreadsLatch;
    public java.util.Map<String, RecordImporter> inputs = new java.util.HashMap<String, RecordImporter>();
    public java.util.Map<String, RecordExporter> outputs = new java.util.HashMap<String, RecordExporter>();
    public java.util.Map<String, ObjectInstantiator> taskInstantiators = new java.util.HashMap<String, ObjectInstantiator>();
    public java.util.Map<String, ObjectInstantiator> eventInstantiators = new java.util.HashMap<String, ObjectInstantiator>();
    public java.util.Map<String, ObjectInstantiator> conversionInstantiators = new java.util.HashMap<String, ObjectInstantiator>();
    public ProcessMonitor monitor;
    public ProcessingElementBaseClasses.ProcessingNetwork network = null;
    public ConfigurationManager configurationManager;
    public ConversionManager conversionManager;
    public java.util.List<Thread> threads = new java.util.LinkedList<Thread>();

    // Responsible for reacting to process state changes.
    public class ProcessListener implements Runnable {

        public ProcessListener() {
            logger.debug("Importer threads: "+Process.this.recordImporterThreadsLatch.getCount());
            logger.debug("Exporter threads: "+Process.this.recordExporterThreadsLatch.getCount());
            logger.debug("DataSource threads: "+Process.this.dataSourcesThreadsLatch.getCount());
        }

        private void recordExportersRequestStop() {
            for (RecordExporter exp: Process.this.outputs.values()) {
                exp.requestStop();
            }
        }

        public void run() {
            if (monitor != null) {
                monitor.onProcessStart();
            }
            try {
                Process.this.recordImporterThreadsLatch.await();
            } catch (InterruptedException ex) {
                logger.debug("Process interrupted.", ex);
            }
            // Once the process has finished running, notify the monitor.
            if (monitor != null) {
                monitor.onInputFinished();
            }
            // If reading the input has finished, then we may ask the data source threads to stop.
            Process.this.network.dataSourcesRequestStop();
                    // modras: Data source threads won't stop until they process all their queues.

            // Now wait for the data source threads to finish their work.
            try {
                Process.this.dataSourcesThreadsLatch.await();
                recordExportersRequestStop();
                    // modras: Record exporter threads won't stop until they process all their queues.
                Process.this.recordExporterThreadsLatch.await();
            } catch (InterruptedException ex) {
                logger.debug("Process interrupted.", ex);
            }
            // Once the record exporters have finished as well, notify the monitor.
            if (monitor != null) {
                monitor.onProcessFinish();
            }
            // TODO: we'll want to take this out later.
            System.exit(0);
        }
    }

    public Process() {
    }

    public boolean start() {
        Thread teardownThread = new Thread(new ProcessListener());
        teardownThread.start();
        network.start();
        return true;
    }
}
