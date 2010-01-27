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
package hu.sztaki.ilab.giraffe.core.io;

import hu.sztaki.ilab.giraffe.core.factories.ProcessingNetworkGenerator;
import hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessingElementBaseClasses;
import hu.sztaki.ilab.giraffe.core.processingnetwork.Stoppable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RecordExporter classes write an exported
 * record to storage (either through jdbc or to a stream like a file).
 * RecordExporters run in a separate thread and communicate with the data sinks
 * in the processing network through a blocking queue.
 * @author neumark
 */
public abstract class RecordExporter implements Runnable, Stoppable {

    protected static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(RecordExporter.class);
    protected java.util.concurrent.BlockingQueue<? extends ProcessingElementBaseClasses.Record> queue;
    protected AtomicBoolean keepRunning = new AtomicBoolean(true);
    java.util.concurrent.CountDownLatch latch = null;
    int timeout = 5;

    public RecordExporter(java.util.concurrent.CountDownLatch latch) {
        this.latch = latch;        
    }

    public void run() {
        while (keepRunning.get()) {
            try {
                // Wait $timeout for next record to arrive if none is available.
                ProcessingElementBaseClasses.Record receivedRecord = null;
                if (null != (receivedRecord = queue.poll(timeout, TimeUnit.SECONDS))) {
                    writeRecord(receivedRecord.getFields());
                }
            } catch (Exception ex) {
                logger.debug("Unhandled exception occured: ", ex);
            }
        }
        close();
        latch.countDown();        
    }

    public void requestStop() {
        keepRunning.set(false);
    }

    public void setQueue(java.util.concurrent.BlockingQueue<? extends ProcessingElementBaseClasses.Record> queue) {this.queue = queue;}
    public java.util.concurrent.BlockingQueue<? extends ProcessingElementBaseClasses.Record> getQueue() {return queue;}
    protected abstract void writeRecord(Object[] record);

    public abstract String getFieldType(String field);

    public abstract ProcessingNetworkGenerator.RecordDefinition getRecordFormat();



    // Override close() to close any resources used by the RecordExporter.
    protected void close() {
    }
}
