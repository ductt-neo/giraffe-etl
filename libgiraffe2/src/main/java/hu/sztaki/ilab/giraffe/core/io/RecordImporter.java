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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RecordImporter classes read records from a data source and present them as
 * an array of Objects.
 * @author neumark
 */
public abstract class RecordImporter implements Runnable, Stoppable {

    protected AtomicBoolean keepRunning = new AtomicBoolean(true);
    protected java.util.concurrent.CountDownLatch latch = null;    
    protected ProcessingElementBaseClasses.QueueWriter queueWriter = null;

    public RecordImporter(java.util.concurrent.CountDownLatch latch) {
        this.latch = latch;        
    }

    public abstract ProcessingNetworkGenerator.RecordDefinition getRecordFormat();
    
    public void run() {
        Object[] data = null;
        while (keepRunning.get() && (null != (data = getNextRecord()))) {
            try {
            this.queueWriter.append(data);
            } catch (InterruptedException ex) {throw new RuntimeException(ex); /* not really sure what to do with this. */}
        }
        close();
        latch.countDown();
    }

     public void requestStop() {
        keepRunning.set(false);        
    }

    public void setQueueWriter(ProcessingElementBaseClasses.QueueWriter qw) {
        this.queueWriter = qw;
    }

    public abstract Object[] getNextRecord();
    // Override this method to release resources.
    public void close() {}
    public abstract String getFieldType(String field);
}
