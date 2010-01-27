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

import hu.sztaki.ilab.giraffe.core.database.*;
//import hu.sztaki.ilab.giraffe.core.io.CSVWriter;
import hu.sztaki.ilab.giraffe.core.util.Pair;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.Date;

/**
 * A ProcessingEngine is an abstract class which acts as a base class for all classes which process
 * input data. Each ProcessingEngine has a separate thread, but may create additional threads if
 * needed (for example, HostNameResolver does).
 * Raw data which needs to be processed is sent to the ProcessingEngine through the process() function.
 * This is usually called by the DataProcessor assigned to the column. The process() function adds the
 * task to the list of queued column values and returns immediately.
 * The run() function of the ProcessingEngine then reads the queued values and processes them, writing
 * the resulting information to the associated LookupTable (currently a Berkeley DB table).
 * @author neumark
 */
public abstract class ProcessingEngine {

    /**
     * Already processed values are recorded into this BDBLookupTable
     */
    protected final LookupTable processedValues;
    static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ProcessingEngine.class);
    protected String resourceDir = null;
    protected BlockingQueue<String> taskQueue = new java.util.concurrent.LinkedBlockingQueue<String>();
    protected int threadPoolSize = 1;
    // The reference on which to synchronize must be final
    // which is the reason I put this value into a Vector.
    private java.util.concurrent.atomic.AtomicInteger numQueuedTasks = new java.util.concurrent.atomic.AtomicInteger();
    private java.util.Vector<ProcessingRunnable> threadPool;
    // TODO(neumark) make polling timeout configurable.
    protected int timeout = 1000;

    public ProcessingEngine(LookupTable table) {
        this.processedValues = table;
    }

    public ProcessingEngine(LookupTable table, int threadPoolSize) {
        this.processedValues = table;
        this.threadPoolSize = threadPoolSize;
    }

    public void setResourceDir(String resourceDir) {
        this.resourceDir = resourceDir;
    }

    public void setParameters(org.w3c.dom.Element parameters) { /* does absolutely nothing by default. */

    }

    public void process(String input) {
        synchronized (processedValues) {
            Record pd = processedValues.get(input);
            if (pd != null && pd.getState() == Record.State.EMPTY) {
                // this data is currently being processed, do not disturb!
                return;
            }
            if (pd == null || !isValid(pd)) {
                // the data is missing or no longer valid: calculate!
                // make sure no other thread tries to process this data at the same time: write an EMPTY ProcessedData to the BDBLookupTable.
                processedValues.put(input, null);
                taskQueue.add(input);
                numQueuedTasks.getAndIncrement();
            }
        }
    }

    /**
     * Checks whether a ProcessedData object is valid. The default implementation only checks if Status == OK. Override if you need something
     * more sophisticated.
     * @param pd
     * @return <code>true</code> if the data is valid.
     */
    public boolean isValid(Record pd) {
        // check the state of the data
        return (pd.getState() == Record.State.OK);
    }

    /**
     * init is called before the ProcessingEngine can be used. Put all setup code here, not in the constructor!
     */
    public boolean init() {
        return createThreadPool(this.threadPoolSize);
    }

    // Override this function if multiple threads are used and the processing is not thread-safe.
    protected abstract ProcessingRunnable getRunnable();

    public interface Stopable {

        void requestStop();

        boolean isInterruptible();
    }

    protected abstract class ProcessingRunnable extends Thread implements Runnable, Stopable {

        public ProcessingRunnable() {
        }
        private Boolean stopRequested = false;
        private Boolean isInterruptible = true;

        public boolean isInterruptible() {
            synchronized (isInterruptible) {
                return isInterruptible;
            }
        }

        protected abstract void performTask(String currentTask);

        public void run() {
            logger.debug("Processing engine thread started.");
            String currentTask = null;
            // Keep running until they ask us to stop.
            while (!stopRequested) {
                try {
                    if (null != (currentTask = taskQueue.poll(timeout, TimeUnit.SECONDS))) {
                        isInterruptible = false;
                        performTask(currentTask);
                        numQueuedTasks.getAndDecrement();
                        isInterruptible = true;
                    }
                } catch (InterruptedException ex) {
                    logger.debug("Processing thread interrupted in waiting on task.");
                }
            }
            logger.debug("Processing engine thread stopped.");
        }

        public void requestStop() {
            synchronized (stopRequested) {
                stopRequested = true;
                logger.debug("Processing engine thread requested to stop.");
            }
        }
    }

    protected boolean createThreadPool(int numThreads) {
        threadPool = new java.util.Vector<ProcessingRunnable>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            ProcessingRunnable currentThread = getRunnable();
            if (null == currentThread) {
                logger.error("Received null for member of thread pool.");
                return false;
            }
            threadPool.add(currentThread);
            currentThread.start();
        }
        return true;
    }

    public void join() throws InterruptedException {
        for (ProcessingRunnable currentThread : threadPool) {
            currentThread.join();
        }
    }

    public void writeResult(String rawData, Record processedData) {
        synchronized (processedValues) {
            //if (null == processedData.getState()) {processedData.setState(ProcessedData.State.OK);}
            processedValues.put(rawData, processedData);
        }
    }

    public void requestStop() {
        for (ProcessingRunnable currentThread : threadPool) {
            currentThread.requestStop();
            if (currentThread.isInterruptible) {
                logger.debug("Interrupting thread " + currentThread.getName() + " because it is polling the task list.");
                currentThread.interrupt();
            }
        }
    }

    public int getNumQueuedTasks() {
        return numQueuedTasks.get();
    }

    // Override this method where an external datasource is used.
    public java.util.Calendar getDataSourceLastModification() {
        return null;
    }

    // Override this method if the processing engine sends output to meta.xml
    public org.w3c.dom.Element getOutput() {
        return null;
    }

    // Until I can think of something better:
    public abstract java.util.List<String> getFieldList();

// Copied from: https://www.cs.auckland.ac.nz/references/java/java1.5/tutorial/jmx/mbeans/mxbeans.html
    public class PEQueueMXBean implements hu.sztaki.ilab.giraffe.core.management.ProcessingEngineTaskQueueMXBean {


        private ProcessingEngine pe = ProcessingEngine.this;

        public PEQueueMXBean() {
        }

        public hu.sztaki.ilab.giraffe.core.management.ProcessingEngineTaskQueueMXBean.QueueSample getQueueSample() {
            return new hu.sztaki.ilab.giraffe.core.management.ProcessingEngineTaskQueueMXBean.QueueSample(new Date(), pe.getNumQueuedTasks(), pe.taskQueue.peek());
        }
    }

    public java.util.Map<String,Object> getMBeans() {
        java.util.Map<String,Object> mbeanMap = new java.util.HashMap<String,Object>();
        mbeanMap.put("QueueMonitor", new PEQueueMXBean());
        return mbeanMap;
    }
}
