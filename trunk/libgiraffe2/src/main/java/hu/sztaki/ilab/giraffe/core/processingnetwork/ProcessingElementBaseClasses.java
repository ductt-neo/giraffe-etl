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

import hu.sztaki.ilab.giraffe.core.Globals;
import hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class contains class and interfaces which are used by the generated code
 * created by ProcessingNetworkGenerator.
 * @author neumark
 */
public class ProcessingElementBaseClasses {

    // defaultEvents is the "everything is OK" event set. When something goes wrong,
    // a new set is created, which contains updated state of the node.
    protected final static java.util.Set<hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType> defaultEvents =
            java.util.Collections.unmodifiableSet(
            new java.util.HashSet<hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType>(java.util.Arrays.asList(
            new hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType[]{hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType.META_OK})));
    protected static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(DataSource.class);

    public static java.util.Set<hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType> addEvent(java.util.Set<hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType>evSet , hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType ev) {
        if (evSet == defaultEvents || evSet == null) {
            evSet = new java.util.HashSet<hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType>();
            evSet.add(ev);
        }
        else evSet.add(ev);
        return evSet;
    }

    // Processing networks must conform to this interface.
    public interface ProcessingNetwork /*extends Stoppable*/ {

        public void start(); // Initialize threads, start the network.

        public void dataSourcesRequestStop();
    }

    public interface Record {
        // The getRaw function returns the fields of the record as an array of Objects.

        public String[] getFieldNames();

        public String[] getFieldTypes();

        public Object getField(int columnNumber);

        public Object[] getFields();

        public void setFields(Object[] o);
    }

    public interface QueueWriter {

        public boolean append(Object[] row) throws InterruptedException;
    }

    public abstract static class ProcessingNetworkNode {

        protected java.util.Set<hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType> events = defaultEvents;
        protected int recordsRead = 0;
    }

    public abstract static class InnerNode extends ProcessingNetworkNode {
        // receive0 is called by the dynamically generated receive() function.

        protected void receive0() {
            try {
                this.work();
            } catch (java.lang.Throwable ex) {
                this.events = ProcessingElementBaseClasses.addEvent(this.events, EventType.ERROR_TASK_EXCEPTION);
                updateErrorRecord(ex);
            } finally {
                send();
            }
        }

        protected abstract void work() throws java.lang.Throwable;

        protected abstract void updateErrorRecord(java.lang.Throwable ex);

        protected abstract void send();
    }

    // DataSource is an abstract base class for all data source objects.
    // There are two main types of data sources: those which run in a separate thread, and those which do not
    // (the later covering embedded processing networks, living within a processingnode of another network).
    public abstract static class DataSource<ReceivedRecordType extends Record, CreatedRecordType extends Record> extends ProcessingNetworkNode {

        protected int timeout = Globals.queueWaitTimeoutSeconds;

        // addRecord is the function called from outside the network to add a new record to this data source.
        public abstract boolean addRecord(ReceivedRecordType record);

        protected abstract void send();
    }

    public abstract static class ThreadedDataSource<ReceivedRecordType extends Record, CreatedRecordType extends Record> extends DataSource<ReceivedRecordType, CreatedRecordType> implements Runnable, Stoppable, QueueWriter {

        protected AtomicBoolean keepRunning = new AtomicBoolean(true);
        protected java.util.concurrent.CountDownLatch latch = null;
        protected java.util.concurrent.BlockingQueue<ReceivedRecordType> queue = new java.util.concurrent.ArrayBlockingQueue<ReceivedRecordType>(hu.sztaki.ilab.giraffe.core.Globals.queueSize);
        protected ReceivedRecordType receivedRecord;
        protected CreatedRecordType createdRecord;

        public void requestStop() {
            keepRunning.set(false);
        }

        public ThreadedDataSource(java.util.concurrent.CountDownLatch latch) {
            this.latch = latch;
        }

        public boolean addRecord(ReceivedRecordType record) {
            return queue.add(record);
        }

        public void run() {
            while (keepRunning.get()) {
                try {
                    // Wait $timeout for next record to arrive if none is available.
                    if (null != (receivedRecord = queue.poll(timeout, TimeUnit.SECONDS))) {
                        // Note that no data source (not even ThreadedDataSource) has to be
                        // thread-safe, as each data source runs in its own thread, so
                        // the run() function will never run concurrently.
                        events = defaultEvents;
                        createdRecord = importConversion(receivedRecord);
                        send();
                        this.recordsRead++;
                    }
                } catch (Exception ex) {
                    logger.error("Unhandled exception occured: ", ex);
                }
            }
            this.latch.countDown();
        }

        // Performs the input -> output record format conversion, If the formats are the same, then it simply returns the input.
        public abstract CreatedRecordType importConversion(ReceivedRecordType record);
    }

    // VirtualDataSource does not need to worry about thread safetly, because
    // addRecord() will only be called when a new record reaches a processing node.
    public abstract static class VirtualDataSource<RecordType extends Record> extends DataSource<RecordType, RecordType> {

        protected RecordType record;

        public boolean addRecord(RecordType record) {
            this.record = record;
            send();
            return true;
        }
    }

    // DataSink is an abstract base class for all data sinks.
    // Virtual data sinks don't have to worry about thread safetly issues, since they simply
    // pass the incoming record along to the next processing node.
    // "Real" (threaded) data sinks *must* be thread safe!
    public abstract static class DataSink<ReceivedRecordType extends Record, CreatedRecordType extends Record> extends ProcessingNetworkNode {

        // receive() is called from within the processing network; it works like a regular node's receive function.
        // It also performs the task of relaying the exported record to the exporter, so no send() is necessary.
        public abstract void receive(ReceivedRecordType rec);
        // send() is not used by data sinks.
    }

    // This is the baseclass for "real" data sinks, that is, data sinks which are connected to
    // an exporter of some sort which runs in a separate thread. Data sinks themselves do not
    // have a thread of their own, in this regard, they are line the inner nodes of a processing network.
    // Although it is ill-advised for "regular" processing network nodes to receive rows
    // from nodes in several threads, in the case of threaded data sinks, this may be desireable in certain cases.
    // Because of this, receive() must be re-entrant.
    // This means queue is the only class member field.
    // If the conversion is successful, then the converted record is placed into the queue where it can
    // be read by the appropriate RecordExporter.
    // If the conversion fails, then the exception is caught and placed inside and errorRecord, which
    // is passed to sendOnError. The default implementation of this function simply logs the error.
    // To pass the error
    public abstract static class ThreadedDataSink<ReceivedRecordType extends Record, CreatedRecordType extends Record> extends DataSink<ReceivedRecordType, CreatedRecordType> {

        // outputQ is the queue of exported records, which is read by an exporter running in a separate thread.
        protected java.util.concurrent.BlockingQueue<CreatedRecordType> queue = new java.util.concurrent.ArrayBlockingQueue<CreatedRecordType>(hu.sztaki.ilab.giraffe.core.Globals.queueSize);

        public ThreadedDataSink() {
        }

        // Override this to route errorRecord to another data sink.
        protected void sendOnError(Record errorRecord) {
            String str = "";
            if (errorRecord != null) {
                for (Object o : errorRecord.getFields()) {
                    if (o != null) {
                        str += o.toString() + " ";
                    }
                }
            }
            logger.error("ThreadedDataSink: export conversion failed! details: " + str);
        }

        // To handle export conversion errors by sending them to an error sink,
        // simply override receive().
        public void receive(ReceivedRecordType rec) {
            CreatedRecordType exportedRecord = null;
            try {
                exportedRecord = exportConversion(rec);
                try {
                    queue.put(exportedRecord);
                } catch (InterruptedException ex) {
                    ; /* Not really sure what to do with this. */
                }
            } catch (Throwable ex) {
                java.util.Set<hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType> ev = ProcessingElementBaseClasses.addEvent(null, EventType.ERROR_CONVERSION_FAILED);
                sendOnError(updateErrorRecord(ex, rec, ev));
            }
        }

        public abstract CreatedRecordType exportConversion(ReceivedRecordType record) throws java.lang.Throwable;

        protected abstract Record updateErrorRecord(java.lang.Throwable ex, ReceivedRecordType received, java.util.Set<hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType> events);
    }

    // Virtual data sinks are useful if a processing network is embedded within a processing node.
    // No conversion is necessary.
    public abstract static class VirtualDataSink<RecordType extends Record> extends DataSink<RecordType, RecordType> {
    }
}
