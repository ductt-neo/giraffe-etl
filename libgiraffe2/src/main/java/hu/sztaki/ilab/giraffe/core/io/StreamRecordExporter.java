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
import hu.sztaki.ilab.giraffe.core.io.streams.LineExporter;
import hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessingElementBaseClasses;
import java.io.IOException;

/**
 *
 * @author neumark
 */
public class StreamRecordExporter extends RecordExporter{
    
    private LineExporter exporter;
    protected java.io.BufferedWriter stream;
    int linesWritten = 0;
    String newline;

    public StreamRecordExporter(java.util.concurrent.CountDownLatch latch, LineExporter exporter, String newline) {
        super(latch);
        this.exporter = exporter;
        this.newline = newline;
    }

    public String getFieldType(java.lang.String _) {return "java.lang.String";}

    public int getLinesWritten() {
        return linesWritten;
    }

    public java.io.BufferedWriter getStream() {
        return stream;
    }

    public ProcessingNetworkGenerator.RecordDefinition getRecordFormat() {
        return new ProcessingNetworkGenerator.RecordDefinition("java.lang.String", exporter.getColumns());
    }

    public void setStream(java.io.BufferedWriter stream) {
        linesWritten = 0;
        this.stream = stream;
    }

    public void writeRecord(Object[] record) {
        String exportedRecord = exporter.export(record);
        try {
        stream.write(exportedRecord);
        // TODO(neumark): newline character should be read from XML config!
        stream.write(newline);
        } catch (java.io.IOException ex) {
            logger.error("IO exception occured on write.",ex);
        }
        linesWritten++;
    }
}
