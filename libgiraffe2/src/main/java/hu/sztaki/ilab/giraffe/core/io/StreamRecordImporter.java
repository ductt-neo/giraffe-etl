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
import hu.sztaki.ilab.giraffe.core.io.streams.LineImporter;
import hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessingElementBaseClasses;
import java.util.Arrays;

/**
 *
 * @author neumark
 */
public class StreamRecordImporter extends RecordImporter {

    protected final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(StreamRecordImporter.class);
    protected LineImporter parser;
    protected java.io.BufferedReader stream;
    protected RecordExporter unparseableDiscardWriter;
    protected String encoding;
    protected int linesRead = 0;

    StreamRecordImporter(java.util.concurrent.CountDownLatch latch, LineImporter parser, String encoding) {
        super(latch);
        this.parser = parser;
        this.encoding = encoding;
    }
    public ProcessingNetworkGenerator.RecordDefinition getRecordFormat() {
        return new ProcessingNetworkGenerator.RecordDefinition("java.lang.String",parser.getColumns());
    }

    public String getFieldType(String _) {return "java.lang.String";}

    public Object[] getNextRecord() {
        try {
        java.util.List<String> record = read();
        if (record == null) return null;
        return read().toArray();
        } catch (java.io.IOException ex) {
            logger.error("IO exception occurred while reading input.", ex);
            return null;
        }
    }

    public void close() {
        try {
            stream.close();
        } catch (Exception ex) {
            logger.error("Error closing stream.", ex);
        }
    }

    public int getLinesRead() {
        return linesRead;
    }

    public RecordExporter getUnparseableDiscardWriter() {
        return unparseableDiscardWriter;
    }

    public void setUnparseableDiscardWriter(RecordExporter unparseableDiscardWriter) {
        this.unparseableDiscardWriter = unparseableDiscardWriter;
    }

    public java.io.BufferedReader getStream() {
        return stream;
    }

    public void setStream(java.io.BufferedReader stream) {
        linesRead = 0;
        this.stream = stream;
    }

    protected void discardLine(String line) throws java.io.IOException {
        unparseableDiscardWriter.writeRecord(new Object[]{"" + linesRead, "Parse Error", line});
    }

    public java.util.List<String> read() throws java.io.IOException {
        /* read() reads the next intelligible line or returns null if the EOF has been reached.
         */
        java.util.List<String> record = null;
        while (null == record) {
            String rawLine = stream.readLine();
            // end of stream reached.
            if (null == rawLine) {
                return null;
            }
            try {
                record = parser.parse(rawLine);
            } catch (java.text.ParseException ex) {
                discardLine(rawLine);
            }
            linesRead++;
        }
        return record;
    }
}
