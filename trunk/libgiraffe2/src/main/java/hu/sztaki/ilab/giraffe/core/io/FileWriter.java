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

import hu.sztaki.ilab.giraffe.core.io.streams.LineExporter;
import hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessMonitor;

/**
 *
 * @author neumark
 */
public class FileWriter extends StreamRecordExporter {

    java.io.File outputFile;
    String encoding;
    ProcessMonitor processMonitor;

    public FileWriter(
            ProcessMonitor processMonitor,
            java.util.concurrent.CountDownLatch latch,
            LineExporter exporter,
            String encoding,
            String newline,
            java.io.File outputFile) {
        super(latch, exporter, newline);
        this.processMonitor = processMonitor;
        this.encoding = encoding;
        this.outputFile = outputFile;
    }

    public boolean init() {
        try {
            super.stream = new java.io.BufferedWriter(
                    new java.io.OutputStreamWriter(
                    new java.io.FileOutputStream(outputFile), encoding));
        } catch (Exception ex) {
            logger.error("Error opening file" + outputFile.getAbsolutePath());
            return false;
        }
        return true;
    }
}
