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

import hu.sztaki.ilab.giraffe.core.io.streams.LineImporter;
import hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessMonitor;
import hu.sztaki.ilab.giraffe.core.util.Pair;
import java.io.File;

/**
 *
 * @author neumark
 */
public class FileReader extends StreamRecordImporter {

    java.util.Queue<File> inputFiles;
    ProcessMonitor processMonitor;

    public FileReader(ProcessMonitor processMonitor, java.util.concurrent.CountDownLatch latch, LineImporter li, String encoding, java.util.Queue<File> inputFiles) {
        super(latch, li, encoding);
        this.processMonitor = processMonitor;
        this.inputFiles = inputFiles;
    }

    private java.io.BufferedReader openNextFile() {
        File currentFile = inputFiles.poll();
        if (currentFile == null) {
            return null;
        }
        try {
            java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(new java.io.FileInputStream(currentFile), super.encoding));
            if (processMonitor != null) {
                processMonitor.customEvent(FileReader.class, "openNextFile", new Pair<String, Boolean>(currentFile.getAbsolutePath(), true));
            }
            return br;
        } catch (Exception ex) {
            StreamRecordImporter.logger.error("Error reading " + currentFile.getAbsolutePath() + ": moving on to next input file.", ex);
            if (processMonitor != null) {
                processMonitor.customEvent(FileReader.class, "openNextFile", new Pair<String, Boolean>(currentFile.getAbsolutePath(), false));
            }
            return openNextFile();
        }
    }

    public java.util.List<String> read() throws java.io.IOException {
        while (this.stream == null) {
            this.stream = openNextFile();
        }
        if (this.stream == null) {
            if (processMonitor != null) processMonitor.customEvent(FileReader.class, "read", "EndOfInput");
            return null; // no more files to read.            
        }
        java.util.List<String> line = null;
        line = super.read();
        if (line == null) {
            getStream().close();
            StreamRecordImporter.logger.debug("Current file contains no more lines. Moving on to next input file if any.");
            java.io.BufferedReader input = openNextFile();
            // If we are out of files to read, return null.
            if (null == input) {
                if (processMonitor != null) processMonitor.customEvent(FileReader.class, "read", "EndOfInput");
                return null;
            }
            super.setStream(input);
            // Call ourselves. This time we will return a line from the newly opened file.
            return read();
        }
        return line;
    }
}
