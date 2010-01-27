/*
 *  Copyright 2010 neumark.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */
package hu.sztaki.ilab.giraffe.core.processingnetwork;

import hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessMonitor;

/**
 *
 * @author neumark
 */
public class ProcessMonitorBase implements ProcessMonitor {

    private final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ProcessMonitorBase.class);

    public void onNodeEntry(String nodeId, int recordsProcessed) {
        logger.info("Node entry (" + nodeId + ") " + recordsProcessed + " record processed.");
    }

    public void onRouteEntry(String routeId, int recordsProcessed) {
        logger.info("Route entry (" + routeId + ") " + recordsProcessed + " record processed.");
    }

    public void onError(hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessingElementBaseClasses.Record errorRecord) {
        String str = "";
        for (Object o : errorRecord.getFields()) {
            str += o.toString() + " ";
        }
        logger.error("Error: " + str);
    }

    public void customEvent(Class source, String eventIdentifier, Object details) {
        logger.info("Custom event '"+eventIdentifier+"' from "+source.getCanonicalName()+" details: "+details.toString());
    }

    public void onProcessStart() {
        logger.info("ETL process started.");
    }

    public void onInputFinished() {
        logger.info("ETL process finished reading input.");
    }

    public void onProcessFinish() {
        logger.info("ETL process finished.");
    }
}
