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

/**
 * ProcessMonitor is an interface for monitoring the ETL process in giraffe 2.
 * It is called from within the processing network. IMPORTANT: ProcessMonitor
 * must be thread-safe: it may be called from several threads simulaneously.
 *
 * @author neumark
 */
public interface ProcessMonitor {
    public void onNodeEntry(String nodeId, int recordsProcessed);
    public void onRouteEntry(String sourceNodeId, String destNodeId, int recordsProcessed);
    public void onError(hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessingElementBaseClasses.Record errorRecord);    
    public void customEvent(Class source, String eventIdentifier, Object details);
    public void onProcessStart();
    public void onInputFinished();
    public void onProcessFinish();
}
