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

package hu.sztaki.ilab.giraffe.dataprocessors.url;
import hu.sztaki.ilab.giraffe.dataprocessors.implementation.URLComponents;
import hu.sztaki.ilab.giraffe.core.database.LookupTable;
import org.apache.log4j.*;
import hu.sztaki.ilab.giraffe.core.dataprocessors.*;
import java.net.MalformedURLException;
import java.util.*;
/**
 *
 * @author neumark
 */
public class PlainURL extends ProcessingEngine {

    static Logger logger = Logger.getLogger(HttpGet.class);        
    public static java.util.List<String> fl = new java.util.LinkedList<String>();
    static
    {
       URLComponents.extendFieldList(fl);
    }
   
    public java.util.List<String> getFieldList() {return fl;}
    public PlainURL(LookupTable lookupTable) {super(lookupTable);}
    public PlainURL(LookupTable lookupTable, int threadCount) {super(lookupTable, threadCount);}

    protected class PlainURLRunner extends ProcessingEngine.ProcessingRunnable {
        public void performTask (String req) {
        ProcessedData ret = new ProcessedData();
        saveparts(ret,req);
        writeResult(req,ret);
        }
    }

    protected ProcessingEngine.ProcessingRunnable getRunnable() {
        return new PlainURLRunner();
    }

    private void saveparts(ProcessedData ret, String req)
    {
            String url = req;
            try {
            URLComponents c = new URLComponents(url);
            c.normalize();
            c.save(ret);
            } catch (java.net.MalformedURLException ex) {
                logger.debug("Error parsing URL '"+url+"'.", ex);
                ret.setState(ProcessedData.State.FAILURE);
            }
    }    
}
