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
package hu.sztaki.ilab.giraffe.dataprocessors.networkaddress;

import hu.sztaki.ilab.giraffe.core.database.LookupTable;
import hu.sztaki.ilab.giraffe.core.dataprocessors.*;
import java.util.*;
import org.apache.log4j.*;

/**
 *
 * @author neumark
 */
public class HostNameResolver extends hu.sztaki.ilab.giraffe.core.dataprocessors.ProcessingEngine {

    static Logger logger = Logger.getLogger(GeoIPResolver.class);
    private Date expirationDate = null;
    public static java.util.List<String> fl = new java.util.LinkedList<String>();
    private String[] nameServers = {};


    static {
        fl.add("hostname");
    }

    public java.util.List<String> getFieldList() {
        return fl;
    }

    public HostNameResolver(LookupTable lookupTable) {
        super(lookupTable);
    }

    // This class needs many threads, typically around 8.
    public HostNameResolver(LookupTable lookupTable, int threadCount) {
        super(lookupTable, threadCount);
    }

    public boolean init() {
        expirationDate = ProcessedData.expirationDate(3);
        // TODO(neumark): get nameservers from RunConfiguration!
        nameServers = new String[]{"195.111.2.2"};
        return super.init();
    }

    public boolean isValid(ProcessedData pd) {
        // make sure the data is correct in general
        if (!super.isValid(pd)) {
            return false;
        // add a check for the correct date
        }
        if (pd.getTimestamp() == null || pd.getTimestamp().before(expirationDate)) {
            logger.debug("previous data expired, must recalculate.");
            return false;
        }
        return true;
    }

    protected class HostNameResolverRunnable extends ProcessingEngine.ProcessingRunnable {

        private hu.sztaki.ilab.giraffe.dataprocessors.implementation.DNSResolver resolver;

        public HostNameResolverRunnable(String[] nameServers) throws java.net.UnknownHostException {
            resolver = new hu.sztaki.ilab.giraffe.dataprocessors.implementation.DNSResolver(nameServers);
        }

        public void performTask(String ipaddr) {
            ProcessedData ret = new ProcessedData();
            try {
                // For debugging only:
                //Thread.sleep(800000);
                ret.setField("hostname", resolver.reverseDns(ipaddr));
            } catch (Exception ex) {
                logger.warn("R-DNS lookup of IP " + ipaddr + " failed.", ex);
                ret.setState(ProcessedData.State.FAILURE);
            }
            writeResult(ipaddr, ret);
        }
    }

    protected ProcessingEngine.ProcessingRunnable getRunnable() {
        try {
            return new HostNameResolverRunnable(nameServers);
        } catch (java.net.UnknownHostException ex) {
            logger.error("Error creating HostNameResolver thread.", ex);
        }
        return null;
    }
}