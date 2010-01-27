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

import java.io.*;
import java.util.*;
import com.maxmind.geoip.*;
import hu.sztaki.ilab.giraffe.core.database.LookupTable;
import org.apache.log4j.*;
import hu.sztaki.ilab.giraffe.core.dataprocessors.*;

/**
 *
 * @author neumark
 */
public class GeoIPResolver extends ProcessingEngine {

    static Logger logger = Logger.getLogger(GeoIPResolver.class);
    LookupService geoIPASNum, geoLiteCity;
    public static java.util.List<String> fl = new java.util.LinkedList<String>();


    static {
        fl.add("asnCode");
        fl.add("countryCode");
        fl.add("countryName");
        fl.add("region");
        fl.add("city");
        fl.add("postalCode");
        fl.add("areaCode");
    }

    public java.util.List<String> getFieldList() {
        return fl;
    }
    Date geoDBModified = null;

    public GeoIPResolver(LookupTable lookupTable) {
        super(lookupTable);
    }

    // This processing engine is thread-safe, so several threads may run simultaneously.
    public GeoIPResolver(LookupTable lookupTable, int threadCount) {
        super(lookupTable, threadCount);
    }

    public boolean init() {
        try {
            loadipDB();
        } catch (IOException e) {
            logger.fatal("Failed to load GeoIP databases!", e);
            return false;
        }
        return super.init();
    }

    void loadipDB() throws IOException {
        // Todo: close these!
        String loc = this.resourceDir + "/unversioned_datasources/GeoIP/";
        String geolitecity = loc + "GeoLiteCity.dat";
        String geoipasnum = loc + "GeoIPASNum.dat";
        geoLiteCity = new LookupService(geolitecity, LookupService.GEOIP_MEMORY_CACHE);
        geoIPASNum = new LookupService(geoipasnum, LookupService.GEOIP_MEMORY_CACHE);
        //geoIP = new LookupService(loc+"GeoIP.dat", LookupService.GEOIP_MEMORY_CACHE );
        geoDBModified = new Date(Math.max(new File(geolitecity).lastModified(), new File(geoipasnum).lastModified()));
    }

    public void cityLookup(String ip, ProcessedData pd) {
        try {
            Location l = geoLiteCity.getLocation(ip);
            if (l == null) {
                logger.warn("GeoIPResolver: could not look up IP address " + ip);
                pd.setState(ProcessedData.State.FAILURE);
            } else {
                pd.setField("countryCode", l.countryCode);
                pd.setField("countryName", l.countryName);
                pd.setField("region", l.region);
                pd.setField("city", l.city);
                pd.setField("postalCode", l.postalCode);
                pd.setField("areaCode", l.area_code + "");
            }
        } catch (Exception e) {
            logger.error("Caught exception during city lookup.", e);
        }
    }

    public void orgLookup(String ipaddr, ProcessedData pd) {
        try {
            String asn = geoIPASNum.getOrg(ipaddr);
            if (asn != null) {
                pd.setField("asnCode", asn);
            }
        } catch (Exception e) {
            logger.error("IO Exception", e);
        }
    }

    protected class GeoIPRunnable extends ProcessingEngine.ProcessingRunnable {
    public GeoIPRunnable() {}
    public void requestStop() {
        geoLiteCity.close();
        geoIPASNum.close();
        super.requestStop();
    }

    public void performTask(String ipaddr) {
        ProcessedData ret = new ProcessedData();
        ret.setDataSourceModified(geoDBModified);
        cityLookup(ipaddr, ret);
        orgLookup(ipaddr, ret);
        writeResult(ipaddr, ret);
    }
    }

    protected ProcessingEngine.ProcessingRunnable getRunnable() {return new GeoIPRunnable();}

}
                               