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
package hu.sztaki.ilab.giraffe.core.database;

import hu.sztaki.ilab.giraffe.core.processingnetwork.Record;
import java.io.*;
import java.util.*;
import com.sleepycat.je.*;
import org.apache.log4j.*;

/**
 * BDBLookupTable provides a simple interface for storing/retrieving (key, data) pairs, where the key is generally a String, and the corresponding data
 * is a ProcessedData object. BDBLookupTable uses a BDB JE database to store the pairs once the ProcessedData object is serialized using the serialization
 * format provided in the constructor. The purpose of the BDBLookupTable object is to act as a cache for the ProcessingEngines: if an existing
 * (Column_Data, Processed_Data) pair is found for some column in the input file, then that version is used, and the value is not recomputed.
 * Because a ProcessingEngine can have several threads, the threads must known when another thread has already started the processing of a value, so
 * it is not simultaneously processed by several threads. To this end, it is possible to call put(), with the value argument as <code>null</code>, which
 * will write an EMPTY ProcessedData value into the Database. This empty value signals other threads to skip that value, because processing is underway.
 * @author neumark
 */
public class BDBLookupTable implements LookupTable {

    static Logger logger = Logger.getLogger(BDBLookupTable.class);
    /**
     * The BDB database which stores the data pairs.
     */
    Database db;

    /**
     * The serialization format used to flatten ProcessedData objects.
     */
    BDBLookupTable(Database d) {
        db = d;
    }

    /**
     * Nobody uses this version of put() except the other version, because it requires the raw byte array as the input parameters.
     * It is public only because somebody may want to use BDBLookupTable for another application some day.
     * @param key
     * @param value
     * @return <code>true</code> if the operations was successful.
     * @throws com.sleepycat.je.DatabaseException
     */
    public boolean put(byte[] key, byte[] value) {
        try {
            DatabaseEntry theKey = new DatabaseEntry(key);
            DatabaseEntry theData = new DatabaseEntry(value);
            // free space for new value:
            return (db.put(null, theKey, theData) == OperationStatus.SUCCESS);
        } catch (Exception e) {
            logger.error("exception", e);
        }
        return false;
    }

    /**
     * Writes a (Unprocessed column value, Corresponding processed data) pair to the database. The corresponding processed data can be <code>null</code>.
     * This means that processing this value is underway, and if it is encountered again, it shall not be computed. For details see ProcessingEngine
     * @param key The raw column data
     * @param pd The ProcessedData object resulting from processing the key
     * @return <code>true</code> if the put() operation was successful
     * @throws com.sleepycat.je.DatabaseException
     */
    public boolean put(String key, Record pd) {
        String value = "";
        if (pd != null) {
            value = pd.serialize();
        //logger.debug("Inserting into lookuptable pair ("+key+","+value+")");
        }//else logger.debug("Inserting into lookuptable pair ("+key+",null)");
        boolean ok = true;
        try {
            ok = put(key.getBytes("UTF-8"), value.getBytes("UTF-8"));
        } catch (Exception e) {
            String dbname = "";
            try {
                dbname = db.getDatabaseName();
            } catch (Exception ex) {
                ;
            }
            logger.error("Exception on put into database " + dbname + " for pair (" + key + "," + value + ")", e);
        }
        return ok;
    }

    /**
     * Returns the ProcessedData belonging to the given raw column value.
     * @param key Column value
     * @return A processedData object if one is found the DB, <code>null</code> otherwise.
     */
    public Record get(String key) {
        Record ret = null;

        try {
            byte[] keyBytes = key.getBytes("UTF-8");
            DatabaseEntry theKey = new DatabaseEntry(keyBytes);
            ret = get(theKey);
        } catch (Exception e) {
            logger.error("Exception caught using key " + key, e);
        }
        return ret;
    }

    /**
     * Very rarely used (if ever). If you need this, read the code!
     * @param theKey
     * @return
     */
    public Record get(DatabaseEntry theKey) {

        Record pd = null;
        Cursor cursor = null;
        try {
            DatabaseEntry theData = new DatabaseEntry();

            // Open a cursor using a database handle
            cursor = db.openCursor(null, null);

            // Perform the search
            OperationStatus retVal = cursor.getSearchKey(theKey, theData, LockMode.DEFAULT);

            // NOTFOUND is returned if a record cannot be found whose key 
            // matches the search key AND whose data begins with the search data.
            if (retVal == OperationStatus.NOTFOUND) {
                //logger.debug("Key '"+new String(theKey.getData())+"' not found in lookupTable.");
            } else {
                // Upon completing a search, the key and data DatabaseEntry 
                // parameters for getSearchBothRange() are populated with the 
                // key/data values of the found record.
                String foundData = new String(theData.getData(), "UTF-8");
                //logger.debug("For key '"+new String(theKey.getData())+"' found associated record '" + foundData + "'");
                pd = new Record(foundData);
            }
            cursor.close();

        } catch (Exception e) {
            logger.error("Exception caught: ", e);
        }
        return pd;
    }

    /**
     * Closes the corresponding JE database.
     * @throws com.sleepycat.je.DatabaseException
     */
    public void close() {
        try {
            db.close();
        } catch (Exception e) {
            logger.error("caught exception", e);
        }
    }

    public Object getMBean() {
        /*
        try {
            return new com.sleepycat.je.jmx.JEMonitor(this.db.getEnvironment().getHome().toString());
        } catch (Exception ex) {
            logger.error("Error creating JMX MBean", ex);
        } */
        return null;
    }
}
