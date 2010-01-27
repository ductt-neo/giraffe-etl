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
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import hu.sztaki.ilab.giraffe.core.processingnetwork.Record;
import java.io.*;
import java.util.*;
import com.sleepycat.je.*;
import org.apache.log4j.*;

/**
 * DatabaseHandler creates/opens/closes BerkelyDB JE database environments. An environment can have several tables, which this class creates/opens
 * when openLooupTable or openNumberedKeyset are called.
 * @author neumark
 */
public class DatabaseHandler {
    private Environment myDbEnvironment = null;
    /**
     * openLookupTables is a list of BDBLookupTable objects returned by openLookupTable(). It is copied to this list because the DatabaseHandler
     * must close all databases within the environment before closing the environment.
     */
    private LinkedList<BDBLookupTable> openLookupTables = new LinkedList<BDBLookupTable>();
    private LinkedList<BDBNumberedKeyset> openNumberedKeysets = new LinkedList<BDBNumberedKeyset>();
    static Logger logger = Logger.getLogger(DatabaseHandler.class);
    
    /**
     * In case this DB environment contains a table named in <code>tableNeme</code>, that table is opened (otherwise it is created).
     * A new BDBLookupTable object is created using this database.
     * @param tableName the name of the table.
     * @param of the format used to serialize ProcessedData objects before writing them to the table.
     * @return BDBLookupTable object which stores its data in the database named by <code>tableName</code>
     * @throws com.sleepycat.je.DatabaseException
     */
    public void cleanEmptyValues(Database d) {
        // iterate over all records in the keyset
        int num_deleted_records = 0;
        Cursor cursor = null;
        Transaction txn = null;
        try {
            // Open the cursor. 
            txn = myDbEnvironment.beginTransaction(null, null);
            cursor = d.openCursor(txn, null);
            // Cursors need a pair of DatabaseEntry objects to operate. These hold
            // the key and data found at any given position in the database.
            DatabaseEntry keyField = new DatabaseEntry();
            DatabaseEntry valueField = new DatabaseEntry();

            // To iterate, just call getNext() until the last database record has been 
            // read. All cursor operations return an OperationStatus, so just read 
            // until we no longer see OperationStatus.SUCCESS
            while (cursor.getNext(keyField, valueField, LockMode.DEFAULT) ==
                    OperationStatus.SUCCESS) {
                // getData() on the DatabaseEntry objects returns the byte array
                // held by that object. We use this to get a String value. If the
                // DatabaseEntry held a byte array representation of some other data
                // type (such as a complex object) then this operation would look 
                // considerably different.
                String key = new String(keyField.getData(), "UTF-8");                
                // logger.debug("For key '"+new String(keyField.getData())+"' found associated record '" + valueField.getData() + "'");
                if (valueField.getData().length == 0) {
                    cursor.delete();
                    logger.debug("Deleteing record with key '"+key+"' because the associated value is EMPTY (length = 0).");
                    num_deleted_records++;
                    continue;
                }
                String value = new String(valueField.getData(), "UTF-8");
                if (value.equals("")) {
                    cursor.delete();
                    logger.debug("Deleteing record with key '"+key+"' because the associated value is EMPTY (value = \"\").");
                    num_deleted_records++;
                    continue;
                }
                Record pd = new Record(value);
                if (pd.getState() == Record.State.EMPTY) {
                    cursor.delete();
                    logger.debug("Deleteing record with key '"+key+"' because the associated value is EMPTY (state is EMPTY).");
                    num_deleted_records++;
                    continue;                
                }
            }
        } catch (Exception e) {
            logger.error("Error cleaning DB of EMPTY values: " + e);
        } finally {
            // Cursors must be closed.
            try {
                if (num_deleted_records > 0) logger.info("Table "+d.getDatabaseName()+": deleted "+num_deleted_records+" records from cache because they were EMPTY.");
                cursor.close();
                cursor = null;
                txn.commit();
            } catch (Exception e) {
                logger.error("Error closing database cursor: " + e);
            }
        }
    }
    
    public BDBLookupTable openLookupTable(String tableName) throws DatabaseException
    {
            Database d = openOrCreateDB(tableName);
            cleanEmptyValues(d);
            BDBLookupTable lt = new BDBLookupTable(d);
            openLookupTables.add(lt);
            return lt;
    }
    
    /**
     * A table named by tableName is opened or created, then used by a fresh instance of BDBNumberedKeyset, which is returned.
     * @param tableName
     * @return A BDBNumberedKeyset which uses the database named by <code>tableName</code> to store its data.
     * @throws com.sleepycat.je.DatabaseException
     */
    public BDBNumberedKeyset openNumberedKeyset(String tableName) throws DatabaseException
    {
            Database d = openOrCreateDB(tableName);
            BDBNumberedKeyset lt = new BDBNumberedKeyset();
            lt.init(d);
            openNumberedKeysets.add(lt);
            return lt;
    }
    
    private Database openOrCreateDB(String name) throws DatabaseException
    {
        DatabaseConfig myDbConfig = new DatabaseConfig();
        myDbConfig.setAllowCreate(true);
        myDbConfig.setTransactional(true);
        return myDbEnvironment.openDatabase(null, name,myDbConfig);        
    }
            
    /**
     * Opens or creates a new DB environment.
     * @param filename The directory where the BDB JE files will be stored for this DB environment.
     * @throws com.sleepycat.je.DatabaseException
     */
    public DatabaseHandler(File filename, int cachepercent) throws DatabaseException
    {
        if (!filename.exists()) filename.mkdir();
        this.myDbEnvironment = openOrCreateEnvironment(filename, cachepercent);
    }
    
    private Environment openOrCreateEnvironment(File path, int cachepercent) throws DatabaseException
    {        
        // Open the environment. Allow it to be created if it does noLookupTablest already exist.        
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setCachePercent(cachepercent);
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        myDbEnvironment = new Environment(path, envConfig);        
        return myDbEnvironment;             
    }
    
    /**
     * Closes the object's DB environment along with all BDBLookupTable and BDBNumberedKeyset objects that have databases in this environment.
     * @throws com.sleepycat.je.DatabaseException
     */
    public void closeEnvironment() throws DatabaseException
    {
        Iterator<BDBLookupTable> ltit = openLookupTables.iterator();
        while (ltit.hasNext()) ltit.next().close();
        Iterator<BDBNumberedKeyset> nkit = openNumberedKeysets.iterator();
        while (nkit.hasNext()) nkit.next().close();
        myDbEnvironment.close();         
    }

}
