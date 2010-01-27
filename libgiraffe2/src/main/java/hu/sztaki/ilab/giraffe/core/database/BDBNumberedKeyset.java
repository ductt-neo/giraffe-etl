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
import hu.sztaki.ilab.giraffe.core.util.Pair;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import java.util.*;
import com.sleepycat.je.*;
import org.apache.log4j.*;
/**
 * BDBNumberedKeyset creates a dictionary by assigning strings numbers. The first time a String is encountered it is given an integer id. The
 * class is implemented by storing (id, string) pairs in a BDB JE database.
 * @author neumark
 */
public class BDBNumberedKeyset extends NumberedKeyset {
    
    static Logger logger = Logger.getLogger(BDBNumberedKeyset.class);
    /**
     * The JE DB used to store the (id,string) pairs.
     */
    Database db;
    
    /**
     * The ID of the next newly encountered string. This value is incremented after it is used, so each string gets a unique number.
     */
    int currentID;
    
    public BDBNumberedKeyset()
    {
    }

    public void init(Database d) {
        db = d;
        currentID = findNextID();
    }

    private int findNextID()
    {
        int id = -1;
        Cursor cursor = null;
        try
        {
            cursor = db.openCursor(null, null);

            // Cursors need a pair of DatabaseEntry objects to operate. These hold
            // the key and data found at any given position in the database.
            DatabaseEntry rawEntry = new DatabaseEntry();
            DatabaseEntry idEntry = new DatabaseEntry();

            // To iterate, just call getNext() until the last database record has been 
            // read. All cursor operations return an OperationStatus, so just read 
            // until we no longer see OperationStatus.SUCCESS
            while (cursor.getNext(rawEntry, idEntry, LockMode.DEFAULT) ==
                    OperationStatus.SUCCESS) {
                // getData() on the DatabaseEntry objects returns the byte array
                // held by that object. We use this to get a String value. If the
                // DatabaseEntry held a byte array representation of some other data
                // type (such as a complex object) then this operation would look 
                // considerably different.
                //String rawString = new String(rawEntry.getData(), "UTF-8");
                EntryBinding myBinding = TupleBinding.getPrimitiveBinding(Integer.class);
                Integer idInt = ((Integer)(myBinding.entryToObject(idEntry)));
                id = Math.max(id, idInt.intValue());                
            }            
        }        
        catch (com.sleepycat.je.DatabaseException e) {logger.error("Could not find highest ID for keyset, assuming 0!",e);} 
        finally
        {
            try {cursor.close();} catch (Exception e) {logger.error("Exception!",e);}
        }
        return id+1;
    }
    
    
    /**
     * Finds the existing ID or assigns a new number for the given string.
     * 
     * @param key The string for which we want the id.
     * @return A pair of the form (id,newly_created) where id is the id assnigned to the string, and newly_created is true if the id was freshly assigned.
     * @throws com.sleepycat.je.DatabaseException
     */
    public Pair<Integer,Boolean> getID(String key)
    {
        Pair<Integer, Boolean> id = new Pair<Integer, Boolean>(new Integer(-1), new Boolean(false));
        try
        {
            id = getID(key.getBytes("UTF-8"));
        }
        catch (Exception e)
        {
            String dbname = "";
            try {  dbname = db.getDatabaseName();}
            catch (DatabaseException ex) {;}
            logger.error("Exception on put into database "+dbname+" for key "+key, e);
        }
        return id;
    }
    
    
    /**
     * Do not use this at home or at all (without reading the source)!
     * @param key
     * @return
     * @throws com.sleepycat.je.DatabaseException
     */
    @SuppressWarnings("unchecked")
    public Pair<Integer, Boolean> getID(byte[] key) throws DatabaseException
    {
        // First element of pair is the ID, second is true if this is a freshly generated new ID.
        Pair<Integer, Boolean> id = new Pair<Integer, Boolean>(new Integer(-1), new Boolean(false));
        DatabaseEntry k = new DatabaseEntry(key);
        Integer keyNumber = new Integer(currentID);
        DatabaseEntry theData = new DatabaseEntry();
        EntryBinding myBinding = TupleBinding.getPrimitiveBinding(Integer.class);
        myBinding.objectToEntry(keyNumber, theData);
        OperationStatus stat = db.putNoOverwrite(null, k, theData);
        if (stat == OperationStatus.SUCCESS)
        {
            // the key is new
            id.first = keyNumber;
            //logger.debug("New ID generated");
            id.second = new Boolean(true);
            currentID++;
        }
        else 
        {
            if (stat != OperationStatus.KEYEXIST) logger.error("Key "+key+" lookup failure on database "+db.getDatabaseName()+ " recieved error code: "+stat.toString());
            // the key already exists, let's read its value from the DB
            DatabaseEntry existingIndex = new DatabaseEntry();
            if (db.get(null, k, existingIndex, LockMode.DEFAULT) == OperationStatus.SUCCESS)
            {
                //logger.debug("Existing ID found");
                id.first =  ((Integer)(myBinding.entryToObject(existingIndex)));
                id.second = new Boolean(false);
            }
            else logger.error("Key "+key+" not in db "+db.getDatabaseName()+" but cannot insert.");
        }
        
        return id;
    }
    
    public void close()
    {
        try {
        db.close();
        }
        catch (Exception e) {
            logger.error("caught exception", e);
        }
    }
    
    private class BDBNumberedKeysetIterator implements java.util.Iterator<Pair<Integer,String>> {
        private Pair<Integer,String> currentValue = null;
        private boolean hasNext = true;
        private Cursor cursor = null;
        // Cursors need a pair of DatabaseEntry objects to operate. These hold
        // the key and data found at any given position in the database.
        DatabaseEntry rawEntry = new DatabaseEntry();
        DatabaseEntry idEntry = new DatabaseEntry();
        public BDBNumberedKeysetIterator() {
              // iterate over all records in the keyset            
            try {
                // Open the cursor. 
                cursor = db.openCursor(null, null);
                next(); // The first call to next returns (null,null)
            } catch (Exception e) {logger.error("Exception in iterator", e);}
        }

        public Pair<Integer, String> next() {
            // To iterate, just call getNext() until the last database record has been 
            // read. All cursor operations return an OperationStatus, so just read 
            // until we no longer see OperationStatus.SUCCESS
            Pair<Integer, String> previousValue = currentValue;
            try {
                if (cursor.getNext(rawEntry, idEntry, LockMode.DEFAULT) ==
                        OperationStatus.SUCCESS) {
                    // getData() on the DatabaseEntry objects returns the byte array
                    // held by that object. We use this to get a String value. If the
                    // DatabaseEntry held a byte array representation of some other data
                    // type (such as a complex object) then this operation would look 
                    // considerably different.
                    String rawString = new String(rawEntry.getData(), "UTF-8");
                    EntryBinding myBinding = TupleBinding.getPrimitiveBinding(Integer.class);
                    Integer idInt = ((Integer) (myBinding.entryToObject(idEntry)));
                    currentValue = new Pair<Integer, String>(idInt, rawString);
                } else {
                    hasNext = false;
                    try { cursor.close(); } catch (Exception e) { logger.error("Error closing database cursor: " + e); }
                }
            } catch (Exception e) { logger.error("Error exporting database: " + e); }
            return previousValue;
        }

        public boolean hasNext() {
            return hasNext;
        }
        // Returns true if the iteration has more elements.

        public void remove() {
            logger.error("Iterator has empty remove()!");
        }
        //Removes from the underlying collection the last element returned by the iterator (optional operation).
    }

    public java.util.Iterator<Pair<Integer,String>> iterator() {
        java.util.Iterator<Pair<Integer,String>> it = new BDBNumberedKeysetIterator();
        return it;
    }

  public Object getMBean() {
      /*
      try {
      return new com.sleepycat.je.jmx.JEMonitor(this.db.getEnvironment().getHome().toString());
      } catch (Exception ex) {
          logger.error("Error creating JMX MBean", ex);
      }*/
      return null;
  }
    
}