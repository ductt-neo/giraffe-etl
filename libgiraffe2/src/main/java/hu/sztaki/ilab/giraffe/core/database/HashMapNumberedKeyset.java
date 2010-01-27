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
import java.io.*;
import java.util.*;
import org.apache.log4j.*;

/**
 *
 * @author neumark
 */
public class HashMapNumberedKeyset extends NumberedKeyset{

/**
 * HashMapNumberedKeyset stores entire string->int dictionary in memory.
 * @author neumark
 */

    static Logger logger = Logger.getLogger(HashMapNumberedKeyset.class);
    /**
     * The JE DB used to store the (id,string) pairs.
     */
    private HashMap<String,Integer> keysetDictionary = new HashMap<String,Integer>();

    /**
     * The ID of the next newly encountered string. This value is incremented after it is used, so each string gets a unique number.
     */
    int currentID = 0;

    public HashMapNumberedKeyset()
    { }

    /**
     * Finds the existing ID or assigns a new number for the given string.
     *
     * @param key The string for which we want the id.
     * @return A pair of the form (id,newly_created) where id is the id assnigned to the string, and newly_created is true if the id was freshly assigned.
     * @throws com.sleepycat.je.DatabaseException
     */
    public Pair<Integer,Boolean> getID(String key) {
        if (keysetDictionary.containsKey(key)) {
            return new Pair<Integer, Boolean>(keysetDictionary.get(key), new Boolean(false));
        }
        keysetDictionary.put(key, currentID);
        Pair<Integer,Boolean> ret = new Pair<Integer,Boolean>(new Integer(currentID), new Boolean(true));
        currentID++;
        return ret;
    }

    public void close()
    {
        keysetDictionary = null;
    }

    private class HashMapNumberedKeysetIterator implements java.util.Iterator<Pair<Integer, String>> {
        private Iterator<Map.Entry<String,Integer>> it;
        public HashMapNumberedKeysetIterator() {
            java.util.Set<Map.Entry<String,Integer>> c = keysetDictionary.entrySet();
            it = c.iterator();
        }
        public void remove() {
            logger.error("Iterator's remove() is empty!");
        }
        public boolean hasNext() {return it.hasNext();}
        public Pair<Integer,String> next() {
            Map.Entry<String,Integer> entry = it.next();
            return new Pair<Integer,String>(entry.getValue(),entry.getKey());
        }
    }


    public java.util.Iterator<Pair<Integer,String>> iterator() {
        return new HashMapNumberedKeysetIterator();
    }

    public Object getMBean() {return null;}
    
}
