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
import java.util.*;
import hu.sztaki.ilab.giraffe.core.util.StringUtils;
import org.apache.log4j.*;
/**
 * This class is responsible for the storage and serialization of processed data. An internal HashMap stores (key,value) pairs, which can be manipulated
 * by the getField()/setField() functions. Serialization and unserialization return a string, which is written to a JE DB and eventually, to a file.
 * This class also has a State field, which describes the state of the data in the HashMap. Optionally, a timestamp can be added to the data.
 * @author neumark
 */
public class Record{
     
    static Logger logger = Logger.getLogger(Record.class);

    /* --- These functions are definitely needed: --
     generated classes will override these: */
    // The conversions take place within these functions, as the destination type
    // of each field in the conversion is only known to the generated code which
    // extends this class.
    public boolean readStringRecord(java.util.List<String> record) {return false;}
    public boolean readJdbcRecord(java.sql.ResultSet rset) {return false;}


    public static java.text.DateFormat dateFormat = null;

    public static class PDFormat {
        public String columnSeparator;
        public String tokenSeparator;
        public PDFormat(String columnSeparator, String tokenSeparator) {
            this.columnSeparator = columnSeparator;
            this.tokenSeparator = tokenSeparator;
        }
        public String getDefaultColumnDelimiter() {return columnSeparator;}
        public String getDefaultTokenSeparator() {return tokenSeparator;}
    }
    /**
     * Format used for serialization/unseralization
     */
    private static PDFormat outputFormat = new PDFormat("|",":");
    
    /**
     * Meaning of State values:
     * FAILURE - the data in the ProcessedData is inconsistent or missing, processing of the raw input failed.
     * OK - The ProcessedData object contains valid data (although further processing my be necessary if the data is still applicable, see
     * ProcessingEngine.isValid()
     * EMPTY - The ProcessedData object is empty, its hashtable has no elements. Although this is deductible from the state of the HashMap, it is
     * a state quite distinct from the previous two, warranting a separate value to State.
     */
    public enum State {FAILURE, OK, EMPTY, UNDEFINED};
    
    private HashMap<String,String> dataPairs = new HashMap<String,String>();
    
    private State dataState = State.EMPTY;
    
    /**
     * A timestamp is set by setTimestamp(). It can be used to determine of the data is too old to use.
     */
    private Date timestamp = null;
    private Date dataSourceModified = null;
    
    public int size() {return dataPairs.size();}
    
    public State getState() {return dataState;}
    public void setState(State s) {this.dataState = s;}
    public Date getTimestamp() { return timestamp; }
    public Date getDataSourceModified() { return dataSourceModified;}
    
    private void updateState()
    {
         if (dataState != State.FAILURE && dataPairs.size() > 0) dataState = State.OK;
    }
    
    public Record(){}
    
    /**
     * Given the number of days a piece of data is considered applicable, this function returns the latest date which is still within the bound.
     * @param days The number of days data can go without becoming stale.
     * @return The expiration date of the data.
     */
    public static Date expirationDate(int days)
    {
       Calendar c= Calendar.getInstance();
       c.add(c.DATE, -days);
       return c.getTime();
    }
    
    /**
     * Creates a new ProcessedData from the serialized data and a serialized data specification.
     * @param of
     * @param serializedData
     */
    public Record(String serializedData)
    {
        //logger.debug("Processed Data will unserialize string '"+serializedData+"'");        
        unserialize(serializedData);
        if (size() > 0) dataState = State.OK;
    
    }
    
    /**
     * Loads the serialized data into the current ProcessedData object.
     * @param input
     */
    public void unserialize(String input)
    {
        if (input == null || input.length() < 1) return;
        String[] entries = input.split(StringUtils.addslashes(outputFormat.getDefaultColumnDelimiter()));
        //logger.debug("found "+entries.length+" entries.");
        for (int i = 0; i < entries.length; i++)
        {
            // TODO: the separator character should be escapable!
            String[] pair = entries[i].split(hu.sztaki.ilab.giraffe.core.util.StringUtils.addslashes(outputFormat.getDefaultTokenSeparator()), 2);
            //logger.debug(" entry: "+entries[i]+" pairSize: "+pair.length);
            
            if (pair.length != 2)
            {
                //logger.debug("Error processing data with separator='"+outputCSVFormat.getColumnDelimiter()+"' and pairSeparator='"+outputCSVFormat.pairSeparator+"': "+entries[i]);
            }
            else
            {
                dataPairs.put(pair[0], pair[1]);
            }                
        }
        this.loadInternalState();
    }
    
    public String serialize()
    {
        String ret = "";
        Iterator<String> it = dataPairs.keySet().iterator();
        while(it.hasNext())
        {
            String key = it.next();
            String data = getField(key);
            if (data == null) data = "";
            if (ret.length() > 0) ret += outputFormat.getDefaultColumnDelimiter();
            ret += key+outputFormat.getDefaultTokenSeparator()+data;
        }
        if (ret.length() > 0) ret+=outputFormat.getDefaultColumnDelimiter();
        return ret+saveInternalState();
    }
    
    private void loadInternalState()
    {
        String s = getField("ProcessedDataInternalState");
        if (s != null)
        {
            dataState = State.valueOf(s);
            dataPairs.remove("ProcessedDataInternalState");
        }
        s = getField("timestamp");
        if (s != null) {
            timestamp = parseDate(s);
            dataPairs.remove("timestamp");
        }
        s = getField("dataSourceModified");
        if (s != null) {
            dataSourceModified = parseDate(s);
            dataPairs.remove("dataSourceModified");
        }

    }
    
    private Date parseDate(String s)
    {
        try {return dateFormat.parse(s);}
        catch (java.text.ParseException e) {logger.error("could not parse date "+s,e);}
        return null;
    }
    
    public void setTimestamp()
    {
        this.timestamp = new Date();
    }
    
    public void setDataSourceModified(Date modDate) {this.dataSourceModified = modDate;}
                
    private String saveInternalState()
    {
        String ret = "";
        if (dataState == State.OK && size() == 0 ) dataState = State.EMPTY;
        ret += "ProcessedDataInternalState"+outputFormat.getDefaultTokenSeparator()+dataState.toString();
        if (timestamp != null) {ret += outputFormat.getDefaultColumnDelimiter()+"timestamp"+outputFormat.getDefaultTokenSeparator()+dateFormat.format(timestamp);}
        if (this.dataSourceModified != null) {ret += outputFormat.getDefaultColumnDelimiter()+"dataSourceModified"+outputFormat.getDefaultTokenSeparator()+dateFormat.format(this.dataSourceModified);}
        return ret;
    }
    
    /**
     * Returns the value of a field
     * @param fieldName the name of the field
     * @return The vale of the field as recorded in the HashMap of this ProcessedData, or <code>null</code> if the field was not set.
     */
    public String getField(String fieldName)
    {
        return dataPairs.get(fieldName);
    }
    
    /**
     * Sets a field value
     * @param fieldName
     * @param value
     * @return The previous value of that field (if it was set).
     */
    public String setField(String fieldName, String value)
    {
        String ret = dataPairs.put(fieldName, value);
        updateState();
        return ret;
    }

    /**
     * Inserts the values of every field named in the fieldList. If no value is given, the empty string ("") is added. Upon return, the
     * length o fieldList and values should be equal.
     * @param fieldList
     * @param values
     */
    public void getFieldValues(Collection<String> fieldList, Collection<String> values)
    {
        for (String key : fieldList) values.add(StringUtils.unnullify(getField(key)));
    }
}
