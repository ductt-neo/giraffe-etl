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
package hu.sztaki.ilab.giraffe.core.io;

import hu.sztaki.ilab.giraffe.core.util.StringUtils;

/**
 *
 * @author neumark
 */
public class JDBCReader  {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(JDBCReader.class);
    private java.sql.Connection connection;
    private java.sql.Statement stmt;
    private java.sql.ResultSet rset;
    private int numColumns;    

    public JDBCReader(String connectionString, String username, String password, String query, int numColumns, String driverClass) throws java.sql.SQLException {
        
        this.numColumns = numColumns;        
        connect(connectionString, username, password, query, StringUtils.unnullify(driverClass));
    }

    private void connect(String connectionString, String username, String password, String query, String driverClass) throws java.sql.SQLException {        
        java.sql.Driver dbDriver = null;
        try {
        dbDriver = java.sql.DriverManager.getDriver(connectionString);
        } catch (java.sql.SQLException ex) {
            logger.debug("No suitable JDBC driver found for connection string "+connectionString+" attempting to load "+driverClass);
            try {
                dbDriver = (java.sql.Driver)this.getClass().getClassLoader().loadClass(driverClass).newInstance();
            } catch (Exception instex) {
                String errorMessage = "Error loading JDBC driver '"+driverClass+"'";
                logger.error(errorMessage, instex);
                throw new java.sql.SQLException(errorMessage);
            }
        }
        java.util.Properties info = new java.util.Properties();
	info.put("user", username);
	info.put("password", password);
        connection = dbDriver.connect(connectionString, info);
        stmt = connection.createStatement();
        rset = stmt.executeQuery(query);
        java.sql.ResultSetMetaData rsmd = rset.getMetaData();
        int resultColumns = rsmd.getColumnCount();
        if (numColumns != resultColumns) {
            String errorMessage = "JDBCReader error: Expected "+numColumns+" received "+resultColumns;
            logger.error(errorMessage);
            throw new java.sql.SQLException(errorMessage);
        }
        // based on: http://www.devdaily.com/java/edu/pj/jdbc/recipes/ResultSet-ColumnType.shtml
        for (int i = 0; i < rsmd.getColumnCount(); ++i) {
            int currentColumnType = rsmd.getColumnType(i);
            if (currentColumnType != 12) {
                String errorMessage = "JDBCReader error: All columns are expected to be strings. Column "+i+" is a "+rsmd.getColumnTypeName(numColumns);
                logger.error(errorMessage);
                throw new java.sql.SQLException(errorMessage);
            }
        }
    }

    public void close() {
        try {
            stmt.close();
            connection.close();
        } catch (java.sql.SQLException ex) {
            logger.error("Error closing JDBC Statment.", ex);
        }
    }

    protected java.util.List<String> readRecordImpl() throws java.text.ParseException {
        try {
            if (!rset.next()) return null;
            // we can safely assume that each record will contain the correct number of columns,
            // otherwise there would have been an SQLException throws in the constructor.
            java.util.List<String> record = new java.util.ArrayList<String>(numColumns);
            for (int i = 0; i < numColumns;++i) record.add(rset.getString(i));
            return record;
        } catch (Exception ex) {
            //throw new StreamRecordReader.RecordReaderException(ex);
        }   
        // STUB
        return null;
    }
}

