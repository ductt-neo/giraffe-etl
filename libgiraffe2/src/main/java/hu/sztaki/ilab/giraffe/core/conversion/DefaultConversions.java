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
package hu.sztaki.ilab.giraffe.core.conversion;

import hu.sztaki.ilab.giraffe.core.xml.annotation.Conversion;
import hu.sztaki.ilab.giraffe.schema.datatypes.Parameters;
import java.io.PrintWriter;
import java.io.StringWriter;
import javax.xml.namespace.QName;

/**
 *
 * @author neumark
 */
public class DefaultConversions {

    static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(DefaultConversions.class);
    public static class IntConverter {
        private Parameters param = null;
        private Integer defaultValue = null;
        public IntConverter(Parameters param) {
            this.param = param;
        }
        public IntConverter() {}
        public boolean init() {
            if (param != null) {
                try {
                    defaultValue = Integer.parseInt(param.getOtherAttributes().get(new QName("default")));
                } catch (Exception ex) {
                    logger.error(ex);
                    return false;
                }
            }
            return true;
        }
        @Conversion(name="string2int")
        public int string2int(String s) {
            if (s == null) {
                if (defaultValue != null) return defaultValue;
                else throw new NullPointerException();
            }
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException ex) {
                if (defaultValue != null) return defaultValue;
                else throw ex;
            }
        }
        @Conversion(name="int2string")
        public String int2string(int i) {
            return i + "";
        }
    }

    @Conversion
    public static String exportException(java.lang.Throwable th) {
        StringWriter sw = new StringWriter();
        th.printStackTrace(new PrintWriter(sw));
        return "Exception "+th.getClass().toString()+" Message: "+th.getMessage()+" Stack trace: "+sw.toString().replace('\n',';');
    }

    public static class DateConverter {

        String formatString;
        java.text.DateFormat df;

        public boolean init() {
            try {                
                df = new java.text.SimpleDateFormat(formatString);
            } catch (Exception ex) {
                logger.error("Error initializing DefaultConversions.DateConverter: ", ex);
                return false;
            }
            return true;
        }

        public DateConverter(Parameters param) {
            formatString = param.getOtherAttributes().get(new QName("format"));
        }

        @Conversion(name="string2date")
        public java.util.Date stringToDate(String s) {
            if (s == null) return null;
            try {
            return df.parse(s);
            } catch (java.text.ParseException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Conversion(name="date2string")
        public String dateToString(java.util.Date d) {
            return df.format(d);
        }
    }

    public static void main(String[] args) {
        Parameters p = new Parameters();
        p.getOtherAttributes().put(new QName("format"),"dd/MMM/yyyy:HH:mm:ss ZZZZZ");
        DateConverter dc = new DateConverter(p);
        if (!dc.init()) System.out.print("Failed to init!");
        System.out.print(dc.stringToDate("04/Nov/2009:02:01:13 +0100").getMinutes());        
    }
}
