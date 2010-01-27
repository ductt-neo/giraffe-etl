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
package hu.sztaki.ilab.giraffe.core;

import com.sun.org.apache.xerces.internal.util.XMLCatalogResolver;
import org.apache.log4j.Logger;

/**
 *
 * @author neumark
 */
public class Globals {

    public static int queueSize = 5000;
    public static int queueWaitTimeoutSeconds = 10;
    private static Logger logger = Logger.getLogger(Globals.class);
    private static XMLCatalogResolver catalogResolver = null;
    static {
        try {
            catalogResolver = new XMLCatalogResolver(new String[] {ClassLoader.getSystemResource("xml/giraffeCatalog.xml").toURI().toString()});
        } catch (Exception ex) {
            logger.error("Error locating XML catalog: ", ex);
            System.exit(1);
        }
    }

    public static XMLCatalogResolver getCatalogResolver() {
        return catalogResolver;
    }

    public static final String standardDateFormat = "yyyy.MM.dd HH:mm:ss";
}
/* GRAND GIRAFFE2 TODO LIST
 * - References in XML files
 * - Input/Output: actual reading of files/tables.
 */
