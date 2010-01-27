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

package hu.sztaki.ilab.giraffe.core.management;

import javax.management.*;
import java.lang.management.*;
import org.apache.log4j.*;
/**
 * The Monitor class is intended to give an overview of the state of the
 * processing job. It's copied from SimpleAgent in:
 * http://java.sun.com/developer/technicalArticles/J2SE/jmx.html
 * JEMonitor API description available at:
 * http://www.oracle.com/technology/documentation/berkeley-db/je/java/com/sleepycat/je/jmx/JEMonitor.html
 * @author neumark
 */
public class Monitor {

   private MBeanServer mbs = null;
   static Logger logger = Logger.getLogger(Monitor.class);
   public Monitor() {
       // Get the platform MBeanServer
       mbs = ManagementFactory.getPlatformMBeanServer();
   }

   public boolean registerBean(String name, Object mbean) {
       try {
        logger.debug("Registering MBean "+name+" "+mbean.toString());
        mbs.registerMBean(mbean, new ObjectName(name));
       } catch(Exception ex) {
            logger.error("An error occured attempting to register the MBean named '"+name+"'.", ex);
            return false;
       }
       return true;
   }
}
