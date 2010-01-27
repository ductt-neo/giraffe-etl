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
 * ProcessingEngineTaskQueueMXBean.java
 *
 * Created on July 28, 2009, 2:45 PM
 */

package hu.sztaki.ilab.giraffe.core.management;

import java.beans.ConstructorProperties;
import java.util.Date;

/**
 * Interface ProcessingEngineTaskQueueMXBean
 *
 * @author neumark
 */
public interface ProcessingEngineTaskQueueMXBean
{
            //QueueSample holds the data returned for the JMX request.

        public class QueueSample {

            private final Date date;
            private final int size;
            private final String head;

            @ConstructorProperties({"date", "size", "head"})
            public QueueSample(Date date, int size, String head) {
                this.date = date;
                this.size = size;
                this.head = head;
            }

            public Date getDate() {
                return date;
            }

            public int getSize() {
                return size;
            }

            public String getHead() {
                return head;
            }
        }
        public QueueSample getQueueSample();
}


