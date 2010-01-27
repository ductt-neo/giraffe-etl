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

package hu.sztaki.ilab.giraffe.core.events;

/**
 *
 * @author neumark
 */
public interface EventPredicate {
    // Call this to pass the event-specific XML subtree which contains config
    // information for the given event type.
    public void setParameters(org.w3c.dom.Element parameters);
    // This function must be called before event type can be used.
    public boolean init();
    // For each column value, isEvent returns whether the given event is true
    // for the given record or not.
    public boolean isEvent(String value);
    // cleanup() should be called after every line has been processed.
    // An optional DOM element of ANY type may be passed. If it is not null
    // and if the predicate object has meta-information is would like to send
    // to meta.xml, then cleanup() can add it.
    public boolean close(org.w3c.dom.Element parameters);
}
