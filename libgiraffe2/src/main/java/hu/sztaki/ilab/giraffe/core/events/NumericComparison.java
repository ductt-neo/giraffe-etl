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
import org.apache.log4j.Logger;
/**
 * The NumericComparison class compares values to a specified value to column
 * contents. Parameters should be:
 * <parameters comparisonValue="12" comparisonResult="0" /> where
 * comparisonValue is the value we compare column contents to, and comparisonResult
 * should be:
 *   -1 if we want value < 12
 *    0 if we want value == 12
 *    1 if we want value > 12
 * @author neumark
 */
// TODO(neumark): write for types other than int!
public class NumericComparison implements EventPredicate {

    private static Logger logger = Logger.getLogger(NumericComparison.class);
    private Integer comparisonResult = null;
    private Integer comparisonValue = null;
    private int unparseable = 0;

    public NumericComparison() {
    }

    private Integer saveAttribute(org.w3c.dom.Element parameters, String attrName) {
        Integer val = null;
        if (parameters.hasAttribute(attrName) && null != parameters.getAttribute(attrName)) {
            try {
                val = new Integer(Integer.parseInt(parameters.getAttribute(attrName)));
            } catch (java.lang.NumberFormatException ex) {
                logger.error("Error initializing event: '"+parameters.getAttribute(attrName)+ "' is not an int.");
            }
        }
        return val;
    }

    public void setParameters(org.w3c.dom.Element parameters) {
        comparisonResult = saveAttribute(parameters, "comparisonResult");
        comparisonValue = saveAttribute(parameters, "comparisonValue");
    }

    public boolean init() {
        return comparisonResult != null && comparisonValue != null;
    }

    public boolean isEvent(String value) {
        try {
            Integer intVal = new Integer(Integer.parseInt(value));
            return Integer.signum(intVal.compareTo(comparisonValue)) == comparisonResult;
        } catch (NumberFormatException ex) {
            unparseable++;
        }
        return false;
    }

    public boolean close(org.w3c.dom.Element parameters) {
        // TODO(neumark) : log conversion failures.
        return true;
    }
}
