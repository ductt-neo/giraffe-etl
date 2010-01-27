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
 * The StringComparison event predicate takes a parameter of the form:
 * <parameter comparisonFunction="equals" comparisonValue="foo">
 */
package hu.sztaki.ilab.giraffe.core.events;

/**
 *
 * @author neumark
 */
public class StringComparison implements EventPredicate {

    private static class StringFunction {

        public StringFunction() {
        }
        protected String comparisonValue = null;

        public boolean init(String comparisonValue) {
            this.comparisonValue = comparisonValue;
            return this.comparisonValue != null;
        }

        public boolean evaluate(String input) {
            // STUB
            return false;
        }
    }

    private static class EndsWith extends StringFunction {

        public EndsWith() {
        }

        public boolean evaluate(String input) {
            if (null == input) {
                return false;
            }
            return input.endsWith(comparisonValue);
        }
    }

    private static class StartsWith extends StringFunction {

        public StartsWith() {
        }

        public boolean evaluate(String input) {
            if (null == input) {
                return false;
            }
            return input.startsWith(comparisonValue);
        }
    }

    private static class Contains extends StringFunction {

        public Contains() {
        }

        public boolean evaluate(String input) {
            if (null == input) {
                return false;
            }
            return input.contains(comparisonValue);
        }
    }

    private static class Equals extends StringFunction {

        public Equals() {
        }

        public boolean evaluate(String input) {
            if (null == input) {
                return false;
            }
            return input.equals(comparisonValue);
        }
    }

    private static class RegexMatches extends StringFunction {

        public RegexMatches() {
        }

        public boolean evaluate(String input) {
            if (null == input) {
                return false;
            }
            return input.matches(comparisonValue);
        }
    }

    private static java.util.Map<String,Class> stringFunctions = new java.util.HashMap<String,Class>();
    static {
        stringFunctions.put("contains", Contains.class);
        stringFunctions.put("endsWith", EndsWith.class);
        stringFunctions.put("equals", Equals.class);
        stringFunctions.put("regexMatches", RegexMatches.class);
        stringFunctions.put("startsWith", StartsWith.class);
    }

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(StringComparison.class);
    private String comparisonValue = null;    
    private StringFunction comparisonFunction = null;

    public void setParameters(org.w3c.dom.Element parameters) {
        if (parameters.hasAttribute("comparisonValue")) {
            comparisonValue = parameters.getAttribute("comparisonValue");
        }
        if (parameters.hasAttribute("comparisonFunction")
                && null != parameters.getAttribute("comparisonFunction")
                && stringFunctions.containsKey(parameters.getAttribute("comparisonFunction"))) {
            try {
            comparisonFunction = (StringFunction) (stringFunctions.get(parameters.getAttribute("comparisonFunction"))).newInstance();
            } catch (InstantiationException ex) {
                logger.error("Bug!", ex);
            } catch (IllegalAccessException ex) {
                logger.error("Bug!", ex);
            }
        }
    }

    public boolean init() {
        if (null == comparisonFunction) {
            logger.error("comparisonFunction attribute must be set.");
            return false;
        }
        if (null == comparisonValue) {
            logger.error("comparisonValue attribute must be set.");
            return false;
        }
        return comparisonFunction.init(comparisonValue);
    }

    public boolean isEvent(String value) {
        return comparisonFunction.evaluate(value);
    }

    public boolean close(org.w3c.dom.Element parameters) {
        // STUB
        return true;
    }
}
