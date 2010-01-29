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
package hu.sztaki.ilab.giraffe.core.factories;

import hu.sztaki.ilab.giraffe.core.util.StringUtils;
import hu.sztaki.ilab.giraffe.schema.datatypes.EventCondition;

/**
 *
 * @author neumark
 */
public class EventConditionGenerator {
    
    ObjectInstantiator eventObjectInstantiator;
    String expression = "";
    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(EventConditionGenerator.class);

    public EventConditionGenerator(ObjectInstantiator eventObjectInstantiator) {
        this.eventObjectInstantiator = eventObjectInstantiator;
    }

    public String getExpression() {
        return expression;
    }

    public boolean build(hu.sztaki.ilab.giraffe.schema.datatypes.EventCondition condition) {
        try {
            expression = recurseCondition(condition);
            return true;
        } catch (Exception ex) {
            logger.error("Failed to construct event condition.",ex);
            return false;
        }
    }

    private String recurseCondition(java.util.List<Object> conditionList, String separator) {
        java.util.List<String> ifFragments = new java.util.LinkedList<String>();
        for (Object listItem : conditionList) {
            if (null != listItem) {
                try {
                    ifFragments.add(castAndRecurse(listItem, listItem.getClass()));
                } catch (ClassCastException ex) {
                    logger.error("Could not interpret event object " + listItem.toString());
                }
            }
        }
        return StringUtils.printContainer(ifFragments.iterator(), separator);
    }

    private String addPredicate(hu.sztaki.ilab.giraffe.schema.datatypes.ObjectInstance eventObj) {
        String code = eventObjectInstantiator.generateCallingCode(eventObj, expression);
        return code;
    }

    private String addPredicate(org.w3c.dom.Element eventElement) {
        logger.debug("Predicate element: <" + eventElement.getTagName() + ">:" + eventElement.getFirstChild().getTextContent());
        return "someelement";
    }

    private String addPredicate(hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType giraffeEvent) {
        // check if the specified event is true:
        return "events.contains(hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType." + giraffeEvent.toString() + ")";
    }

    private String recurseCondition(EventCondition.Predicate pred) {
        // A predicate can contain two types of things:
        // 1. It can be a dt:objectInstance, which is
        // mapped to the java type hu.sztaki.ilab.giraffe.schema.datatypes.ObjectInstance;
        // 2. It can be a giraffe event, which is mapped to
        // hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType;
        Object predicate = pred.getAny();        
        try {
            return addPredicate((hu.sztaki.ilab.giraffe.schema.datatypes.ObjectInstance) predicate);
        } catch (ClassCastException ex1) {
            ;
        }
        try {
            return addPredicate(((javax.xml.bind.JAXBElement<hu.sztaki.ilab.giraffe.schema.dataprocessing.EventType>) predicate).getValue());
        } catch (ClassCastException ex2) {
            ;
        }
        try {
            return addPredicate((org.w3c.dom.Element) predicate);
        } catch (ClassCastException ex3) {
            logger.error(ex3);
        }
        throw new java.lang.ClassCastException("Could not interpret predicate of type "+pred.getClass().toString());
    }

    private String recurseCondition(EventCondition.Not event) {
        return " !(" + selectGetter(event) + ") ";
    }

    private String recurseCondition(EventCondition.And event) {
        return " (" + recurseCondition(event.getBooleanExpression(), " && ") + ") ";
    }

    private String recurseCondition(EventCondition.Or event) {
        return " (" + recurseCondition(event.getBooleanExpression(), " || ") + ") ";
    }

    private String recurseCondition(EventCondition.Xor event) {
        return " (" + recurseCondition(event.getBooleanExpression(), " ^ ") + ") ";
    }

    private String recurseCondition(EventCondition.True event) {
        return "true";
    }


    private String recurseCondition(EventCondition event) {
        return selectGetter(event);
        // Event references have already been replaced by events at this point,
        // so event references can safely be ignored.
    }

    private String selectGetter(Object o) {        
        for (java.lang.reflect.Method m : o.getClass().getDeclaredMethods()) {
            if (!m.getName().startsWith("get")) {
                continue;
            }
            try {
                Object ret = m.invoke(o, new Object[]{});
                if (null != ret) {
                    return castAndRecurse(ret, m.getReturnType());
                }
            } catch (Exception ex) {
                logger.error("Error invoking "+m.getName(), ex);
            }
        }
        logger.error("Could not cast event to type!");
        return null;
    }

    private <T> String castAndRecurse(Object o, Class<T> destinationClass) {

        if (destinationClass.equals(EventCondition.And.class)) {
            return recurseCondition((EventCondition.And) o);
        }
        if (destinationClass.equals(EventCondition.Not.class)) {
            return recurseCondition((EventCondition.Not) o);
        }
        if (destinationClass.equals(EventCondition.Or.class)) {
            return recurseCondition((EventCondition.Or) o);
        }
        if (destinationClass.equals(EventCondition.Xor.class)) {
            return recurseCondition((EventCondition.Xor) o);
        }
        if (destinationClass.equals(EventCondition.Predicate.class)) {
            return recurseCondition((EventCondition.Predicate) o);
        }
        if (destinationClass.equals(EventCondition.True.class)) {
            return recurseCondition((EventCondition.True) o);
        }
        if (destinationClass.equals(EventCondition.class)) {
            return recurseCondition((EventCondition) o);
        }
        logger.error("Failed to cast object " + o.toString());
        return null;
    }
}
