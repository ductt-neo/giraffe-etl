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

import java.lang.reflect.Method;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * A number of places in the XML configuration files allow the user to
 * refer to another XML fragment by reference. Reference resolver
 * replaces these referenes with the actual XML object the refer to or throws
 * an error.
 * Case 1 - A string refers to an element somewhere else in the XML document (by its
 * name attribute for example). This element is present in a Map [string->element].
 * In this case, resolution means fetching the correct member of the map.
 * Case 2 - A <choice> element allows use to use either a string (which refers to
 * another element) or the element itself. In this case, we must determine the
 * type of the expression, and if it is a string, revert to case 1, otherwise
 * return the element itself.
 * @author neumark
 */
public class ReferenceResolver <T> {

    T rootElement;
    java.util.Set<String> referredTo;
    java.util.Set<String> references;
    java.util.Map<Class, // Class of object
            java.util.Map<String, // Name of object
                Object // reference to the object itself.
                    >> namedObjects = new java.util.HashMap<Class,java.util.Map<String,Object>>();

    public ReferenceResolver(T rootElement) {
        this.rootElement = rootElement;
    }

    public void setReplacements(java.util.List<String> replaceables) {
        referredTo = new java.util.HashSet<String>();
        references = new java.util.HashSet<String>();
        for (String r : replaceables ) {
            referredTo.add(r);
            references.add(r+"Ref");
        }
    }

    public static <ReferencedType> ReferencedType resolveIfReference(
            java.util.Map<String, ReferencedType> referencedObjects,
            String reference,
            ReferencedType object) throws ReferenceResolutionException {
        if (null == reference && null == object) {
            throw new ReferenceResolutionException("Object or reference must be not null!");
        }
        if (null != object) {
            return object;
        }
        if (!referencedObjects.containsKey(reference)) {
            throw new ReferenceResolutionException("No object pointed to by referencde '" + reference + "'");
        }
        return referencedObjects.get(reference);
    }

    public static void logRefResEx(org.apache.log4j.Logger logger, ReferenceResolutionException ex) {
        logRefResEx(logger, ex, "");
    }

    public static void logRefResEx(org.apache.log4j.Logger logger, ReferenceResolutionException ex, String error) {
        StringWriter strWriter = new StringWriter();
        PrintWriter stackTraceWriter = new PrintWriter(strWriter);
        if (ex.getEnvelopedException() != null) {
            ex.getEnvelopedException().printStackTrace(stackTraceWriter);
        }
        stackTraceWriter.flush();
        logger.error(error + ex.toString() + strWriter.toString(), ex);
    }

    public static <ReferencedType> ReferencedType resolveIfReference(
            java.util.Map<String, ReferencedType> referencedObjects,
            Object objectOrReference) throws ReferenceResolutionException {
        if (null == objectOrReference) {
            throw new ReferenceResolutionException("Object or reference must be not null!");
        }
        // First we assume that we have a string reference to an object.
        try {
            String reference = (String) objectOrReference;
            if (referencedObjects.containsKey(reference)) {
                return referencedObjects.get(reference);
            }
            throw new ReferenceResolutionException("Reference refers to unknwon object " + reference);

        } catch (java.lang.ClassCastException ex) {
            // If the cast to string didn't work, we try to cast it to a ProcessingEngine.
            try {
                return (ReferencedType) objectOrReference;
            } catch (ClassCastException ex2) {
                throw new ReferenceResolutionException("Could not cast to String nor object of ActualType", ex2);
            }
        }
    }

    public static class ReferenceResolutionException extends Throwable {

        private Throwable envelopedException = null;

        public ReferenceResolutionException(String message) {
            super(message);
        }

        public ReferenceResolutionException(String message, Throwable ex) {
            super(message);
            envelopedException = ex;
        }

        public Throwable getEnvelopedException() {
            return envelopedException;
        }
    }
}
