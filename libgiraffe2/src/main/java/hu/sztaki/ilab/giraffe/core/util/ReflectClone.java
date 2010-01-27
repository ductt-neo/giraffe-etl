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
package hu.sztaki.ilab.giraffe.core.util;

import java.lang.reflect.Field;

/**
 * Uses the reflection API to copy values of fields from source to target classes.
 * @author neumark
 */
public class ReflectClone {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ReflectClone.class);
    // Only copies fields of source which are also present in target.
    // Performs a shallow copy.

    public static void CopyExistingFields(Object source, Object target) throws java.lang.CloneNotSupportedException{
        for (Class sourceClass = source.getClass(); sourceClass != Object.class; sourceClass = sourceClass.getSuperclass()) {
            for (Field currentField : sourceClass.getDeclaredFields()) {
                try {
                    currentField.setAccessible(true);
                    currentField.set(target, currentField.get(source));
                } catch (Exception ex) {
                    logger.error("Error copying object fields, ", ex);
                    throw new java.lang.CloneNotSupportedException();
                }
            }
        }        
    }
}
