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

package hu.sztaki.ilab.giraffe.core.xml.annotation;
import java.lang.annotation.*;
// copied from: http://java.sun.com/j2se/1.5.0/docs/guide/language/annotations.html
/**
 * Describes the conversion.
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
// At least the source or the destination type must be String!
public @interface Conversion {
String name() default "";
}