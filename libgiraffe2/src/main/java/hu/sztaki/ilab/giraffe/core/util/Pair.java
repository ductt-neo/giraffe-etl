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

/**
 *
 * @author neumark
 */
public class Pair<F,S> {
    public F first;
    public S second;
    public Pair(F f, S s) {first = f; second = s;}
    public boolean equals(Pair<F,S> other) {return first.equals(other.first) && second.equals(other.second);}
    public String toString() {return "("+((first!=null)?first.toString():"null")+", "+((second!=null)?second.toString():"null")+")";}
}
