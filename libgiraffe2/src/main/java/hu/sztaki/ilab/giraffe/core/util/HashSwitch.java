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
import java.util.*;
/**
 *
 * @author neumark
 */

public class HashSwitch<K,I,O,C extends IHashCase<I,O>> {
    HashMap<K,C> hash;
    
    public HashSwitch(int size)
    {
        hash = new HashMap<K,C>(size);
    }

    public IHashCase<I,O> put(K key, C fun)
    {
        return hash.put(key,fun);
    }
    
    public boolean contains(K key)
    {
        return hash.containsKey(key);
    }
    
    public O switchby(K key,I input)
    {
        return hash.get(key).process(input);
    }
}
