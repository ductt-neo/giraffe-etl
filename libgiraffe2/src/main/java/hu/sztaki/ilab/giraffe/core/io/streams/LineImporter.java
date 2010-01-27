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

package hu.sztaki.ilab.giraffe.core.io.streams;

/**
 * The purpose of the LineParser class is to transform one line of input (a String) into
 * a Vector of Strings, where element of the vector corresponds to a column of the 
 * standard file format.
 * @author neumark
 */
public interface LineImporter {
    public java.util.List<String> parse (String line) throws java.text.ParseException;
    public boolean init();        
    public java.util.List<String> getColumns();
}
