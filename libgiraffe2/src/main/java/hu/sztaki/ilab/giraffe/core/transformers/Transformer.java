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

package hu.sztaki.ilab.giraffe.core.transformers;
/**
 * The Transformer interface allows a class to make simple modifications to a column. Once the TokenReader returns a standard-sized record, each column
 * is fed to it's Transformer in BatchProcessor.process(). Transformer's are handy for simple String transformations, for example, if we want to add 
 * http:// to the referrer in columns where it is missing. There is no way for the ProcessingEngines to tell if the a column was changed by the
 * Transformer or not, and unprocessed columns go straight from the transformer to the output file. If you need to know the value of a field as present
 * in the original input file, use a ProcessingEngine instead!
 * @author neumark
 */
public interface Transformer {
    /**
     * modifies column data
     * @param input original string
     * @return possibly modified column data
     */
    public String transform(String input);
}