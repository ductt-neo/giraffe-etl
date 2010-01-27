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

import hu.sztaki.ilab.giraffe.core.util.StringUtils;
import hu.sztaki.ilab.giraffe.schema.dataformat.Quotes;
import hu.sztaki.ilab.giraffe.schema.dataformat.StringColumn;

/**
 *
 * @author neumark
 */
public class StandardExporter implements LineExporter {

    String startQuote = null, endQuote = null, columnSeparator = null, escapeSequence = null;
    java.util.List<String> substringsToEscape = new java.util.LinkedList<String>();
    hu.sztaki.ilab.giraffe.schema.dataformat.StreamFormat format;

    private void addSpecialString(String str) {
        if (str != null) substringsToEscape.add(str);
    }

    public java.util.List<String> getColumns() {
        java.util.List<String> ret = new java.util.LinkedList<String>();
        for (StringColumn col: this.format.getFields().getColumn()) ret.add(col.getName());
        return ret;
    }

    public StandardExporter(hu.sztaki.ilab.giraffe.schema.dataformat.StreamFormat format) {
        this.format = format;
        for (Quotes q : format.getDefaultQuotes()) {
            addSpecialString(q.getEnd());
            addSpecialString(q.getStart());
            addSpecialString(q.getQuotes());
        }
        addSpecialString(format.getEscape());
        addSpecialString(format.getSeparator());
    }

    public String export(Object[] fields) {
        java.io.StringWriter exportStream = new java.io.StringWriter();
        boolean firstColumn = true;
        for (Object fieldObject : fields) {
            String currentField = fieldObject.toString();
            if (!firstColumn) {
                // add column separator
                exportStream.write(columnSeparator);
            }
            firstColumn = false;
            if (null != startQuote) {
                exportStream.write(startQuote);
            }
            if (null != escapeSequence) {
                currentField = StringUtils.escapeSpecialSequences(currentField, this.substringsToEscape, escapeSequence);
                exportStream.write(currentField);
            } else {
                exportStream.write(currentField);
            }
            if (null != endQuote) {
                exportStream.write(endQuote);
            }
        }
        return exportStream.toString();
    }

    public boolean init() {
        // STUB
        return true;
    }
}
