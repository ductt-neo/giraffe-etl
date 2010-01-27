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
package hu.sztaki.ilab.giraffe.dataprocessors.url;

/**
 *
 * @author neumark
 */
import hu.sztaki.ilab.giraffe.dataprocessors.implementation.URLComponents;
import hu.sztaki.ilab.giraffe.core.database.LookupTable;
import hu.sztaki.ilab.giraffe.core.io.lineimporters.TokenizerImporter;
import org.apache.log4j.*;
import hu.sztaki.ilab.giraffe.core.dataprocessors.*;
import hu.sztaki.ilab.giraffe.core.util.Pair;
import hu.sztaki.ilab.info.schema.logformats.Column;
import java.net.MalformedURLException;
import java.util.*;
import org.xbill.DNS.Tokenizer;

/**
 * HTTP GET expectes input of the form:
 * hostname, HTTPMETHOD DOCUMENT [PROTOCOL], eg:
 * www.aegon.hu,GET / HTTP/1.1
 * @author neumark
 */
public class HttpGet extends ProcessingEngine {

    private static Logger logger = Logger.getLogger(HttpGet.class);
    public static java.util.List<String> fl = new java.util.LinkedList<String>();
    private static TokenizerImporter requestImporter = new TokenizerImporter();

    static {
        fl.add("httprequest");
        fl.add("document");
        fl.add("protocol");
        URLComponents.extendFieldList(fl);

        requestImporter.setColumns(Arrays.asList(new Column[]{newColumn("hostname"), newColumn("request"), newColumn("document"), newColumn("protocol")}));
        requestImporter.addColumnDelimiter("hostname", ",");
        requestImporter.addColumnDelimiter("request", " ");
        requestImporter.addColumnDelimiter("document", " ");
        if (!requestImporter.init()) {
            logger.error("HttpGet: Could not init tokenizer!");
            assert (false);
        }
    }

    private static Column newColumn(String columnName) {
        Column col = new Column();
        col.setName(columnName);
        return col;
    }

    @Override
    public java.util.List<String> getFieldList() {
        return fl;
    }

    public HttpGet(LookupTable lookupTable) {
        super(lookupTable);
    }

    public HttpGet(LookupTable lookupTable, int threadCount) {
        super(lookupTable, threadCount);
    }

    protected class HttpGetRunnable extends ProcessingEngine.ProcessingRunnable {

        @Override
        public void performTask(String getRequest) {
            ProcessedData ret = new ProcessedData();
            saveparts(ret, getRequest);
            writeResult(getRequest, ret);
        }
    }

    @Override
    protected ProcessingEngine.ProcessingRunnable getRunnable() {
        return new HttpGetRunnable();
    }

    private String getNextWord(Iterator<List<Pair<TokenizerImporter.TokenType, String>>> it) {
        if (!it.hasNext()) {return null;}
        List<Pair<TokenizerImporter.TokenType, String>> currentColumn = it.next();
        if (currentColumn.size() < 1 || currentColumn.get(0).first != TokenizerImporter.TokenType.WORD) { return null;}
        return currentColumn.get(0).second;
    }

    private void saveparts(ProcessedData ret, String req) {
        ret.setState(ProcessedData.State.FAILURE);
        // FAILURE will be replaced by OK if all goes well.
        List<List<Pair<TokenizerImporter.TokenType, String>>> decomposedRequest = null;
        try {
            decomposedRequest = requestImporter.tokenize(req);
        } catch (java.text.ParseException ex) {
            logger.error("Error parsing Get request", ex);
            return;
        }
        Iterator<List<Pair<TokenizerImporter.TokenType, String>>> columnIt = decomposedRequest.iterator();
        
        String hostname = getNextWord(columnIt);
        String method = getNextWord(columnIt);
        String document = getNextWord(columnIt);
        String protocol = getNextWord(columnIt);
        if (hostname == null || method == null || document == null) {
            return;
        }
        ret.setState(ProcessedData.State.OK);
        ret.setField("protocol", protocol);
        ret.setField("httprequest", method);
        ret.setField("document", document);
        // convert document path to URL
        String url = "http://"+hostname+document;
        try {
        URLComponents c = new URLComponents(url);
        c.save(ret);
        } catch (java.net.MalformedURLException ex ) {
            logger.debug("error parsing URL "+url,ex);
        }
    }
}
