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
package hu.sztaki.ilab.giraffe.dataprocessors.implementation;

import hu.sztaki.ilab.info.schema.logformats.Column;
import hu.sztaki.ilab.giraffe.core.io.lineimporters.TokenizerImporter;
import java.util.*;
import hu.sztaki.ilab.giraffe.core.util.*;
import hu.sztaki.ilab.giraffe.core.dataprocessors.ProcessedData;
import hu.sztaki.ilab.giraffe.core.io.lineimporters.TokenizerImporter.TokenType;
import java.text.ParseException;

/**
 *
 * @author neumark
 */
public class URLComponents {

    /* data representation:  if input is http://www.example.com/display?id=123&fakefoo=fakebar
     * then output will be:
     * schema = http, host = {www,example,com}, path = {display}, getparameters = {(id,123),(fakefoo,fakebar)}
     */
    static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(URLComponents.class);
    private final static String schemaDelimiter = "://";
    private final static String hostnameSeparator = ".";
    private final static String pathDelimiter = "?";
    private final static String pathSeparator = "/";
    private final static String paramDelimiter = "#";
    private final static String paramSeparator = "&";
    private final static String valueSeparator = "=";
    private final static String hostnameDelimiterIfPortGiven = ":";
    private final static java.util.Set<String> indexFiles = new java.util.HashSet<String>();
    // URL data:
    // Initialized to default values:
    private String schema = "http";
    private java.util.List<String> hostname = new java.util.LinkedList<String>();
    private java.util.List<String> path = new java.util.LinkedList<String>();
    private int portNumber = 0;
    private boolean pathIsDir = false;
    private List<Pair<String, String>> getParameters = new java.util.LinkedList<Pair<String, String>>();
    private String fragment = "";
    private static TokenizerImporter rawURLImporter = new TokenizerImporter();

    static {
        indexFiles.addAll(Arrays.asList(new String[]{"index.html", "index.htm", "index.php", "default.asp"}));
        java.util.List<Column> urlColumns = new java.util.LinkedList<Column>();
        urlColumns.add(newColumn("schema"));
        urlColumns.add(newColumn("hostname_port"));
        urlColumns.add(newColumn("path"));
        urlColumns.add(newColumn("params"));
        urlColumns.add(newColumn("fragment"));
        rawURLImporter.setColumns(urlColumns);
        // Schema
        rawURLImporter.addColumnDelimiter("schema", schemaDelimiter);
        // hostname_port
        rawURLImporter.addColumnDelimiter("hostname_port", pathSeparator);
        rawURLImporter.addTokenSeparator("hostname_port", hostnameSeparator);
        rawURLImporter.addTokenSeparator("hostname_port", hostnameDelimiterIfPortGiven);
        // path
        rawURLImporter.addColumnDelimiter("path", pathDelimiter);
        rawURLImporter.addTokenSeparator("path", pathSeparator);
        // params_fragment
        rawURLImporter.addColumnDelimiter("params", paramDelimiter);
        rawURLImporter.addTokenSeparator("params", paramSeparator);
        // fragment has no options
        if (!rawURLImporter.init()) {
            logger.error("URLComponents: Could not init tokenizer!");
            assert (false);
        }
    }

    private static Column newColumn(String columnName) {
        Column col = new Column();
        col.setName(columnName);
        return col;
    }

    public static void extendFieldList(java.util.List<String> fl) {
        fl.add("schema");
        fl.add("host");
        fl.add("port");
        fl.add("path");
        fl.add("isdir");
        fl.add("parameters");
        fl.add("fragment");
        fl.add("fullurl");
    }

    private boolean decompose(String rawURL) {
        List<List<Pair<TokenizerImporter.TokenType, String>>> decomposedURL = null;
        try {
            decomposedURL = rawURLImporter.tokenize(rawURL);
        } catch (java.text.ParseException ex) {
            logger.error("Error parsing URL", ex);
            return false;
        }
        Iterator<List<Pair<TokenizerImporter.TokenType, String>>> columnIt = decomposedURL.iterator();
        List<Pair<TokenizerImporter.TokenType, String>> currentColumn = null;
        // --- save schema ---
        if (!columnIt.hasNext()) {
            return false;
        }
        currentColumn = columnIt.next();
        if (currentColumn.size() < 1 || currentColumn.get(0).first != TokenizerImporter.TokenType.WORD) {
            return false; // no schema!
        }
        schema = currentColumn.get(0).second;
        // --- save hostname ---
        if (!columnIt.hasNext()) {
            return false; // hostname is mandatory
        }
        currentColumn = columnIt.next();
        Pair<TokenizerImporter.TokenType, String> pair = null;
        Iterator<Pair<TokenizerImporter.TokenType, String>> hostnameIt = currentColumn.iterator();
        boolean nextWordIsPortNumber = false;
        while (hostnameIt.hasNext()) {
            pair = hostnameIt.next();
            // this should be a word
            if (pair.first != TokenizerImporter.TokenType.WORD) {
                return false;
            }
            if (nextWordIsPortNumber) {
                nextWordIsPortNumber = false;
                this.portNumber = Integer.parseInt(pair.second);
            } else {
                if (portNumber > 0) {
                    logger.debug("No hostname component may come after port number!");
                    return false;
                }
                hostname.add(pair.second);
            }
            // The next token should be a delimiter
            if (!hostnameIt.hasNext()) {
                break;
            }
            pair = hostnameIt.next();
            if (pair.first == TokenizerImporter.TokenType.TOKENSEPARATOR &&
                    pair.second.equals(hostnameDelimiterIfPortGiven)) {
                nextWordIsPortNumber = true;
                if (portNumber > 0) {
                    logger.debug("Only a single port number may be given per URL!");
                    return false;
                }
            }

        }
        // --- save path ---
        if (columnIt.hasNext()) {
            currentColumn = columnIt.next();
            for (Pair<TokenizerImporter.TokenType, String> pathIt : currentColumn) {
                if (pathIt.first == TokenizerImporter.TokenType.WORD) this.path.add(pathIt.second);
            }
        }
        // --- save get parameters ---
        if (columnIt.hasNext()) {
            currentColumn = columnIt.next();
            for (Pair<TokenizerImporter.TokenType, String> pathIt : currentColumn) {
                if (pathIt.first == TokenizerImporter.TokenType.WORD) {
                    // attempt to split the "var=value" structure into "var" and "value" parts
                    String[] parts = pathIt.second.split("=");
                    if (parts.length == 2) {
                        this.getParameters.add(new Pair<String,String>(parts[0], parts[1]));
                    } else {this.getParameters.add(new Pair<String,String>(parts[0], null));}
                }
            }
        }
        // --- save document fragment ---
        if (columnIt.hasNext()) {
            currentColumn = columnIt.next();
            if (currentColumn.size() == 1 && currentColumn.get(0).first == TokenizerImporter.TokenType.WORD) {
                this.fragment = currentColumn.get(0).second;
            } else {
                logger.debug("Error parsing document fragment");
                return false;
            }
        }
        if (this.portNumber == 0) portNumber = 80;
        return true;
    }

    public URLComponents(String rawURL) throws java.net.MalformedURLException {
        if (!decompose(rawURL)) {
            throw new java.net.MalformedURLException();
        }
    }

    public URLComponents(String schema, List<String> hostname, int portNumber, List<String> path, boolean pathIsDir, List<Pair<String, String>> getParameters, String fragment) {

        this.schema = schema;
        this.hostname = hostname;
        this.portNumber = portNumber;
        this.path = path;
        this.pathIsDir = pathIsDir;
        this.getParameters = getParameters;
        this.fragment = fragment;

    }

    public void save(ProcessedData pd) {

        pd.setField("schema", schema);
        pd.setField("host", StringUtils.printContainer(hostname.iterator(), hostnameSeparator));
        pd.setField("port", portNumber + "");
        pd.setField("path", StringUtils.printContainer(path.iterator(), pathSeparator));
        pd.setField("isdir", new Boolean(pathIsDir).toString());
        String params = "";
        for (Pair<String, String> var : getParameters) {
            if (params.length() > 0) {
                params += paramSeparator;
            }
            params += var.first;
            if (null != var.second) {
                params += valueSeparator + var.second;
            }
        }
        pd.setField("parameters", params);
        pd.setField("fragment", fragment);
        pd.setField("fullurl", this.toString());

    }

    public String toString() {
        String params = "";
        for (Pair<String, String> var : getParameters) {
            if (params.length() > 0) {
                params += paramSeparator;
            }
            params += var.first;
            if (null != var.second) {
                params += valueSeparator + var.second;
            }
        }
        return schema + schemaDelimiter +
                StringUtils.printContainer(hostname.iterator(), hostnameSeparator) +
                ((portNumber != 80) ? hostnameDelimiterIfPortGiven + portNumber : "") +
                ((path.size() > 0) ? pathSeparator : "") +
                StringUtils.printContainer(path.iterator(), pathSeparator) +
                ((pathIsDir) ? pathSeparator : "") +
                ((params.length() > 0 || fragment.length()>0)? pathDelimiter:"")+
                params +
                ((fragment.length() > 0) ? paramDelimiter : "") +
                fragment;
    }

    public void normalize() {
        /*
         *  URL normalization rules form wikipedia: http://en.wikipedia.org/wiki/URL_normalization
         *
         *  * Converting the scheme and host to lower case. The scheme and host components of the URL are case-insensitive. Most normalizers will convert them to lowercase. Example:
         *         HTTP://www.Example.com/ → http://www.example.com/
         */
        String newSchema = schema.toLowerCase();
        List<String> newHostname = new LinkedList<String>();
        for (String hostPart : hostname) {
            newHostname.add(hostPart.toLowerCase());
        }
        hostname = newHostname;
        /*
         * Removing directory index. Default directory indexes are generally not needed in URLs. Examples:
        http://www.example.com/default.asp → http://www.example.com/
        http://www.example.com/a/index.html → http://www.example.com/a/
         */
        if (path.size() > 0 && indexFiles.contains(path.get(path.size()-1))) {
            path.remove(path.size()-1);
            pathIsDir=true;
        }
        
        if (path.size() == 0) pathIsDir = true;
        /*
         * Converting the entire URL to lower case. Some web servers that run on top of case-insensitive file systems allow URLs to be case-insensitive. URLs from a case-insensitive web server may be converted to lowercase to avoid ambiguity. Example:
        BAD IDEA ON LINUX!!!
        http://www.example.com/BAR.html → http://www.example.com/bar.html

         * Capitalizing letters in escape sequences. All letters within a percent-encoding triplet (e.g., "%3A") are case-insensitive, and should be capitalized. Example:

        http://www.example.com/a%c2%b1b → http://www.example.com/a%C2%B1b

         * Removing the fragment. The fragment component of a URL is usually removed. Example:

        http://www.example.com/bar.html#section1 → http://www.example.com/bar.html

         * Removing the default port. The default port (port 80 for the “http” scheme) may be removed from (or added to) a URL. Example:

         */
        //String newPort = port;
        //if (port != null && port.equals("80")) newPort = null;
    /*
        http://www.example.com:80/bar.html → http://www.example.com/bar.html

         * Removing dot-segments. The segments “..” and “.” are usually removed from a URL according to the algorithm described in RFC 3986 (or a similar algorithm). Example:

        http://www.example.com/../a/b/../c/./d.html → http://www.example.com/a/c/d.html
         */
        List<String> newPath = new LinkedList<String>();
        for (String pathSegment : this.path) {
            if (pathSegment.equals(".")) {
                continue;
            }
            if (pathSegment.equals("..")) {
                if (newPath.size() > 0) {
                    newPath.remove(newPath.size() - 1);
                }
                continue;
            }
            newPath.add(pathSegment);
        }
        path = newPath;

        /*
         * Removing “www” as the first domain label. Some websites operate in two Internet domains: one whose least significant label is “www” and another whose name is the result of omitting the least significant label from the name of the first. 
         For example, http://example.com/ and http://www.example.com/ may access the same website. Although many websites redirect the user to the non-www address (or vice versa), some do not.
         A normalizer may perform extra processing to determine if there is a non-www equivalent and then normalize all URLs to the non-www prefix. Example:

        http://www.example.com/ → http://example.com/
         */
        if (newHostname.size() > 0 && newHostname.get(0).equals("www")) {
            newHostname.remove(0);
        }
        /*

         * Sorting the variables of active pages. Some active web pages have more than one variable in the URL. A normalizer can remove all the variables with their data, sort them into alphabetical order (by variable name), and reassemble the URL. Example:

         http://www.example.com/display?lang=en&article=fred → http://www.example.com/display?article=fred&lang=en

         */
        Collections.sort(this.getParameters, new java.util.Comparator<Pair<String,String>>() {
            public int compare(Pair<String,String> o1, Pair<String,String> o2) {
                return o1.first.compareTo(o2.first);
            }
        });
        /*
         * Removing arbitrary querystring variables. An active page may expect certain variables to appear in the querystring; all unexpected variables should be removed. Example:

        http://www.example.com/display?id=123&fakefoo=fakebar → http://www.example.com/display?id=123

         * Removing default querystring variables. A default value in the querystring will render identically whether it is there or not. When a default value appears in the querystring, it should be removed. Example:

        http://www.example.com/display?id=&sort=ascending → http://www.example.com/display

         * Removing the "?" when the querystring is empty. When the querystring is empty, there is no need for the "?". Example:

        http://www.example.com/display? → http://www.example.com/display
         */
        //return new URLComponents(newSchema,newHostname, newPort, newPath, newPathIsDir, getParameters,fragment);        
    }

    public static void main(String[] args) {
        //http://www.example.com/../a/b/../c/./d.html → http://www.example.com/a/c/d.html
        String original = "http://thome.hu:8080/root.go?appid=tsearch&tsearchRT=http%3A%2F%2Ftest.telekom.t-online.private%2Fsajtoszoba%2Fsajtohirek%2F2003%2Ffebruar_18&tsearchRF=telefon#top";
        //String original = "http://ewarthog.org";
        System.out.println(original);
        try {
            URLComponents uc = new URLComponents(original);
            uc.normalize();            
            System.out.println(uc.toString());
            ProcessedData pd = new ProcessedData();
            uc.save(pd);
            System.out.println(pd.serialize());
        } catch (Exception e) {
            System.out.println("Exception caught!");
        }
        //System.out.println(uc.toXML());
    }
}
