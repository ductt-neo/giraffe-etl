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

import hu.sztaki.ilab.giraffe.core.factories.TerminalFactory;
import hu.sztaki.ilab.giraffe.core.util.Pair;
import hu.sztaki.ilab.giraffe.core.util.StringUtils;
//import hu.sztaki.ilab.giraffe.schema.dataformat.StreamFormat.Columns.Column;
import hu.sztaki.ilab.giraffe.schema.dataformat.Quotes;
import hu.sztaki.ilab.giraffe.schema.dataformat.StreamFormat.ColumnFormatting.Column;
import hu.sztaki.ilab.giraffe.schema.dataformat.StringColumn;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.apache.log4j.Logger;

/**
 *
 * @author neumark
 */
public class TokenizerImporter implements LineImporter {

    /* The Tokenizer class parses a string into a set of columns, with each column
     * composed of one or more "tokens". In order to implement the LineParser interface,
     * Tokenizer provides parse(), which concatenates the tokens that compose a column.
     * Use Tokenizer instead of SplitParser any of the following is true:
     * - several column delimiters are used within the record
     * - some columns are enclosed in quotes
     * - columns may include the column separator string or some other special symbol (such as a quote)
     * Tokenizer may be of use even if these conditions do not hold, because to provides an interface to access the
     * tokens which compose a column (this is not available with SplitParser).
     */
    // Tokenizer splits the input string into several pieces (tokens), which
    // all have one of the following types:
    public enum TokenType {

        WORD, // The actual token values. In the URL example below, this would be something like "www" or "example" or "cgi-scripts".
        TOKENSEPARATOR, // This is a sequence of characters which separates tokens eg: "/" between path components
        QUOTE, // Not used in the URL example.
        COLUMNDELIMITER, // The character sequence which marks the end of a column.
        ESCAPE
    };
    private static final Logger logger = Logger.getLogger(TokenizerImporter.class);
    private Vector<ColumnSpecification> columnSpecifications;
    private ColumnSpecification defaultColumn = new ColumnSpecification();
//    private java.util.List<Column> columns = null;
    // The following structure is used by tokenizer.
    // Each column is associated with an Action, which forms the root of the tree.
    private java.util.Vector<Action> columnActionTree = null;
    private hu.sztaki.ilab.giraffe.schema.dataformat.StreamFormat format;

    // Action is used to construct a tree of possible actions based on the previously encountered characters.
    private class Action {

        public Action() {
        }
        // Choices represents further possibilites based on the character freshly encountered.
        // If the value associated with a given key is null, then tokenType contains the appropriate response.
        public Map<Integer, Action> choices = new HashMap<Integer, Action>();
        public TokenType tokenType = null;
    }


    /* ColumnSpecification constains all the information used by the tokenizer
     * to split the incoming string into a sequence of columns. One column corresponds to
     * a single ColumnSpecification object. Each column is made up of an
     * arbitrary number of tokens, which are separated by tokenSeparators.
     * The column may optionally be enclosed in quotes. Columns are separated
     * by endMark characters, which mark the end of a column (and the beginning of the next).
     * Finally, escape sequences allow the column to contain quotes or endMark characters as values.
     * It is important to note that this class assumes a constant number of columns. Therefore
     * optional columns should be lumped together with columns that are always present.
     * Using a URL as an example:
     *   http://www.example.com:8080/www/cgi-scripts/doit.cgi?docid=5&name=john#top
     * We would like to parse the URL into the following columns:
     *  protocol, hostname, port, path, get parameters, fragment
     * Because the port, get parameters, and fragment fields are optional,
     * we use the
     *  protocol, hostname and port, path and get parameters and fragment
     * columns.

     * column: protocol
     * endMark: :// depending on whether the port is given
     *
     * column: hostname and port
     * tokenSeparator: . and :
     * endMark: /
     *
     * column: path, get parameters, and fragments
     * tokenSeparator: / and ? and & and =
     *
     */
    private class ColumnSpecification {
        // There can be several of these of each column, hence the List of Strings.

        public String columnName;
        public List<Pair<String, String>> quotes = new LinkedList<Pair<String, String>>(); // Quotes enclosing column, may be null, meaning no quotes are associated with the column.
        public List<String> columnDelimiters = new LinkedList<String>(); // Character which marks the end of column if encountered.
        public List<String> escapeSequences = new LinkedList<String>(); // A sequence of characters which mean the next endMark or quote should be interpretted literally.
        public List<String> tokenSeparators = new LinkedList<String>(); // This sequence separates the tokens from each other within the column.

        public ColumnSpecification clone() {
            ColumnSpecification ret = new ColumnSpecification();
            ret.columnName = this.columnName;
            ret.quotes.addAll(quotes);
            ret.columnDelimiters.addAll(columnDelimiters);
            ret.escapeSequences.addAll(escapeSequences);
            ret.tokenSeparators.addAll(tokenSeparators);
            return ret;
        }

        ColumnSpecification() {
        }
    }

    private Action newInnerNodeAction() {
        Action a = new Action();
        a.choices = new HashMap<Integer, Action>();
        return a;
    }

    public java.util.List<String> getColumns() {
        java.util.List<String> columnData = new java.util.LinkedList<String>();
        for (ColumnSpecification colSpec : this.columnSpecifications) {
            columnData.add(colSpec.columnName);
        }
        return columnData;
    }

    private void addAction(Action rootAction, String trigger, TokenType tokenType) throws java.text.ParseException {
        Action currentAction = rootAction;
        for (int i = 0; i < trigger.length(); i++) {
            Integer currentCodePoint = new Integer(trigger.codePointAt(i));
            if (null == currentAction.choices) {
                throw new java.text.ParseException("Attempting to assign a child to a root node! Trigger string: " + trigger, 0);
            }
            if (!currentAction.choices.containsKey(currentCodePoint)) {
                currentAction.choices.put(currentCodePoint, newInnerNodeAction());
            }
            currentAction = currentAction.choices.get(currentCodePoint);
        }
        if (null == currentAction.choices && currentAction.tokenType != tokenType) {
            throw new java.text.ParseException("Sequence '" + trigger + "' already registered with different token type.", 0);
        }
        if (null != currentAction.choices && currentAction.choices.size() > 0) {
            throw new java.text.ParseException("Arrived at a node which should be a leaf, but has children. Trigger string: " + trigger, 0);
        }
        currentAction.choices = null;
        currentAction.tokenType = tokenType;
    }

    private void setupActionTrees() throws java.text.ParseException {
        // SetupActionTrees creates an action tree for each column
        //columnActionTree = new Vector<Action>(columns.size());
        columnActionTree = new java.util.Vector<Action>(columnSpecifications.size());
        for (ColumnSpecification colSpec : columnSpecifications) {
            Action rootAction = newInnerNodeAction();
            // Construct action trees:
            for (String endMark : colSpec.columnDelimiters) {
                addAction(rootAction, endMark, TokenType.COLUMNDELIMITER);
            }
            for (String esc : colSpec.escapeSequences) {
                addAction(rootAction, esc, TokenType.ESCAPE);
            }
            for (String tokenSeparator : colSpec.tokenSeparators) {
                addAction(rootAction, tokenSeparator, TokenType.TOKENSEPARATOR);
            }
            for (Pair<String, String> quote : colSpec.quotes) {
                addAction(rootAction, quote.first, TokenType.QUOTE);
                addAction(rootAction, quote.second, TokenType.QUOTE);
            }
            columnActionTree.add(rootAction);
        }
    }

    private class LineTokenizer {

        // Contains the raw string which must be split into tokens.
        private String line;
        // The outer list represents the columns, while the
        // inner list represents tokens belonging to the given column.
        private List<List<Pair<TokenType, String>>> columnTokenList = new LinkedList<List<Pair<TokenType, String>>>();
        private List<Pair<TokenType, String>> currentColumnTokens = null;
        // currentToken contains what has been collected of the token so far.
        private String currentWordToken = "";
        // potentialSpecialSequence is non-empty of we are currently in a special sequence
        private String currentSpecialSequence = "";
        // The index of the column currently being read
        private int currentColumnIndex = 0;
        // If an opening quote has been encountered at the beginning of the column,
        // then the associated closing quote is contained in waitingForEndQuote.
        private String waitingForEndQuote = "";
        // If the tokenizer just encountered an escape sequence, then
        // mark the position from which the string should be interpretted literally.
        // By convention, a value of -1 means no escape sequence has been encountered.
        private int escapedCharacterAt = -1;
        private ColumnSpecification columnSpecification = null;
        private Action columnAction = null;
        private int characterPosition;
        private Action currentAction = null;

        public LineTokenizer(String line) {
            this.line = line;
        }

        private void startColumn(int columnIndex) {
            // save the current column
            if (null != currentColumnTokens) {
                columnTokenList.add(currentColumnTokens);
            }
            currentColumnTokens = new LinkedList<Pair<TokenType, String>>();
            // reset state
            currentSpecialSequence = "";
            waitingForEndQuote = "";
            columnSpecification = columnSpecifications.get(columnIndex);
            columnAction = columnActionTree.get(currentColumnIndex);
        }

        private void saveWordToken() {
            if (currentWordToken.length() > 0) {
                currentColumnTokens.add(new Pair<TokenType, String>(TokenType.WORD, currentWordToken));
                currentWordToken = "";
            }
        }

        private void specialSequence(TokenType sequenceType) throws java.text.ParseException {
            // If we are in a quoted field, then all special sequences should be interpretted literally except escape and quote.
            if (this.waitingForEndQuote.length() > 0 && !(sequenceType == TokenType.ESCAPE || sequenceType == TokenType.QUOTE)) {
                currentWordToken += currentSpecialSequence;
            } else {
                switch (sequenceType) {
                    case COLUMNDELIMITER:
                        // Before the special sequence is saved, the preceding word token must be saved.
                        saveWordToken();
                        // Save special sequence.
                        currentColumnTokens.add(new Pair<TokenType, String>(sequenceType, currentSpecialSequence));
                        // If an endmark is encounterd, then there are two possibilities:
                        // 1. We still need a quote character (which would cause a ParseException.)
                        // 2. The column has ended successfull, start the next one.
                        if (waitingForEndQuote.length() == 0) {
                            currentColumnIndex++;
                            startColumn(currentColumnIndex);
                        } else {
                            throwParseEx("Endmark for column encountered before closing quotes.");
                        }
                        break;
                    case TOKENSEPARATOR:
                        // Before the special sequence is saved, the preceding word token must be saved.
                        saveWordToken();
                        // Save special sequence.
                        currentColumnTokens.add(new Pair<TokenType, String>(sequenceType, currentSpecialSequence));
                        // Save current token and empty currentToken
                        if (currentWordToken.length() > 0) {
                            currentColumnTokens.add(new Pair<TokenType, String>(TokenType.WORD, currentWordToken));
                            currentWordToken = "";
                        }
                        break;
                    case QUOTE:
                        // Before the special sequence is saved, the preceding word token must be saved.
                        saveWordToken();
                        // Save special sequence.
                        currentColumnTokens.add(new Pair<TokenType, String>(sequenceType, currentSpecialSequence));
                        // This can either be a starting quote or an ending quote.
                        // starting quotes:
                        if (waitingForEndQuote.length() == 0) {
                            if (currentWordToken.length() > 0) {
                                throwParseEx("Opening quote found within column unescaped.");
                            }
                            // else register ending quote to wait for:
                            for (Pair<String, String> quotes : columnSpecification.quotes) {
                                if (quotes.first.equals(currentSpecialSequence)) {
                                    waitingForEndQuote = quotes.second;
                                }
                            }
                            if (waitingForEndQuote.length() < 1) {
                                throwParseEx("Unknown start quote char sequence for column.");
                            }
                        } else {
                            if (currentSpecialSequence.equals(waitingForEndQuote)) {
                                waitingForEndQuote = "";
                            } else {
                                throwParseEx("Excpecting ending quote '" + waitingForEndQuote + "' received '" + currentSpecialSequence + "' instead.");
                            }
                        }
                        break;
                    case ESCAPE:
                        // don't save the escape sequence!
                        escapedCharacterAt = characterPosition + 1;
                        break;
                    // The remaining cases are error conditions!
                    case WORD:
                    default:
                        // This should never happen!
                        assert (false);
                }
            }
            currentSpecialSequence = "";
            currentAction = null;
        }

        private void throwParseEx(String errmsg) throws java.text.ParseException {
            throw new java.text.ParseException("Error parsing column " + currentColumnIndex + ": " + errmsg + "; Line: '" + (line + 1) + /* Humans prefer counting from 1. */ "'.", 0);
        }

        public List<List<Pair<TokenType, String>>> tokenize() throws java.text.ParseException {
            if (null == line) {
                return null;
            }
            startColumn(currentColumnIndex);
            // Move through the entire string:            
            for (characterPosition = 0; characterPosition < line.length(); characterPosition++) {
                // If we are already in a special sequence, attempt to continue the sequence.
                Integer currentCharacterCode = new Integer(line.codePointAt(characterPosition));
                if (null != currentAction) {
                    // If the current character is the continuation of an already started sequence, then store the
                    // new character, increment our position in the sequence, and continue with the next character.
                    if (currentAction.choices.containsKey(currentCharacterCode)) {
                        currentSpecialSequence += line.charAt(characterPosition);
                        currentAction = currentAction.choices.get(currentCharacterCode);
                        // Check if we have completed the special sequence:
                        if (null == currentAction.choices) {
                            // leaf node reached: end of special sequence
                            specialSequence(currentAction.tokenType);
                        }
                        continue;
                    } else {
                        // If we have started a special sequence, but the current character is not the next expected
                        // character in the sequence, then we add the previously collected characters to our word token.
                        currentWordToken += currentSpecialSequence;
                        currentSpecialSequence = "";
                        currentAction = null;
                    }
                }
                // We attempt to find a special sequence starting with the current character
                if (characterPosition != escapedCharacterAt && columnAction.choices.containsKey(currentCharacterCode)) {
                    currentSpecialSequence += line.charAt(characterPosition);
                    currentAction = columnAction.choices.get(currentCharacterCode);
                    // Check if we have completed the special sequence:
                    if (null == currentAction.choices) {
                        // leaf node reached: end of special sequence
                        specialSequence(currentAction.tokenType);
                    }
                    continue;
                } else {
                    // This is not the beginning of a special sequence, it is a simple character withing a word token
                    currentWordToken += line.charAt(characterPosition);
                }
            }
            // save last field
            saveWordToken();
            if (!currentColumnTokens.isEmpty()) {
                columnTokenList.add(currentColumnTokens);
            }
            return columnTokenList;
        }
    }

    public List<List<Pair<TokenType, String>>> tokenize(String s) throws java.text.ParseException {
        LineTokenizer tokenizer = new LineTokenizer(s);
        return tokenizer.tokenize();
    }

    public List<String> parse(String s) throws java.text.ParseException {
        List<List<Pair<TokenType, String>>> tokens = tokenize(s);
        List<String> columns = new LinkedList<String>();
        for (List<Pair<TokenType, String>> columnTokens : tokens) {
            String column = "";
            for (Pair<TokenType, String> currentToken : columnTokens) {
                if (currentToken.first == TokenType.WORD || currentToken.first == TokenType.TOKENSEPARATOR) {
                    column = column + currentToken.second;
                }
            }
            columns.add(column);
        }
        return columns;
    }

    public TokenizerImporter(hu.sztaki.ilab.giraffe.schema.dataformat.StreamFormat format) {
        this.format = format;
    }

    public boolean init() {
        // First set up defaults for all columns.
        for (Quotes q : format.getDefaultQuotes()) {
            defaultColumn.quotes.add(TerminalFactory.quoteConverter(q));
        }
        if (format.getEscape() != null) {
            defaultColumn.escapeSequences.add(format.getEscape());
        }
        if (format.getSeparator() != null) {
            defaultColumn.columnDelimiters.add(format.getSeparator());
        }
        // Add all columns
        java.util.Map<String, ColumnSpecification> colMap = new java.util.HashMap<String, ColumnSpecification>();
        this.columnSpecifications = new java.util.Vector<ColumnSpecification>(format.getFields().getColumn().size());
        for (StringColumn col : format.getFields().getColumn()) {
            ColumnSpecification currentSpec = this.defaultColumn.clone();
            currentSpec.columnName = col.getName();
            this.columnSpecifications.add(currentSpec);
            colMap.put(col.getName(), currentSpec);
        }
        // Add extra formatting for columns which specify it:
        for (Column col : format.getColumnFormatting().getColumn()) {
            ColumnSpecification colSpec = null;
            colSpec = colMap.get(col.getColumnRef());
            if (null == colSpec) {
                logger.error("No such column: " + col.getColumnRef());
                return false;
            }
            if (col.getColumnDelimiter().size() > 0) {
                // drop default column delimiter if column explicitly lists delimiters.
                colSpec.columnDelimiters.clear();
                colSpec.columnDelimiters.addAll(col.getColumnDelimiter());
            }
            if (col.getEscape().size() > 0) {
                colSpec.escapeSequences.clear();
                colSpec.escapeSequences.addAll(col.getEscape());
            }
            if (col.getQuotes().size() > 0) {
                colSpec.quotes.clear();
                for(Quotes q : col.getQuotes()) colSpec.quotes.add(TerminalFactory.quoteConverter(q));
            }
        }
        try {
            setupActionTrees();
        } catch (java.text.ParseException ex) {
            logger.error("Error parsing tokenizer specification.", ex);
            return false;
        }
        return true;
    }
}
