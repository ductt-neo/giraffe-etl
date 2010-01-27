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
public class StringUtils {

    // StringUtils.nullify makes sure all deformed string objects are null.
    public static String addslashes(String s) {
        // TODO(neumark): add handle more characters!
        if (null == s) {
            return s;
        }
        if (s.equals("|")) {
            return "\\" + s;
        }
        return s;
    }

    public static <T> String printContainer(Iterator<T> it, String sep) {
        String ret = "";
        while (it.hasNext()) {
            ret += it.next().toString();
            if (it.hasNext()) {
                ret += sep;
            }
        }
        return ret;
    }

    public static String xmlizeContainer(Iterator<String> it, String elem) {
        String ret = "";
        while (it.hasNext()) {
            ret += "<" + elem + ">" + it.next() + "</" + elem + ">";
        }
        return ret;
    }

    public static String firstPart(String fullName) {
        String ret = "";
        if (fullName == null) {
            return ret;
        }
        for (int i = 0; i < fullName.length(); i++) {
            if (Character.isLetter(fullName.charAt(i))) {
                ret = ret + fullName.charAt(i);
            } else {
                break;
            }
        }
        return ret;
    }

    public static String nullify(String x) {
        if (x == null) {
            return null;
        }
        x = x.trim();
        return (x.length() > 0) ? x : null;
    }

    public static String unnullify(String x) {
        return (x == null) ? "" : x;
    }

    public static String smartConcat(String prefix, String suffix) {
        if (prefix == null) {
            return suffix;
        }
        if (prefix.length() == 0 || (prefix.length() > 0 && prefix.charAt(prefix.length() - 1) == ' ')) {
            return prefix + suffix;
        }
        return prefix + " " + suffix;
    }

    public static String correctCase(String word) {
        if (word == null || word.length() < 1) {
            return word;
        }
        return word.substring(0, 1).toUpperCase() + word.substring(1).toLowerCase();
    }

    public static String arrayToString(String[] tokens) {
        String s = "[ ";
        for (int i = 0; i < tokens.length; i++) {
            s = StringUtils.smartConcat(s, tokens[i]);
        }
        return s + " ]";
    }

    public static boolean arrayMember(String[] array, String member) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] != null && array[i].equals(member)) {
                return true;
            }
        }
        return false;
    }

    public static boolean endsWith(String s, String ending) {
        if (ending == null) {
            return true;
        }
        if (s == null || s.length() < ending.length()) {
            return false;
        }
        return s.substring(s.length() - ending.length()).equals(ending);
    }

    public static boolean startsWith(String s, String start) {
        if (start == null) {
            return true;
        }
        if (s == null || s.length() < start.length()) {
            return false;
        }
        return s.substring(0, start.length()).equals(start);
    }

    public static int findPos(String s, char c) {
        if (s == null) {
            return -1;
        }
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == c) {
                return i;
            }
        }
        return -1;
    }

    public static String maybePrint(String label, String var) {
        return ((var == null || var.equals("UNKNOWN")) ? "" : label + ": " + var + "\n");
    }

    public static String maybePrint(Object var) {
        return ((var == null || var.equals("UNKNOWN")) ? "" : var.toString());
    }

    public static String maybePrint(String label, java.util.HashMap<String, String> var) {
        return ((var == null) ? "" : label + ": " + var + "\n");
    }

    public static String escapeSpecialSequences(String rawString, java.util.List<String> specialSequences, String escapeSequence) {
        // copied from: http://forums.devarticles.com/java-development-38/java-addslashes-5333.html
        if (rawString == null) {
            return rawString;
        }
        if (specialSequences.size() == 0) {
            return rawString;
        }
        if (escapeSequence == null) {
            escapeSequence = "";
        }

        int sequencePosition = 0;
        while(true) {
            int nextSequencePosition = rawString.length();
            int sequenceLength = 0;
            for (String seq : specialSequences) {
                int currentSeqIndex = rawString.indexOf(seq, sequencePosition);
                if (currentSeqIndex > 0 && currentSeqIndex < nextSequencePosition) {
                    nextSequencePosition = currentSeqIndex;
                    sequenceLength = seq.length();
                }
            }
            if (sequenceLength > 0) {
                sequencePosition = nextSequencePosition;
                rawString = rawString.substring(0, sequencePosition) + escapeSequence + rawString.substring(sequencePosition);
                sequencePosition += escapeSequence.length()+sequenceLength;
            } else {
                break;
            }
        }
        return rawString;
    }

    public static byte[] stringToIP(String ip) {
        String parts[] = ip.split("\\.");
        if (parts.length != 4) {
            return null;
        }
        byte[] b = new byte[4];
        try {
            for (int i = 0; i < 4; i++) {
                b[i] = new Integer(parts[i]).byteValue();
            }
        } catch (NumberFormatException e) {
            return null;
        }
        return b;
    }

    public static boolean isInteger(String input) {
        try {
            Integer.parseInt(input);
            return true;
        } catch (Exception e) {
            ;
        }
        return false;
    }

    public static String removeTrailingString(String input, String ending) {
        if (input == null || input.length() < ending.length() || !input.endsWith(ending)) {
            return input;
        }
        return input.substring(0, input.length() - ending.length());
    }

    public static void main(String[] args) {
        String s = "a|b\\c|d\\e";
        String expResult = "a_|b_\\c_|d_\\e|c";
        String result = StringUtils.escapeSpecialSequences(s, Arrays.asList(new String[]{"\\", "|c"}), "_o");
        System.out.println(result);
    }
}
