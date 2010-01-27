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
package hu.sztaki.ilab.giraffe.dataprocessors.useragent;

import hu.sztaki.ilab.giraffe.core.util.*;
import java.util.*;
import java.util.regex.*;
import hu.sztaki.ilab.giraffe.core.database.*;
import hu.sztaki.ilab.giraffe.core.dataprocessors.*;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *  A useragenteket leíró dokumentumok:
 *  Mozilla alapú böngészők: http://www.mozilla.org/build/revised-user-agent-strings.html
 *  MSIE: http://msdn2.microsoft.com/en-us/library/ms537503.aspx
 *  Safari: http://developer.apple.com/internet/safari/faq.html#anchor2
 * @author neumark
 */
public class UserAgent extends hu.sztaki.ilab.giraffe.core.dataprocessors.ProcessingEngine {

    public static Pattern email;
    static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(UserAgent.class);
    public static Vector<String> fl = new Vector<String>();


    static {
        email = Pattern.compile(".+@.+\\.[a-z]+");
        fl.add("agentName");
        fl.add("agentVersion");
        fl.add("agentLanguage");
        fl.add("agentOS");
        fl.add("agentOSVersion");
        fl.add("agentOSDistro");
        fl.add("layoutEngine");
        fl.add("layoutMode");
        fl.add("layoutResolution");
        fl.add("agentType");
        fl.add("agentDevice");
        fl.add("agentProcessor");
        fl.add("agentSWPlatform");
    }

    private enum ParserStates {

        NAME, VERSION, DETAILS, LANGUAGE, DONE
    };
    private HashSwitch<String, SwitchParameters, Boolean, IHashCase<SwitchParameters, Boolean>> dictionary;

    public Vector<String> getFieldList() {
        return fl;
    }
    Date dataSourceModified = null;

    private static boolean isIdentified(AgentInfo a) {
        return (a.getAgentName() != null);
    }

    public static void setLinux(SwitchParameters p) {
        WordLocation w = p.getLocation();
        if (w.getRecord() <= -1) {
            return; // Bad location
        }
        String distro, ver;
        AgentInfo agentid = p.getAgentId();
        Vector<AgentInfo> ad = p.getAgentDetails();
        if (w.getDetail() <= -1) {
            distro = w.nameAt(ad);
            ver = ad.get(w.getRecord()).getAgentVersion();
        } else {
            String fullname = w.detailAt(ad);
            distro = StringUtils.nullify(StringUtils.firstPart(fullname));
            ver = StringUtils.nullify(UserAgent.extractVersion(fullname.substring(distro.length())));
        }

        agentid.setAgentSWPlatform(AgentInfo.AgentSWPlatform.UNIX);
        agentid.setAgentOS("Linux");
        if (distro != null) {
            agentid.setAgentOSDistro(distro);
        }
        if (ver != null) {
            agentid.setAgentOSVersion(ver);
        }
    }

    public static void setBrowserDetails(SwitchParameters p) {
        /* Identifies browser based on the given AgentInfo tuple, assuming the
         * user agent string uses the conventional Name/Version [Language] ...
         * format.
         * @param AgentInfo ainfo - the record to use.
         */
        WordLocation w = p.getLocation();
        Vector<AgentInfo> ad = p.getAgentDetails();
        if (w.getRecord() <= -1) {
            return; // Empty location
        }
        AgentInfo agentid = p.getAgentId(), a = w.recordAt(ad);
        boolean allowOverride = !p.getAgentId().getAgentNameIsFinal();
        if ((allowOverride || agentid.getAgentLanguage() == null) && UserAgent.isLocaleString(a.getAgentLanguage())) {
            agentid.setAgentLanguage(a.getAgentLanguage());
        }
        String name, version;

        if (w.namePassed(ad)) // Name given as Name/Ver (details)
        {
            // The name is in the form Name/XX (details)
            name = a.getAgentName();
            version = a.getAgentVersion();
        } else // Name given as a token in the details list.
        {
            name = StringUtils.nullify(StringUtils.firstPart(w.detailAt(ad)));
            version = StringUtils.nullify(UserAgent.extractVersion(w.detailAt(ad).substring(name.length())));
        }
        if (agentid.getAgentNameIsFinal()) {
            return;
        }
        // See if name is given in the form NAME XX.YY (which is incorrect, but Opera for one does this)
        if (version == null) {
            String[] nameparts = name.split(" ");
            name = nameparts[0];
            for (int i = 1; i < nameparts.length; i++) {
                if (UserAgent.isVersionString(nameparts[i])) {
                    version = nameparts[i];
                    nameparts[i] = null;
                } else if (UserAgent.isEmailAddress(nameparts[i])) {
                    agentid.addExtraBrowserInfo("email", nameparts[i]);
                    nameparts[i] = null;
                } else {
                    name = StringUtils.smartConcat(name, nameparts[i]);
                }
            }

            // If the version information is still missing, then we may be dealing with Name-Ver or Name.ver patterns
            // Example: "Java1.4.2_03", "larbin_2.6.3 ghary@sohu.com"
            if (version == null) {
                for (int i = 0; i < name.length(); i++) {
                    if (UserAgent.isVersionString(name.substring(i))) {
                        version = name.substring(i);
                        name = StringUtils.firstPart(name);
                        break;
                    }
                }
            }
        }

        // set values
        if ((allowOverride || agentid.getAgentName() == null) && name != null) {
            agentid.setAgentName(name);
        }
        if ((allowOverride || agentid.getAgentVersion() == null) && version != null) {
            agentid.setAgentVersion(version);
        }
    }

    public static boolean isEmailAddress(String addr) {
        Matcher matcher = email.matcher(addr);
        return matcher.find();
    }

    public static boolean isLocaleString(String lang) {
        /*
         * Locale information has two types:
         * 2 character, eg en
         * 5 character, eg en_GB, or en-us
         */
        if (lang == null) {
            return false;
        }
        if ((lang.length() == 2) && Character.isLetter(lang.charAt(0)) && Character.isLetter(lang.charAt(1))) {
            return true;
        }
        if ((lang.length() == 5) &&
                (lang.charAt(2) == '_' || lang.charAt(2) == '-') &&
                (isLocaleString(lang.substring(0, 2))) &&
                (isLocaleString(lang.substring(3, 5)))) {
            return true;
        }
        return false;
    }

    public static boolean isVersionString(String ver) {
        // We'll accept anything as a string that stars with a number.
        if (ver == null || ver.length() < 1) {
            return false;
        }
        return Character.isDigit(ver.charAt(0));
    }

    public static boolean isPossibleName(String n) {
        if (n == null || n.length() < 1) {
            return false;
        }
        if (Character.isLetter(n.charAt(0))) {
            return true;
        }
        return false;
    }

    public static boolean isResolutionString(String res) {
        /* Resolution strings are not present in most ua strings, but if they are, their format is
         * NNNxNNN or NNNxNNN-N where N is a number. The x is mandatory, the - is not. */
        if (res == null) {
            return false;
        }
        boolean seenx = false;
        boolean seendash = false;
        for (int i = 0; i < res.length(); i++) {
            if (!Character.isDigit(res.charAt(i))) {
                if (res.charAt(i) == 'x') {
                    if (seenx) {
                        return false;
                    } else {
                        seenx = true;
                    }
                } else if (res.charAt(i) == '-') {
                    if (seendash) {
                        return false;
                    } else {
                        seendash = true;
                    }
                } else {
                    return false;
                }
            }
        }
        return seenx;
    }

    public static String extractVersion(String ver) {
        if (ver == null || ver.length() < 2) {
            return null;
        }
        String ret = "";
        for (int i = 1; i < ver.length(); i++) {
            if (ver.charAt(i) == ' ') {
                break;
            }
            ret = ret + ver.charAt(i);
        }
        return StringUtils.nullify(ret);
    }

    public static Vector<WordLocation> iterateOverDetails(SwitchParameters p, HashSwitch<String, SwitchParameters, Boolean, IHashCase<SwitchParameters, Boolean>> dictionary) {

        Vector<AgentInfo> ad = p.getAgentDetails();
        int numDetails = p.getLocation().numDetails(ad);
        Vector<WordLocation> unrecognized = new Vector<WordLocation>(numDetails);
        for (int i = p.getLocation().getDetail(); i < numDetails; i++) {
            WordLocation l = new WordLocation(p.getLocation().getRecord(), i, -1);
            String name = l.detailAt(ad);
            // System.out.println("iteratOverDetails "+name);
            if (name == null) {
                continue;
            }
            if (name.equals("U")) {
                continue;
            }
            if (name.equals("I")) {
                continue;
            }
            if (name.equals("N")) {
                continue;
            }
            if (name.equals("x")) {
                continue;
            }
            if (name.equals("compatible")) {
                continue;
            }

            // Tokens dealing with resolution, etc.
            if (UserAgent.isResolutionString(name)) {
                p.getAgentId().setLayoutResolution(name);
                continue;
            }
            if (UserAgent.isLocaleString(name)) {
                p.getAgentId().setAgentLanguage(name);
                continue;
            }

            // compare everything else against the dictionary:
            // First, we try to match the entire detail token. If that
            // doesn't work, then we just match the firstPart of the token

            if (dictionary.contains(name) && dictionary.switchby(name, new SwitchParameters(p.getAgentId(), ad, l))) {
                continue;
            }
            // If the full detail token was not recognized, try just the first part:
            String shortName = StringUtils.firstPart(name);
            //System.out.println("shortName: " + shortName);
            if (!dictionary.contains(shortName) || !dictionary.switchby(shortName, new SwitchParameters(p.getAgentId(), ad, new WordLocation(l.getRecord(), l.getDetail(), 0)))) {
                unrecognized.add(new WordLocation(l));
            }
        }
        //System.out.println("Unrecognized: "+unrecognized);
        return unrecognized;
    }

    public UserAgent(LookupTable lookupTable) {
        super(lookupTable);
    }

    public UserAgent(LookupTable lookupTable, int threadCount) {
        super(lookupTable, threadCount);
    }

 public ProcessingEngine.ProcessingRunnable getRunnable() {
        return new UserAgentRunnable();
    }

    protected class UserAgentRunnable extends ProcessingEngine.ProcessingRunnable {

        private Vector<AgentInfo> agentDetails;
        private AgentInfo agentid;

        public UserAgentRunnable() {}
        public void performTask(String uaString) {
            /* Akkor volt sikeres a useragent feldolgozása ha sikeresen szétválasztottuk a useragent string
             * részeit, és utána sikerült azonosítani ez alapján a böngészőt.
             */
            //System.err.println("Processing "+uaString);
            agentDetails = new Vector<AgentInfo>(6);
            agentid = new AgentInfo();
            boolean success = separateParts(uaString) && classify();
            ProcessedData ret;
            if (!success) {
                logger.debug("UserAgent processor failed to process string '" + uaString + "'.");
                ret = new ProcessedData();
                ret.setState(ProcessedData.State.FAILURE);
            } else {
                ret = agentid.toProcessedData();
                ret.setDataSourceModified(dataSourceModified);
            }
            writeResult(uaString, ret);
        }

        private Vector<Vector<WordLocation>> iterateOverNames(Vector<AgentInfo> ad) {
            Vector<Vector<WordLocation>> unrec = new Vector<Vector<WordLocation>>(ad.size());
            for (int i = 0; i < ad.size(); i++) {
                WordLocation l = new WordLocation(i, -1, -1);
                String name = l.nameAt(ad);
                if (agentid.getAgentLanguage() == null && l.recordAt(ad).getAgentLanguage() != null) {
                    agentid.setAgentLanguage(l.recordAt(ad).getAgentLanguage());
                }
                //System.out.println("iterateOverNames: "+name);
                if (name == null) {
                    continue;
                }
                if (!dictionary.contains(name) || !dictionary.switchby(name, new SwitchParameters(agentid, ad, l))) {
                    identifyUnknown(l);
                }
                unrec.add(iterateOverDetails(new SwitchParameters(agentid, ad, new WordLocation(l.getRecord(), 0, -1)), dictionary));
            }
            return unrec;
        }

        private void identifyUnknown(WordLocation l) {
            setBrowserDetails(new SwitchParameters(agentid, agentDetails, l));
        }

        private void identifyMozillaSuite() {
            HashMap<String, String> h = agentid.getExtraBrowserInfo();
            if (h != null && h.containsKey("Mozilla revision")) {
                agentid.setAgentName("Mozilla Suite");
                agentid.setAgentVersion(h.get("Mozilla revision"));
            }
        }

        private void identifyByUnrecognizedDetails(Vector<Vector<WordLocation>> unrec) {
            Vector<WordLocation> d = unrec.get(0);
            if (d.size() == 0) {
                if (agentid.getHasMozillaField() && StringUtils.startsWith(agentDetails.get(0).getAgentVersion(), "4.") || StringUtils.startsWith(agentDetails.get(0).getAgentVersion(), "3.")) {
                    agentid.setAgentName("Netscape");
                    agentid.setAgentVersion(agentDetails.get(0).getAgentVersion());
                    agentid.setLayoutEngine("Old Netscape");
                }
            } else {
                for (int i = 0; i < d.size(); i++) {
                    WordLocation l = d.get(i);
                    if (isPossibleName(l.detailAt(agentDetails))) {
                        setBrowserDetails(new SwitchParameters(agentid, agentDetails, d.get(i)));
                    }
                }
            }
        }

        private boolean classify() {
            /* classfiy tries to identify the user agent and its platform based on the agentDetails vector.
             * @param none (the agentdetails vector is used automatically).
             * @return boolean (true if classification was successful, false otherwise).
             */
            if (agentDetails.size() < 1) {
                return false;
            }
            Vector<Vector<WordLocation>> unrec = iterateOverNames(agentDetails);
            if (agentid.getAgentDevice() == AgentInfo.AgentDevice.UNKNOWN &&
                    agentid.getAgentProcessor() != AgentInfo.AgentProcessor.UNKNOWN) {
                identifyDeviceByProcessor(agentid);
            }
            if (!isIdentified(agentid) && agentid.getHasMozillaField()) {
                identifyMozillaSuite();
            }
            if (!isIdentified(agentid)) {
                identifyByUnrecognizedDetails(unrec);
            }
            if (isIdentified(agentid) && agentid.getAgentType() == AgentInfo.AgentType.UNKNOWN) {
                identifyTypeByName();
            }
            //Link Validity Check From: http://www.w3dir.com/cgi-bin (Using: Hot Links SQL by Mrcgiguy.com)
            return (isIdentified(agentid));
        }

        private void identifyTypeByName() {
            if ((agentid.getAgentName()).contains("Valid")) {
                agentid.setAgentType(AgentInfo.AgentType.ROBOT);
            }
        }

        private void identifyDeviceByProcessor(AgentInfo agentid) {
            if (agentid.getAgentProcessor() == AgentInfo.AgentProcessor.I386 ||
                    agentid.getAgentProcessor() == AgentInfo.AgentProcessor.I586 ||
                    agentid.getAgentProcessor() == AgentInfo.AgentProcessor.I686 ||
                    agentid.getAgentProcessor() == AgentInfo.AgentProcessor.IA_64 ||
                    agentid.getAgentProcessor() == AgentInfo.AgentProcessor.AMD_64 ||
                    agentid.getAgentProcessor() == AgentInfo.AgentProcessor.SUN ||
                    agentid.getAgentProcessor() == AgentInfo.AgentProcessor.WIN32_ON_64 ||
                    agentid.getAgentProcessor() == AgentInfo.AgentProcessor.ALPHA ||
                    agentid.getAgentProcessor() == AgentInfo.AgentProcessor.IA_64) {
                agentid.setAgentDevice(AgentInfo.AgentDevice.COMPUTER);
            }
        }

        private String extractAgentInfo(String ua) {
            /* @param String ua : A teljes useragent stringnek egy olyan része, mely
             * nem feltétlenül a string elején kezdődik, de az eredeti useragent string végéig tart.
             * Vagyis ha userAgent a teljes useragent string, akkor ua = userAgent.substring(x,userAgent.length());
             * @return String remainder : Leválasztva egy AgentInfo rekordot, a függvény a maradékot adja vissza.
             * Amennyiben nem tudott semmit kezdeni a bemenő ua stringgel, ""-vel tér vissza.
             * Mellékhatások: a leválaszott userAgent adatok az agentInfo struktúra szerint az AgentDetails vektorba kerülnek.
             */
            ua = ua.trim();

            /* Terméknév/Verzió [nyelv] (Részletezés)  négyeseket keresünk, először tehát egy Terméknévre van szűkségünk.
             * A terméknév tartalmazhat számokat, de nem keződhet azzal (tapasztalatom szerint). Sajnos szünetek is lehetnek benne.
             * A terméknév tehát az ua első alfanumerikus karakterétől az első '/', '(', vagy stringvége karakterig tart.
             */
            ParserStates state = ParserStates.NAME;
            int pos = 0, len = ua.length(), parenLevel = 0;
            if (len > 0 && Character.isLetterOrDigit(ua.charAt(0)) || ua.charAt(0) == '!') {
                String name = "", version = "", details = "", language = "";
                for (; pos < len && state != ParserStates.DONE; pos++) {
                    char c = ua.charAt(pos);
                    //System.out.println("state: "+state+" char:"+c); //debug

                    // Állapotátmenetek:
                    if (state == ParserStates.NAME && c == '/') {
                        // Vége a névnek, mostmár a verzió szám jön.
                        state = ParserStates.VERSION;
                        continue;
                    }


                    if ((state == ParserStates.NAME || state == ParserStates.VERSION || state == ParserStates.LANGUAGE) && c == '(') {
                        // Most a részletekhez megyünk.
                        state = ParserStates.DETAILS;
                        parenLevel = 1;
                        continue;
                    }


                    if ((state == ParserStates.VERSION) && c == '[') {
                        // Vége a verziónak, és mostmár a nyelv jön.
                        state = ParserStates.LANGUAGE;
                        continue;
                    }


                    if (state == ParserStates.NAME || state == ParserStates.VERSION && c == ' ') {
                        /* Verzióban nem lehet szünet, így ez már egy új elemet jelöl.
                         * Ha '(' a következő nem szünet karakter akkor részletek jönnek, egyébként pedig egy új név
                         */
                        char f = lookForward(pos, ua);
                        if (f == '(') {
                            state = ParserStates.DETAILS;
                            continue;
                        }
                        if (f == '[') {
                            state = ParserStates.LANGUAGE;
                            continue;
                        }
                        if (state == ParserStates.VERSION && Character.isLetter(f)) {
                            state = ParserStates.DONE;
                            break;
                        }
                    }

                    // Állapotátmenet nem volt ha idáig eljutottunk, mentsük el az új karaktert!
                    switch (state) {
                        case NAME:
                            name += c;
                            break;
                        case VERSION:
                            version += c;
                            break;
                        case DETAILS:
                            // lehetnek beágyazott zárójelek:
                            if (c == ')') {
                                if (parenLevel == 1) {
                                    // Opera's odd langauge string format
                                    if (lookForward(pos, ua) == '[') {
                                        state = ParserStates.LANGUAGE;
                                    } else {
                                        state = ParserStates.DONE;
                                    }
                                    parenLevel = 0;
                                } else {
                                    parenLevel--;
                                }
                            } else if (c == '(') {
                                parenLevel++;
                            } else {
                                details += c;
                            }
                            break;
                        case LANGUAGE:
                            if (c == '[') {
                                continue;
                            }
                            if (c == ']') {
                                state = ParserStates.DETAILS;
                            } else {
                                language += c;
                            }
                            break;
                    }
                }

                // Adjuk hozzá a kiszűrt adatokat a agentDetails vektorhoz.
                if (name.length() >= 1) {
                    //System.out.println("Regsitering new AgentDetails("+agentDetails.size()+") entry:");
                    AgentInfo u = new AgentInfo(name, version, details, language);
                    //System.out.println(u);
                    agentDetails.add(u);
                } else {
                    // Valószínűleg elrontottunk valamit
                    System.out.println("Suspcious input: name = " + name + " version = " + version + " details=" + details + " language = " + language);
                }

                return ua.substring(pos); // visszadobjuk a maradék stringet
            } else {
                return ""; // nem tudtunk értelemes terméknevet kiszedni ua-ból.
            }
        }

        private char lookForward(int pos, String ua) {
            char ret = ' ';
            for (; pos < ua.length(); pos++) {
                if (ua.charAt(pos) != ' ') {
                    ret = ua.charAt(pos);
                    break;
                }
            }
            return ret;
        }

        private boolean separateParts(String ua) {
            /* A User Agent string értelmezése
             * Terméknév/Verzió (Részletezés) [nyelv] négyesekből áll.
             * Egyedül a Terméknév megadása kötelező.
             * Több ilyen négyes is előfordulhat, pl. Safari böngészőknél egy ilyen
             * tuple azonosítja a böngészőt, ismét egy a WebKit renderert, stb.
             */

            // Addig válasszunk le újabb és újabb tuple-eket a useragent stringből ameddig lehet.
            while (!ua.equals("")) {
                ua = extractAgentInfo(ua);
            }
            return agentDetails.size() > 0;
        }
    }

    public boolean init() {        
        java.net.URL filename =  ClassLoader.getSystemResource("versioned_datasources/UserAgentConfiguration.xml");
        Document doc = null;
        try {
            doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(filename.toURI().toString());
            // TODO(neumark): determine datasource modification date!
            //dataSourceModified = new Date(filename.lastModified());
        } catch (Exception e) {
            logger.error("Error opening useragent database " + filename, e);
            return false;
        }
        dictionary = new HashSwitch<String, SwitchParameters, Boolean, IHashCase<SwitchParameters, Boolean>>(120);
        UserAgentConfig.fillDictionary(dictionary, doc);
        return super.init();
    }
}