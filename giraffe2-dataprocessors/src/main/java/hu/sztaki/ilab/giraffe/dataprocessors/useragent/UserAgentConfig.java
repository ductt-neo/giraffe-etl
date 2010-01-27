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

package hu.sztaki.ilab.giraffe.dataprocessors.useragent;
import hu.sztaki.ilab.giraffe.core.util.*;
import java.util.*;
import java.io.*;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

/**
 *
 * @author neumark
 */
public class UserAgentConfig {
   
   private static class Ignore implements IHashCase<SwitchParameters,Boolean>
   {    
    public Boolean process (SwitchParameters p)
    {            
           return new Boolean(true);                      
    }
   }    
   
   private static class JustSetName implements IHashCase<SwitchParameters,Boolean>
   {
    String agentName ,agentLayoutEngine;
    boolean lowpriority;
    AgentInfo.AgentType type;
    public JustSetName(String n, AgentInfo.AgentType t,String l, boolean lp) {agentName = n; type = t; agentLayoutEngine = l;lowpriority = lp;}
    public Boolean process(SwitchParameters p)
    {                      
        String name = agentName, layoutEngine = agentLayoutEngine;
        p.getAgentId().setAgentNameIsFinal(!lowpriority);
        if (name != null) p.getAgentId().setAgentName(name);
        if (p.getAgentId().getLayoutEngine() == null && layoutEngine != null) p.getAgentId().setLayoutEngine(layoutEngine);
        if (type != AgentInfo.AgentType.UNKNOWN) p.getAgentId().setAgentType(type);        
        return new Boolean(true);
    }
   }          
      
   private static class JustSetLayoutEngine implements IHashCase<SwitchParameters,Boolean>
   {
    String agentName;
    JustSetLayoutEngine () {agentName = null;}
    JustSetLayoutEngine (String n) {agentName = n;}
    public Boolean process(SwitchParameters p)    
    {
        String name = agentName;
        WordLocation w = new WordLocation(p.getLocation());
        if (w.getRecord() <= -1 ) return new Boolean(false); // bad location
        AgentInfo agentid = p.getAgentId();
        Vector<AgentInfo> ad = p.getAgentDetails();
        if (w.namePassed(ad))
        {
            agentid.setLayoutEngine(w.nameAt(ad));            
            if (w.recordAt(ad).getDetails().size() > 0) agentid.setLayoutMode(w.recordAt(ad).getDetails().toString());
        }
        else if (w.detailPassed(ad))
        {
            String fullName = w.detailAt(ad);            
            if (agentid.getLayoutEngine() == null)agentid.setLayoutEngine(fullName);
        }
        if (agentid.getAgentName() == null && name != null) agentid.setAgentName(name);
        return new Boolean(true); 
    }
   }
   
   private static class IdentifyLinux implements IHashCase<SwitchParameters,Boolean>
   {
    public Boolean process(SwitchParameters p)    
    {
        UserAgent.setLinux(p);
        return new Boolean(true); 
    }
   }
   
   private static class IdentifyWindows implements IHashCase<SwitchParameters,Boolean>
   {
    String ver;
    IdentifyWindows() {ver = null;}
    IdentifyWindows(String v) {ver = v;}
    public Boolean process(SwitchParameters p)    
    {
          String version = ver;
          WordLocation w = p.getLocation();
          if (w.getRecord() <= -1 || w.getDetail() <= -1) return new Boolean(false); // bad location
          AgentInfo agentid = p.getAgentId();
          Vector<AgentInfo> ad = p.getAgentDetails();
          // It's possible that the version has already been determined, in this case, exit now!
          if (agentid.getAgentOSVersion() != null) return new Boolean(true);
          agentid.setAgentSWPlatform(AgentInfo.AgentSWPlatform.WINDOWS);
          agentid.setAgentOS("Microsoft Windows");
          if (version == null)           
          {
                 String fullName = w.detailAt(ad);
                 version = StringUtils.nullify(fullName.substring((StringUtils.firstPart(fullName)).length()));                 
          }
          if (version != null) agentid.setAgentOSVersion(version);
          if (agentid.getAgentOSVersion() != null && agentid.getAgentOSVersion().equals("CE")) {if (agentid.getAgentDevice() == AgentInfo.AgentDevice.UNKNOWN) agentid.setAgentDevice(AgentInfo.AgentDevice.PDA);}
          else agentid.setAgentDevice(AgentInfo.AgentDevice.COMPUTER);
          return new Boolean(true);                 
    }
   }
   
   private abstract static class RecurseOnWords implements IHashCase<SwitchParameters,Boolean>
   {
    HashSwitch<String,SwitchParameters,Boolean, IHashCase<SwitchParameters,Boolean>> dictionary ;
    RecurseOnWords(HashSwitch<String,SwitchParameters,Boolean, IHashCase<SwitchParameters,Boolean>> d) {dictionary = d;}
    
    public abstract boolean identify(String w, SwitchParameters p);
    
    private Vector<Boolean> iterateOverWords(WordLocation wl,AgentInfo agentid, Vector<AgentInfo> ad)
    {       
       int numWords = wl.numWords(ad);
       Vector<Boolean> s = new Vector<Boolean>(numWords);                     
       for (int i = wl.getWord(); i < numWords ;i++)
       {
        WordLocation l = new WordLocation(wl.getRecord(), wl.getDetail(),i);                
        if (dictionary.contains(l.wordAt(ad)) && dictionary.switchby(l.wordAt(ad), new SwitchParameters(agentid,ad,l))) s.add(new Boolean(true));
        else s.add(new Boolean(false));
       }
       return s; 
    }
    
    public Boolean process(SwitchParameters p)
    {   
        WordLocation loc = p.getLocation();
        AgentInfo agentid = p.getAgentId();
        Vector<AgentInfo> ad = p.getAgentDetails();                    
        
        if (loc.namePassed(ad)) return new Boolean(identify(loc.nameAt(ad),p));
        if (loc.detailPassed(ad)) return new Boolean(identify(loc.detailAt(ad),p));
        if (loc.getWord() > 0) return new Boolean(identify(loc.wordAt(ad),p));
                
        iterateOverWords(new WordLocation(loc.getRecord(),loc.getDetail(),1),agentid,ad);                
        return new Boolean(identify(loc.wordAt(ad),p));
                
    }
   }
   
   private static class IdentifyProcessor extends RecurseOnWords
   {
    AgentInfo.AgentProcessor proc;    
        
    IdentifyProcessor(AgentInfo.AgentProcessor p, HashSwitch<String,SwitchParameters,Boolean, IHashCase<SwitchParameters,Boolean>> d) 
    {
        super(d);
        proc = p;
    }
    
    public boolean identify(String w, SwitchParameters p)
    {
        WordLocation loc = p.getLocation();
        AgentInfo agentid = p.getAgentId();
        Vector<AgentInfo> ad = p.getAgentDetails();                    
        
        agentid.setAgentProcessor(proc);
        return true;
    }    
   }
   
   private static class IdentifyPlatform extends RecurseOnWords
   {
    String aos, aosver;
    AgentInfo.AgentSWPlatform plat;    
    AgentInfo.AgentProcessor proc;
    
    IdentifyPlatform(String o, String v, AgentInfo.AgentSWPlatform p, AgentInfo.AgentProcessor pp, HashSwitch<String,SwitchParameters,Boolean, IHashCase<SwitchParameters,Boolean>> d) 
    {
        super(d);
        aos = o;
        aosver = v;
        plat = p;
        proc = pp;
    }
    
    public boolean identify(String w,SwitchParameters p)
    {     
        WordLocation loc = p.getLocation();
        AgentInfo agentid = p.getAgentId();
        Vector<AgentInfo> ad = p.getAgentDetails();                    
        
        String os = aos, osver = aosver;
        
        if (loc.namePassed(ad))
        {
           if (os == null) os = loc.nameAt(ad);
           if (osver == null) osver = StringUtils.nullify(loc.recordAt(ad).getAgentVersion());
        }
        
        if (loc.detailPassed(ad))
        {
            if (os == null) os = w;
            if (osver == null) osver = StringUtils.nullify(loc.detailAt(ad).substring(w.length()));
        }
        if (loc.wordPassed(ad))
        {
            // The version information is in the current word if it is longer than w
            if (w.length() < loc.wordAt(ad).length())
            {
                if (os == null) os = loc.wordAt(ad);
                if (osver == null) osver = os.substring(w.length());
            }
            else 
            {
                if (os == null) os = loc.wordAt(ad);
                if (osver == null && loc.getWord() < (loc.numWords(ad)-1) )
                {
                    WordLocation verloc = new WordLocation(loc.getRecord(), loc.getDetail(), loc.getWord()+1);
                    String vercandidate = StringUtils.nullify(verloc.wordAt(ad));
                    if (UserAgent.isResolutionString(vercandidate)) osver = vercandidate;
                }
            }
        }
        if (plat != AgentInfo.AgentSWPlatform.UNKNOWN) agentid.setAgentSWPlatform(plat);
        if (os != null) agentid.setAgentOS(os);
        if (osver != null) agentid.setAgentOSVersion(osver);
        if (proc != AgentInfo.AgentProcessor.UNKNOWN) agentid.setAgentProcessor(proc);
        return true;
    }    
   }
   
   private static class IdentifyDevice extends RecurseOnWords
   {
    AgentInfo.AgentDevice dev;    
    String name;
    
    IdentifyDevice(AgentInfo.AgentDevice p, HashSwitch<String,SwitchParameters,Boolean, IHashCase<SwitchParameters,Boolean>> d) 
    {        
        super(d);
        name = null;
        dev = p;
    }
    
    public boolean identify(String w, SwitchParameters p)
    {        
        WordLocation loc = p.getLocation();
        AgentInfo agentid = p.getAgentId();
        Vector<AgentInfo> ad = p.getAgentDetails();                    
        agentid.setAgentDevice(dev);
        if (name != null && agentid.getAgentName() == null) agentid.setAgentName(name);
        return true;
    }    
   }
   
   public static HashMap<String,String> getChildren(Node node)
   {
       HashMap<String,String> map = null;
       map = new HashMap<String,String>();
       if (node.hasChildNodes())
       {
           NodeList nl = node.getChildNodes();
           for (int i = 0; i < nl.getLength();i++)
           {
               Node n = nl.item(i);
               map.put(n.getNodeName(), n.getTextContent());
           }
           map.put("TextContent", node.getTextContent());
       }
       return map;
   }
   
   public static void fillDictionary(HashSwitch<String,SwitchParameters,Boolean, IHashCase<SwitchParameters,Boolean>> dictionary, Document doc)
   {
       if (doc != null)
       {
           NodeList nl = doc.getElementsByTagName("userAgentConfig").item(0).getChildNodes();
           for (int i = 0; i < nl.getLength();i++)
           {
               Node current = nl.item(i);
               if (current.getNodeType() != current.ELEMENT_NODE) continue; // Skip everything but XML elements.
               String tag = current.getNodeName();
               String pattern = current.getAttributes().getNamedItem("pattern").getTextContent();
               HashMap<String,String> contents = getChildren(current);
               if (tag.equals("setBrowserName"))
               {          
                   String layoutEngine = null;
                   AgentInfo.AgentType type = AgentInfo.AgentType.UNKNOWN;
                   boolean lowpriority = false;
                   if (contents != null && contents.containsKey("lowpriority")) lowpriority = true;
                   if (contents != null && contents.containsKey("type")) type = AgentInfo.AgentType.valueOf(contents.get("type"));
                   if (contents != null && contents.containsKey("engine")) layoutEngine = contents.get("engine");
                                      
                   dictionary.put(pattern, new UserAgentConfig.JustSetName(pattern,type,layoutEngine,lowpriority));
               }
               if (tag.equals("identifyLinux"))
               {
                   dictionary.put(pattern, new UserAgentConfig.IdentifyLinux());
               }
               if (tag.equals("ignore"))
               {
                   dictionary.put(pattern, new UserAgentConfig.Ignore());
               }
               if (tag.equals("setLayoutEngine"))
               {
                   if (contents.get("TextContent") != null && contents.get("TextContent").length() > 0) dictionary.put(pattern, new UserAgentConfig.JustSetLayoutEngine(contents.get("TextContent")));
                   else dictionary.put(pattern, new UserAgentConfig.JustSetLayoutEngine());
               }
               if (tag.equals("identifyWindows"))
               {
                   dictionary.put(pattern,new IdentifyWindows(contents.get("TextContent")));
               }
               if (tag.equals("identifyPlatform"))
               {
                   //<identifyPlatform pattern="Intel Mac OS X Mach-O"><name>"Mac OS"</name><version>X</version><platform>MACINTOSH</platform><processor>Intel</processor></identifyPlatform>
                   //dictionary.put("Intel Mac OS Mach-O",new IdentifyPlatform("Mac OS",null,AgentInfo.AgentSWPlatform.MACINTOSH,AgentInfo.AgentProcessor.MACTEL,dictionary));            
                   String OSName = null;
                   String OSVersion = null;
                   AgentInfo.AgentSWPlatform platform = AgentInfo.AgentSWPlatform.UNKNOWN;
                   AgentInfo.AgentProcessor processor = AgentInfo.AgentProcessor.UNKNOWN;
                   
                   if (contents.containsKey("name")) OSName = contents.get("name");
                   if (contents.containsKey("version")) OSVersion = contents.get("version");
                   if (contents.containsKey("platform")) platform = AgentInfo.AgentSWPlatform.valueOf(contents.get("platform"));
                   if (contents.containsKey("processor")) processor = AgentInfo.AgentProcessor.valueOf(contents.get("processor"));
                   
                   dictionary.put(pattern, new IdentifyPlatform(OSName, OSVersion, platform, processor,dictionary));
               }
               if (tag.equals("identifyProcessor"))
               {
                   //dictionary.put("IA64", new IdentifyProcessor(AgentInfo.AgentProcessor.IA_64,dictionary));
          
                   AgentInfo.AgentProcessor processor = AgentInfo.AgentProcessor.UNKNOWN;                 
                   if (contents.containsKey("TextContent")) processor = AgentInfo.AgentProcessor.valueOf(contents.get("TextContent"));                   
                   dictionary.put(pattern, new IdentifyProcessor( processor,dictionary));
               }
               if (tag.equals("identifyDevice"))
               {
                   //dictionary.put("Sprint", new IdentifyDevice(AgentInfo.AgentDevice.PHONE,dictionary));
                   AgentInfo.AgentDevice device = AgentInfo.AgentDevice.UNKNOWN;
                   if (contents.containsKey("TextContent")) device = AgentInfo.AgentDevice.valueOf(contents.get("TextContent"));                   
                   dictionary.put(pattern, new IdentifyDevice( device,dictionary));
               }
           }
       }
    
        dictionary.put("Nav",new IHashCase<SwitchParameters,Boolean>()
        {    
            public Boolean process (SwitchParameters p)
            {                   
                Vector<AgentInfo> ad = p.getAgentDetails();
                WordLocation w = p.getLocation();
                AgentInfo agentid = p.getAgentId();
                if (!w.detailPassed(ad) || !agentid.getHasMozillaField()) return new Boolean(false);
                agentid.setAgentName("Netscape");
                agentid.setAgentVersion(ad.get(0).getAgentVersion());
                agentid.setLayoutEngine("Old Netscape");
                agentid.setAgentType(AgentInfo.AgentType.STANDARD_BROWSER);
                return new Boolean(true);
            }
        });     
    
        // Mozilla's revision string:
        dictionary.put("rv", new IHashCase<SwitchParameters,Boolean>()
        {    
            public Boolean process (SwitchParameters p)
            {            
                Vector<AgentInfo> ad = p.getAgentDetails();
                WordLocation w = p.getLocation();
                if (!w.detailPassed(ad)) return new Boolean(false);
                String value = w.detailAt(ad);
                if (value.startsWith("rv:") && value.length() > 3)
                {
                    p.getAgentId().addExtraBrowserInfo("Mozilla revision", value.substring(3));
                    return new Boolean(true);
                }                    
                return new Boolean(false);
            }
        });    
        
        dictionary.put("Version", new IHashCase<SwitchParameters,Boolean>()
        {    
            public Boolean process (SwitchParameters p)
            {            
                Vector<AgentInfo> ad = p.getAgentDetails();
                WordLocation w = p.getLocation();
                if (!w.namePassed(ad)) return new Boolean(false);
                String value = w.nameAt(ad);
                if (value != null)
                {
                    p.getAgentId().setAgentVersion(value);
                    return new Boolean(true);
                }                    
                return new Boolean(false);
            }
        });
        
        dictionary.put("textmode", new IHashCase<SwitchParameters,Boolean>()
        {    
            public Boolean process (SwitchParameters p)
            {      
                p.getAgentId().setAgentType(AgentInfo.AgentType.TEXTMODE_BROWSER);                
                return new Boolean(true);
            }
        });                
        
        dictionary.put("Mozilla", new IHashCase<SwitchParameters,Boolean>()
        {    
            public Boolean process (SwitchParameters p)
            {      
                if (p.getLocation().namePassed(p.getAgentDetails())) p.getAgentId().setHasMozillaField(true);
                return new Boolean(true);
            }
        });                
        //End
    }    
   }      