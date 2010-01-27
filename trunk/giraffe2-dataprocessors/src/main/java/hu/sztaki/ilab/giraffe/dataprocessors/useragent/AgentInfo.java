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
import hu.sztaki.ilab.giraffe.core.dataprocessors.ProcessedData;
import hu.sztaki.ilab.giraffe.core.util.*;
import java.util.*;
/**
 *
 * @author neumark
 */

public class AgentInfo {

   public enum AgentType {STANDARD_BROWSER, TEXTMODE_BROWSER, ROBOT, UNKNOWN
    };
   public enum AgentDevice {COMPUTER, WEBTV, CONSOLE, PHONE, KIOSK, PDA, UNKNOWN
    };
   public enum AgentProcessor {I386, I586, I686, IA_64, AMD_64, PPC, MACTEL, SUN, M68K, WIN32_ON_64, ALPHA, UNKNOWN
    };
   public enum AgentSWPlatform {WINDOWS, OS2, UNIX, MACINTOSH, BEOS, AMIGA, UNKNOWN    
    };
   private String agentName,  agentLanguage,  agentVersion, // information for browser layout engine where provided
             layoutEngine,  layoutMode,  layoutResolution,  agentOS,  agentOSDistro,  agentOSVersion;
   public AgentType agentType;
   public AgentDevice agentDevice;
   public AgentProcessor agentProcessor;
   public AgentSWPlatform agentSWPlatform;
   private boolean hasMozillaRecord, agentNameIsFinal;
   private HashMap<String,String> extraBrowserInfo;
   private Vector<String> details;

   public ProcessedData toProcessedData()
   {
       ProcessedData ret = new ProcessedData();
        //if (agentName != null) {
            ret.setField("agentName", agentName);
        //}
        //if (agentVersion != null) {
            ret.setField("agentVersion", agentVersion);
        //}
        //if (agentLanguage != null) {
            ret.setField("agentLanguage", agentLanguage);

        //}
        //if (agentOS != null) {
            ret.setField("agentOS", agentOS);
        //}
        //if (agentOSVersion != null) {
            ret.setField("agentOSVersion", agentOSVersion);
        //}        
        //if (agentOSDistro != null) {
            ret.setField("agentOSDistro", agentOSDistro);
        //}
        //if (layoutEngine != null) {
            ret.setField("layoutEngine", layoutEngine);     
        //}
        //if (layoutMode != null) {
            ret.setField("layoutMode", layoutMode);   
        //}
        //if (layoutResolution != null) {
            ret.setField("layoutResolution", layoutResolution);    
        //}
        //if (agentType != AgentType.UNKNOWN) {
            ret.setField("agentType", agentType.toString());           
        //}
        //if (agentDevice != AgentDevice.UNKNOWN) {
            ret.setField("agentDevice", agentDevice.toString());
        //}
        //if (agentProcessor != AgentProcessor.UNKNOWN) {
            ret.setField("agentProcessor",agentProcessor.toString() );
        //}
        //if (agentSWPlatform != AgentSWPlatform.UNKNOWN) {
            ret.setField("agentSWPlatform", agentSWPlatform.toString());         
        //}

       return ret;
   }

   private Vector<String> processDetails(String d) {
        String[] tokens = d.split(";");
        Vector<String> parts = new Vector<String>();
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = StringUtils.nullify(tokens[i]);
            if (tokens[i] != null) {
                parts.add(tokens[i]);
            }
        }
        return parts;
    }

    private void clear() {
        hasMozillaRecord = false;
        agentNameIsFinal = false;
        extraBrowserInfo = null;
        details = null;
        agentType = AgentType.UNKNOWN;
        agentDevice = AgentDevice.UNKNOWN;
        agentProcessor = AgentProcessor.UNKNOWN;
        agentSWPlatform = AgentSWPlatform.UNKNOWN;
    }
    
    public AgentInfo() {clear();}

    public AgentInfo(String n, String v, String d, String l) {
        clear();
        this.agentName = StringUtils.nullify(n);
        this.agentVersion = StringUtils.nullify(v);
        this.details = processDetails(d);
        this.agentLanguage = StringUtils.nullify(l);
    }

    public AgentInfo(String n, String v, String l, AgentSWPlatform plat, AgentDevice dev, AgentProcessor proc, AgentType t,
            String aOS, String aOSDistro, String aOSVer, String alayoutEngine, String alayoutMode, String alayoutRes) {        
        clear();
        this.agentName = StringUtils.nullify(n);
        this.agentVersion = StringUtils.nullify(v);        
        this.agentLanguage = StringUtils.nullify(l);
        agentOS = StringUtils.nullify(aOS);
        agentOSVersion = StringUtils.nullify(aOSVer);   
        agentOSDistro = StringUtils.nullify(aOSDistro);
        layoutEngine = StringUtils.nullify(alayoutEngine);
        layoutMode = StringUtils.nullify(alayoutMode);
        layoutResolution = StringUtils.nullify(alayoutRes);
        agentType = t;
        agentDevice = dev;
        agentProcessor = proc;
        agentSWPlatform = plat;
    }

   
    
    public void addExtraBrowserInfo(String key, String value)
    {
        if (extraBrowserInfo == null) extraBrowserInfo = new HashMap<String,String>(10);
        extraBrowserInfo.put(key, value);
    }
    
    public HashMap<String,String> getExtraBrowserInfo() {return extraBrowserInfo;}

    public String toString() {
        return "" +
                StringUtils.maybePrint("Name", agentName) +
                StringUtils.maybePrint("Version", agentVersion) +
                StringUtils.maybePrint("Language", agentLanguage) +
                StringUtils.maybePrint("Type", agentType.toString()) +
                StringUtils.maybePrint("Processor", agentProcessor.toString()) +
                StringUtils.maybePrint("Platform", agentSWPlatform.toString()) +
                StringUtils.maybePrint("Device", agentDevice.toString()) +
                StringUtils.maybePrint("Layout Engine", this.layoutEngine) +
                StringUtils.maybePrint("Layout Mode", this.layoutMode) +
                StringUtils.maybePrint("Screen Resolution", this.layoutResolution) +
                StringUtils.maybePrint("OS", this.agentOS) +                
                StringUtils.maybePrint("OS Distro", this.agentOSDistro) +                
                StringUtils.maybePrint("OS Version", this.agentOSVersion) +
              //StringUtils.maybePrint("has Mozilla field", (new Boolean(this.hasMozillaRecord)).toString()) +
              //StringUtils.maybePrint("Extra", this.extraBrowserInfo) +
                "";
    }

    public void load(AgentInfo c) {
        if (c.agentName != null) {
            agentName = c.agentName;
        }
        if (c.agentVersion != null) {
            agentVersion = c.agentVersion;
        }
        if (c.agentLanguage != null) {
            agentLanguage = c.agentLanguage;
        }
        if (c.agentOS != null) {
            agentOS = c.agentOS;
        }
        if (c.agentOSVersion != null) {
            agentOSVersion = c.agentOSVersion;
        }        
        if (c.agentOSDistro != null) {
            agentOSDistro = c.agentOSDistro;
        }
        if (c.layoutEngine != null) {
            layoutEngine = c.layoutEngine;
        }
        if (c.layoutMode != null) {
            layoutMode = c.layoutMode;
        }
        if (c.layoutResolution != null) {
            layoutResolution = c.layoutResolution;
        }
        if (c.agentType != AgentType.UNKNOWN) {
            agentType = c.agentType;
        }
        if (c.agentDevice != AgentDevice.UNKNOWN) {
            agentDevice = c.agentDevice;
        }
        if (c.agentProcessor != AgentProcessor.UNKNOWN) {
            agentProcessor = c.agentProcessor;
        }
        if (c.agentSWPlatform != AgentSWPlatform.UNKNOWN) {
            agentSWPlatform = c.agentSWPlatform;
        }
    }
    
    public void setAgentName (String an) {agentName = StringUtils.nullify(an);}
    public void setAgentVersion (String an) {agentVersion = StringUtils.nullify(an);}
    public void setAgentLanguage (String an) {agentLanguage = StringUtils.nullify(an);}
    public void setAgentOS (String an) {agentOS = StringUtils.nullify(an);}
    public void setAgentOSVersion (String an) {agentOSVersion = StringUtils.nullify(an);}
    public void setAgentOSDistro (String an) {agentOSDistro = StringUtils.nullify(an);}
    public void setLayoutEngine (String an) {layoutEngine = StringUtils.nullify(an);}
    public void setLayoutMode (String an) {layoutMode = StringUtils.nullify(an);}
    public void setLayoutResolution (String an) {layoutResolution = StringUtils.nullify(an);}
    public void setAgentType (AgentType an) {agentType = an;}
    public void setAgentDevice (AgentDevice an) {agentDevice = an;}
    public void setAgentProcessor (AgentProcessor an) {agentProcessor = an;}
    public void setAgentSWPlatform (AgentSWPlatform an) {agentSWPlatform = an;}
    public void setHasMozillaField (boolean s) {this.hasMozillaRecord = s;}
    public void setAgentNameIsFinal (boolean s) {this.agentNameIsFinal = s;}
    
    public String getAgentName () {return agentName;}
    public String getAgentVersion () {return agentVersion ;}
    public String getAgentLanguage () {return agentLanguage ;}
    public String getAgentOS () {return agentOS ;}
    public String getAgentOSVersion () {return agentOSVersion ;}
    public String getAgentOSDistro () {return agentOSDistro ;}
    public String getLayoutEngine () {return layoutEngine ;}
    public String getLayoutMode () {return layoutMode ;}
    public String getLayoutResolution () {return layoutResolution ;}
    public AgentType getAgentType () {return agentType ;}
    public AgentDevice getAgentDevice () {return agentDevice ;}
    public AgentProcessor getAgentProcessor () {return agentProcessor ;}
    public AgentSWPlatform getAgentSWPlatform () {return agentSWPlatform ;}
    public Vector<String> getDetails() {return details;}
    public boolean getHasMozillaField () {return this.hasMozillaRecord;}
    public boolean getAgentNameIsFinal () {return this.agentNameIsFinal ;}    
}
