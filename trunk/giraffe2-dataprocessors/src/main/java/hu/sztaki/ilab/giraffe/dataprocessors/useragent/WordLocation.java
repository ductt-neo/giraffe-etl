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

/**
 *
 * @author neumark
 */
public class WordLocation {   
    int record, detail, word;
    public WordLocation(int r, int d, int w)
    {
        record = r;
        detail = d;
        word = w;
    }
    
    public WordLocation (WordLocation l)
    {
        this.record = l.record;
        this.detail = l.detail;
        this.word = l.word;                
    }
    
    public int numDetails(Vector<AgentInfo> ad)
    {
     return (ad.get(getRecord())).getDetails().size();
    }
    
    public int numWords(Vector<AgentInfo> ad)
    {
     return (((ad.get(record)).getDetails()).get(detail).split(" ")).length;
    }
        
    public String nameAt(Vector<AgentInfo> ad)
    {
     return StringUtils.nullify((ad.get(getRecord())).getAgentName());
    }
           
    public String detailAt(Vector<AgentInfo> ad)
    {
     return StringUtils.nullify(((ad.get(record)).getDetails()).get(detail));
    }
    
    public AgentInfo recordAt(Vector<AgentInfo> ad)
    {
        return ad.get(record);
    }
    
    public String wordAt(Vector<AgentInfo> ad)
    {
        return StringUtils.nullify(((ad.get(record).getDetails().get(detail)).split(" "))[word]);
    }
        
    public boolean namePassed(Vector<AgentInfo> ad)
    {
        return (record >= 0 && detail == -1 && word == -1);
    }
    
    public boolean detailPassed(Vector<AgentInfo> ad)
    {
        return (record >= 0 && detail >= 0 && word == -1 );
    }
    
    public boolean wordPassed (Vector<AgentInfo> ad)
    {
        return (record >= 0 && detail >= 0 && word >= 0);
    }
            
    public int getRecord() {return record;}    
    public int getDetail() {return detail;}    
    public int getWord() {return word;}        
    public String toString () {return "Location: record "+record+" token "+detail+" word "+word;}             
}
