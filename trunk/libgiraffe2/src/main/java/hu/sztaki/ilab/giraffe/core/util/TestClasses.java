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

/**
 * THIS FILE WILL BE DELETED.
 * This file will exist as long as giraffe2-dataprocessors does not compile.
 * After that, this file should be considered deprecated, and NOT USED!
 * @author neumark
 */
public class TestClasses {
  public static class SomeClass {
      String str;
      public boolean runTask(String str) {this.str = str; return true;}
      public String getApache_id() {return "apache id for "+str;}
      public String getPhpsessid() {return "phpsessid id for "+str;}
  }

  public static class AnotherClass {
      String ua,loc;
      public boolean runTask(String ua, String loc) {this.ua = ua; this.loc = loc; return true;}
      public String getLanguage() {return "lang for ua, log "+ua + loc;}
  }

  public static String fn1(String ip) {return "Resolved hostname for "+ip;}
  public static String fn2(String ip) {return "Country for "+ip;}
}
