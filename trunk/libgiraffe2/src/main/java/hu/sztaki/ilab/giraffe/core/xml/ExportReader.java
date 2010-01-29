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
package hu.sztaki.ilab.giraffe.core.xml;

import java.net.URL;
import java.net.URLClassLoader;
import hu.sztaki.ilab.giraffe.schema.exports.Exports;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URLConnection;

/**
 *
 * @author neumark
 */
public class ExportReader {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ExportReader.class);
    private final java.util.List<Class> exportedClasses = new java.util.LinkedList<Class>();

    public ExportReader() {
        readExportFiles();
    }

    public java.util.List<Class> getExportedClasses() {
        return exportedClasses;
    }

    private void readExportFiles() {
        URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        // Iterate through the URL's loaded by the system class loader, opening the JAR
        // files in the classpath.
        for (URL u : sysloader.getURLs()) {
            // add the ending to the url:
            URL url = null;
            try {
                if (u.getFile().endsWith(".jar")) {
                    url = new URL("jar:" + u.toString() + "!/xml/giraffe2_exports.xml");
                } else {
                    url = new URL(u.toString() + "xml/giraffe2_exports.xml");
                }
            } catch (Exception ex) {
                logger.error(ex);
            }
            try {
                Exports exports = loadExportsFile(url);
                logger.debug("Reading exports file for classpath item "+u+".");
                for (String className : exports.getClazz()) {
                        try {
                            Class c = sysloader.loadClass(className);
                            this.exportedClasses.add(c);
                        } catch (Exception ex) {
                            logger.error("Exports file " + url.toString() + " explicitly lists " + className + " as an exported class, however, no such class could be loaded.", ex);
                            continue;
                        }
                }
            } catch (Throwable ex) {
                logger.debug("Could not unmarshall exports file for classpath item "+u+".");
            }
        }
    }

    private Exports loadExportsFile(java.net.URL exportsFile) throws java.lang.Throwable{
        Exports exports = null;
        if (null == exportsFile) {
            return null;
        }
        return hu.sztaki.ilab.giraffe.core.xml.XMLUtils.unmarshallXmlFile(
                hu.sztaki.ilab.giraffe.schema.exports.Exports.class,
                "hu.sztaki.ilab.giraffe.schema.exports:",
                "http://info.ilab.sztaki.hu/giraffe/schema/exports", exportsFile);
    }
}
