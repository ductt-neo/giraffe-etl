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

import hu.sztaki.ilab.giraffe.core.Globals;
import hu.sztaki.ilab.giraffe.schema.report.Report;
import java.io.File;
import javax.xml.bind.ValidationEventHandler;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import org.apache.log4j.Logger;

/**
 *
 * @author neumark
 */
public class XMLUtils {

    private static Logger logger = Logger.getLogger(XMLUtils.class);

    @SuppressWarnings("unchecked")
    public static <K> K unmarshallXmlFile(Class<K> targetClass, String targetNamespace, java.net.URL inputURL) {
        try {
            javax.xml.bind.JAXBContext jaxbCtx = javax.xml.bind.JAXBContext.newInstance(targetClass);
            javax.xml.bind.Unmarshaller unmarshaller = jaxbCtx.createUnmarshaller();
            unmarshaller.setSchema(getSchema(targetNamespace));
            unmarshaller.setEventHandler(new ValidationEventHandler() {
                public boolean handleEvent(javax.xml.bind.ValidationEvent evt) {
                    logger.error("JAXB unmarshall event: "+evt.getMessage());
                    return true;
                }
            }
            );
            try {
            return (K) ((javax.xml.bind.JAXBElement<K>) unmarshaller.unmarshal(inputURL)).getValue();
            } catch (ClassCastException e) {
                return (K) unmarshaller.unmarshal(inputURL);
            }
        } catch (Exception ex) {
            logger.error("Error unmarshalling " + inputURL, ex);
        }
        return null;
    }

    public static <K> K unmarshallXmlFile(Class<K> targetClass, String packages, String targetNamespace, java.net.URL inputURL) {
        try {
            javax.xml.bind.JAXBContext jaxbCtx = javax.xml.bind.JAXBContext.newInstance(packages);
            javax.xml.bind.Unmarshaller unmarshaller = jaxbCtx.createUnmarshaller();
            unmarshaller.setSchema(getSchema(targetNamespace));
            unmarshaller.setEventHandler(new ValidationEventHandler() {
                public boolean handleEvent(javax.xml.bind.ValidationEvent evt) {
                    logger.error("JAXB unmarshall event: "+evt.getMessage());
                    return true;
                }
            }
            );
            try {
            return (K) ((javax.xml.bind.JAXBElement<K>) unmarshaller.unmarshal(inputURL)).getValue();
            } catch (ClassCastException e) {
                return (K) unmarshaller.unmarshal(inputURL);
            }
        } catch (Exception ex) {
            logger.error("Error unmarshalling " + inputURL, ex);
        }
        return null;
    }

    public static boolean writeReportXml(Report report, File output) throws javax.xml.bind.JAXBException, org.xml.sax.SAXException {

        javax.xml.bind.JAXBContext jaxbCtx = javax.xml.bind.JAXBContext.newInstance(report.getClass());
        javax.xml.bind.Marshaller marshaller = jaxbCtx.createMarshaller();
        marshaller.setProperty(javax.xml.bind.Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.setProperty(javax.xml.bind.Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        //marshaller.setProperty(javax.xml.bind.Marshaller.JAXB_SCHEMA_LOCATION, getSchemaForNamespace("http://info.ilab.sztaki.hu/giraffe/schema/report"));
        try {
            marshaller.setSchema(getSchema("http://info.ilab.sztaki.hu/giraffe/schema/report"));
        } catch (org.xml.sax.SAXException ex) {
            logger.error("Schema error, validation failed! ");
            logger.error("Validation details:", ex);
        } catch (java.io.IOException ex) {
            logger.error("IO exception encountered during validation ", ex);
        } catch (java.net.URISyntaxException ex) {
            logger.error("URI syntax error during validation ", ex);
        }
        marshaller.marshal(report, output);
        return true;
    }

    public static Schema getSchema(String namespace) throws org.xml.sax.SAXException, java.io.IOException, java.net.URISyntaxException {
        SchemaFactory sf = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);
        java.net.URI schemaURI = getSchemaForNamespace(namespace);
        sf.setResourceResolver(Globals.getCatalogResolver());
        Schema schema = sf.newSchema(schemaURI.toURL());
        return schema;
    }

    public static java.net.URI getSchemaForNamespace(String namespace) throws java.io.IOException, java.net.URISyntaxException {
        return new java.net.URI(Globals.getCatalogResolver().resolveURI(namespace));
    }

    public static void main(String[] args) {
        try {
            getSchema("http://info.ilab.sztaki.hu/giraffe/conversions");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}

