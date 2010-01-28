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
package hu.sztaki.ilab.giraffe.core.ui;

import hu.sztaki.ilab.giraffe.core.conversion.ConversionManager;
import hu.sztaki.ilab.giraffe.core.factories.ProcessFactory;
import hu.sztaki.ilab.giraffe.core.processingnetwork.ProcessMonitorBase;
import hu.sztaki.ilab.giraffe.core.util.Pair;
import hu.sztaki.ilab.giraffe.core.xml.ConfigurationManager;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.log4j.Logger;

/*
 * Lots of promised features are still missing. Notably: 
 * TODO(neumark): meta.xml process element should only have child events which are used!
 * TODO(neumark): implement a progress bar for processing job.
 * TODO(neumark): nameserver values passed on the command line are ignored
 * TODO(neumark): The dataSourceLastModified should be used to set this value in processedData objects if avaialable.
 * TODO(neumark): quotes should be available for columns of output files. (FIX this in CSVWriter, spec is ignored right now!).
 * TODO(neumark): The fact file (incorrectly) uses the same quote data for each column as the input format. 
 *
 */
/**
 * Weblog ETL tool
 * The goal of this program is to transform weblogs into a standard format, which is easily loaded into a database. There are several stages involved.
 * The basic unit of processing is a single line of the logfile. Each line goes through the following steps:
 * <ol>
 * <li> It is read from the file by a <b>CSVReader</b> object.</li>
 * <li> It is split into <i>tokens</i> by the <b>Tokenizer</b>.</li>
 * <li> The tokens are then processed by a <b>TokenReader</b>, which outputs a fixed number of <i>columns</i>.</li>
 * <li> The columns which require further processing are then sent to a <b>DataProcessor</b>.</li>
 * <li> The <b>DataProcessor</b> forwards the content of its column to each <b>ProcessingEngine</b> which does the actual data processing and takes care of caching.</li>
 * <li> Unprocessed columns are copied to the output file <i>processed_log.txt</i> unmodified, while processed columns contain only the index number of the input data.</li>
 * <li> Once all the data processing is completed, the <b>lookup tables</b> which map indexes to processed data are also written out.
 * </ol> 
 * In addition to the output log and the lookup tables, all important events are written to a <i>log file</i>. The detail of the logging is configurable.
 * A file named <i>meta.xml</i> is also created, which stores all information about the processing job.
 * @author neumark
 */
public class Cli {
    // from: http://blog.blip.tv/blog/2005/07/24/log4jxml-inside-your-jar/

    private static Logger logger = null;

    private static void configureLogging() {
        /*
         * If log4j.configuration system property isn't set,
         * then assume I'm inside a jar and configure
         * log4j using the config file that shipped in the jar.
         */
        if (System.getProperty("log4j.configuration") == null) {
            java.net.URL url = ClassLoader.getSystemResource("xml/log4j.xml");
            System.out.println("Using log4j configuration file at " + url);
            DOMConfigurator.configure(url);
        }
    }

    public static String printUsage() {
        return "USAGE: giraffecli --process=PROCESS [optional parameters] LOGFILE(S)\n" +
                "where optional parameters are:\n" +
                "  --process_definitions=PD --usecache=boolean --keepkeyset=boolean --output=OUTPUTDIR --data=DATADIR\n" +
                "  --nameserver=NSIP\n" +
                " The order of parameters does not matter, but they must precede log files.\n" +
                " boolean is 'true' or 'false'. \n" +
                " PD is the location of the process definitions file. If omitted, attempts to use system property\n" +
                "   processDefinitions.\n" +
                " PROCESS is the name of the process to execute.\n" +
                " LOGFILE(S) are the name(s) of the log file(s) to be processed (wildcards allowed).\n" +
                " usecache=true reuses the existing dictionary in $datadir/dictionary (default: false).\n" +
                " keepkeyset=true leaves the keys/ subdirectory of the output dir after processing (default: false).\n" +
                " OUTPUTDIR is the location of the processed log information (default is the name of the log file). \n" +
                " DATADIR is the location of the data files used by weblog_etl (default is ./resources/)." +
                " NSIP is the ip address of the domain name server to use for r-DNS lookups.";
    }

    public static class CLIProcessMonitor extends ProcessMonitorBase {

        // Override customEvent to provide a friendlier message:
        public void customEvent(Class source, String eventIdentifier, Object details) {
            if (source.equals(hu.sztaki.ilab.giraffe.core.io.FileReader.class)) {
                if ("openNextFile".equals(eventIdentifier)) {
                    try {
                        Pair<String,Boolean> status = (Pair<String,Boolean>) details;
                        if (status.second) {
                            logger.info("Successfully opened input file "+status.first+" for reading.");
                        } else {
                            logger.error("Error opening input file "+status.first+" for reading.");
                        }
                    } catch (Exception ex) {logger.error(ex);}
                    return;
                }
                if ("EOF".equals(eventIdentifier)) {
                    logger.info("Reach end of current input file.");
                    return;
                }
            }
            super.customEvent(source, eventIdentifier, details);
        }
    }

    public static void main(String[] args) {
        try {
            // Configure log4j.
            configureLogging();
            logger = Logger.getLogger(Cli.class);
            System.out.println("giraffe2 cli (c) 2008-2010 SZTAKI ilab");
            // Process command-line parameters.
            ConfigurationManager configurationManager = new ConfigurationManager();
            if (!configurationManager.processCommandLineArgs(args)) {
                System.out.println(printUsage());
                return;
            }
            ConversionManager conversionManager = new ConversionManager(configurationManager);
            ProcessFactory processFactory = new ProcessFactory(configurationManager, conversionManager);
            hu.sztaki.ilab.giraffe.core.processingnetwork.Process process = processFactory.get(configurationManager.getRequestedProcessName(), new CLIProcessMonitor());
            if (null == process) {
                return;
            }
            if (!process.start()) {
                logger.error("Failed to finish processing task.");
                System.exit(1);
            }
            logger.info("Processing completed successfully.");
            // Record program end time.
            /*
            process.getProcessMetaData().getStatistics().getTimes().setProgramEnd(Calendar.getInstance());
            logger.info("Writing meta.xml");
            if (!XMLUtils.writeReportXml(process.getProcessMetaData(), new File(process.getProcessMetaData().getRunConfiguration().getPaths().getOutputDir() + "/meta.xml"))) {
            logger.error("Failed to marshal meta.xml file.");
            System.exit(1);
            }
             */
        } catch (Exception ex) {
            logger.error("Uncaught exception encountered (logged).", ex);
            System.exit(1);
        }
    }
}
