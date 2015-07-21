package au.org.aurin.scatsanalysis;

import org.kohsuke.args4j.Option;

import au.org.aurin.tweetcommons.CLIOptions;

/**
 * Class used to process the CLI interface if ScatsAnalysis
 * 
 * @author Siqi Wu
 *
 */
public class ScatsAnalysisOptionsTest extends CLIOptions {

  // Options definitions
  @Option(name = "--readingsFile", required = true, usage = "the HDFS file holding the SCATS data to process")
  public String readingsFile;
  
}
