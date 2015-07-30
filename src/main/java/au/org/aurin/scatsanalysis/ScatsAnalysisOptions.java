package au.org.aurin.scatsanalysis;

import org.kohsuke.args4j.Option;
import au.org.aurin.tweetcommons.GeoMesaOptions;

/**
 * @author Siqi Wu
 * 
 * Class used to process the CLI interface options specific to ScatsCruncher
 */
public class ScatsAnalysisOptions extends GeoMesaOptions {
  
  private static final long serialVersionUID = 4434545562158958553L;
  
  // Options definitions
  @Option(name = "--readingsFile", required = true, usage = "the HDFS file "
      + "holding the SCATS data to process")
  public String readingsFile;

}
