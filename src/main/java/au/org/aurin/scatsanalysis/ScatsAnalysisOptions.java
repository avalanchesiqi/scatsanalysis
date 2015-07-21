package au.org.aurin.scatsanalysis;

import java.io.Serializable;
import org.kohsuke.args4j.Option;
import au.org.aurin.tweetcommons.GeoMesaOptions;

/**
 * Class used to process the CLI interface options specific to ScatsCruncher
 * 
 * @author Siqi Wu
 */
public class ScatsAnalysisOptions extends GeoMesaOptions implements Serializable {

  private static final long serialVersionUID = -4752033547428169583L;

  // Options definitions
  @Option(name = "--readingsFile", required = true, usage = "the HDFS file holding the SCATS data to process")
  public String readingsFile;

}
