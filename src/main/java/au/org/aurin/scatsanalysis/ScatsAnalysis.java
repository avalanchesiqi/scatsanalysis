package au.org.aurin.scatsanalysis;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.geotools.feature.SchemaException;
import org.kohsuke.args4j.CmdLineException;
import org.locationtech.geomesa.utils.text.WKTUtils$;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Class used to aggregate Scats data by intersection number and timestamp
 * 
 * @author Siqi Wu
 */
public class ScatsAnalysis {
  private static ScatsAnalysisOptions options;

  /**
   * @param args
   * 
   * @throws IOException
   * @throws SchemaException
   * @throws ParseException
   * @throws NoSuchFieldException
   * @throws SecurityException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws CmdLineException
   */
  public static void main(String[] args)
      throws IOException, SchemaException, ParseException, NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException, CmdLineException {
    
    ScatsAnalysis.options = new ScatsAnalysisOptions();
    ScatsAnalysis.options.parse(args);

    SparkConf sparkConf = new SparkConf();
    final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    // Reads the file of sensor readings
    JavaRDD<String> input = sparkContext
        .textFile(ScatsAnalysis.options.readingsFile);

    // Transform input lines into record strings
    // Format:
    // ["100-2009-06-26-0-1 30", "100-2009-06-26-0-2 30", "100-2009-06-26-0-3-21", ...]
    JavaRDD<String> records = input.flatMap((String s) -> {
      return Arrays.asList(parseRecord(s));
    });

    // Transform record strings into record pairs
    // Format:
    // "100-2009-06-26-0-1": 30
    // "100-2009-06-26-0-2": 30
    // "100-2009-06-26-0-3": 21
    // "100-2009-06-26-0-4": 21
    // ...
    JavaPairRDD<String, Integer> recordPairs = records.mapToPair((String s) -> {
      return new Tuple2<String, Integer>(s.split("\\s+")[0], Integer.parseInt(s.split("\\s+")[1]));
    });
    
    JavaPairRDD<String, Integer> aggregate = recordPairs.reduceByKey((Integer a, Integer b) -> {
      return a + b;
    });
        
    // Ensures the Accumulo Feature Type is created
    ScatsFeatureStore.createFeatureType(ScatsAnalysis.options);
    // Saves clean Scats to Accumulo
    System.out.println("**** n. features written: "
        + JavaRDDScats.saveToGeoMesaTable(aggregate,
        		ScatsAnalysis.options));
    // Saves clean Scats to HDFS file
    aggregate.saveAsTextFile("hdfs://localhost:9000/output");
//    JavaRDDScats.savePairRDDAsHDFS(aggregate, "hdfs://localhost:9000/scats/output");

    // Closes the context
    sparkContext.close();
  }
  
  /**
   * Parse SCATS text and returns site-timestamp-vc information
   * 
   * @param record
   *          Record to process
   */
  protected static String[] parseRecord(String line) {
	  String[] records = new String[96];
	  String[] parsedFields = line.replaceAll("\"", "").split(",");
	  String siteNo = parsedFields[0];
	  String date = parsedFields[1].split("\\s+")[0];
	  int len = parsedFields.length;
	  for (int i=0; i<24; i++) {
		  for (int j=0; j<4; j++) {
		    if (i*4+j+3 >= len) {
		      records[i*4+j] = siteNo + "-" + date + "-" + Integer.toString(i)
	            + "-" + Integer.toString(j+1) + " 0";
		    } else {
		      String vehicleCount = parsedFields[i*4+j+3];
	        if (vehicleCount.equals("") || vehicleCount.charAt(0) == '-') {
	          vehicleCount = "0";
	        }
	        records[i*4+j] = siteNo + "-" + date + "-" + Integer.toString(i)
	            + "-" + Integer.toString(j+1) + " " + vehicleCount;
		    }
		  }
	  }
	  return records;
  }

}
