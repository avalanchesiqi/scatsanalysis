package au.org.aurin.scatsanalysis;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kohsuke.args4j.CmdLineException;
import org.locationtech.geomesa.utils.text.WKTUtils$;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Geometry;

import au.org.aurin.tweetcommons.Tweet;

public class ScatsAnalysisIT {

  static private ScatsAnalysisOptionsTest options;
  static private String[] args = System.getProperty("geomesa-args").split(
      "\\s+");

  static private final JavaSparkContext sc = new JavaSparkContext((new SparkConf())
      .setAppName("ScatsCruncherIT").setMaster("local[2]"));
  static private final Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

  static {
	  ScatsAnalysisIT.options = new ScatsAnalysisOptionsTest();
    try {
    	ScatsAnalysisIT.options.parse(args);
    } catch (NoSuchFieldException | SecurityException
        | IllegalArgumentException | IllegalAccessException | CmdLineException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testAggregateOfScats() {
  }

}
