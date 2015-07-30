package au.org.aurin.scatsanalysis;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;

import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.core.data.AccumuloFeatureStore;
import org.locationtech.geomesa.utils.text.WKTUtils$;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import scala.Tuple2;

/**
 * @author Siqi Wu
 * 
 * Class used as an helper for RDD composed of Scats
 */
public class JavaRDDScats {

  /**
   * Action that writes an RDD composed of K,V pair to a geo-enabled Accumulo
   * table, at the appending/overwriting of an existing table
   *
   * @param rddIn
   *          RDD to write
   * @param options
   *          Parameters needed to create a feature store (passing a
   *          FeatureStore would not do, since it should point to a different
   *          feature store on every node)
   *
   * @return N of features written
   * 
   * @throws IOException
   * @throws SchemaException
   */
  public static long saveToGeoMesaTable(JavaPairRDD<String, Integer> rddIn,
      ScatsAnalysisOptions options) throws IOException,SchemaException {
    	
	  // The following closure is executed once per partition
	  JavaRDD<Integer> countFeatures = rddIn
	      .mapPartitions((Iterator<Tuple2<String, Integer>> iter) -> {
	        
	        // Builds a feature collection
	        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
	        AccumuloFeatureStore featureSource = ScatsFeatureStore
	            .getFeatureType(options);
	        Integer nFeatures = 0;
	        
	        // Adds all the features in the partition to the feature collection
	        while (iter.hasNext()) {
	          Tuple2<String, Integer> pair = iter.next();
	          featureCollection.add(toSimpleFeature(pair._1, pair._2));
	          nFeatures++;
	        }
		
	        // Writes the feature collection to GeoMesa
	        featureSource.addFeatures(featureCollection);
		
	        // Returns the number of features written
	        Integer[] result = { nFeatures };
	        return Arrays.asList(result);
	      });

    // Triggers the evaluation and returns the number of features written
    long totalFeatures = 0;
    for (Integer n : countFeatures.collect()) {
      totalFeatures += n;
    }

    return totalFeatures;
  }
  
  /**
   * Writes an JavaPairRDD<String, Integer> to an HDFS file, overwriting the
   * file if it exists. NODE: this should be in a helper class, since it
   * applies to a wider range than a JavaRDD made of scats
   *
   * @param rddIn
   *          RDD to write
   * @param fileOut
   *          Name of file to write to
   */
  public static void savePairRDDAsHDFS(JavaPairRDD<String, Integer> rddIn,
      String fileOut) {
    try {
      URI fileOutURI = new URI(fileOut);
      URI hdfsURI = new URI(fileOutURI.getScheme(), null, fileOutURI.getHost(),
          fileOutURI.getPort(), null, null, null);
      Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
      FileSystem hdfs = org.apache.hadoop.fs.FileSystem
          .get(hdfsURI, hadoopConf);
      hdfs.delete(new org.apache.hadoop.fs.Path(fileOut), true);
      rddIn.saveAsTextFile(fileOut);
    } catch (URISyntaxException | IOException e) {
      Logger.getRootLogger().error(e);
    }
  }
  
  /**
   * Returns the Scats marshalled in SimpleFeature according to the given schema
   * 
   * @param schema
   *          Schema of the feature type
   * 
   * @return simple feature applied certained schema
   * @throws SchemaException
   */
  public static SimpleFeature toSimpleFeature(String key, int vehicleCount)
      throws SchemaException {
    // Replace lon/lat by provided geospatial information
    Double lon = 50.0;
    Double lat = 50.0;
	  
    String[] tokens = key.split("-");
    int year = Integer.parseInt(tokens[1]);
    int monthOfYear = Integer.parseInt(tokens[2]);
    int dayOfMonth = Integer.parseInt(tokens[3]);
    int hourOfDay = Integer.parseInt(tokens[4]);
    int minuteOfHour = Integer.parseInt(tokens[5]) * 15 - 15;
    DateTime timestamp = new DateTime(year, monthOfYear, dayOfMonth, hourOfDay,
        minuteOfHour, DateTimeZone.UTC);
    String siteNo = tokens[0];
	  
    // Creates a new feature
    SimpleFeatureType simpleFeatureType = ScatsFeatureStore
        .createSchema("tweet");
    String id = key;
    Object[] VALUES = { timestamp.toDate(), siteNo,
        WKTUtils$.MODULE$.read("POINT(" + lon + " " + lat + ")"),
        vehicleCount };
    SimpleFeature simpleFeature = SimpleFeatureBuilder.build(simpleFeatureType,
        VALUES, id);
    simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID,
        java.lang.Boolean.TRUE);

    return simpleFeature;
  }

}
