package au.org.aurin.scatsanalysis;

import java.io.IOException;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.locationtech.geomesa.core.data.AccumuloDataStore;
import org.locationtech.geomesa.core.data.AccumuloFeatureStore;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Class implementing GeoMesa feature store to hold Scats records
 * 
 * @author Siqi Wu
 */
public class ScatsFeatureStore {

  protected static String schemaDef = "timestamp:Date, siteNo:String, location:Point:srid=4326, vehicleCount:int";

  public ScatsFeatureStore() {
    // TODO Auto-generated constructor stub
  }

  /**
   * Returns a SimpleFeatureType to hold Scats with simpleFeatureTypeName
   * 
   * @param simpleFeatureTypeName
   *          Accumulo table name for the schema
   * @throws SchemaException
   */

  public static SimpleFeatureType createSchema(String simpleFeatureTypeName)
      throws SchemaException {
    
    SimpleFeatureType simpleFeatureType = DataUtilities.createType(
        simpleFeatureTypeName, ScatsFeatureStore.schemaDef);

    return simpleFeatureType;
  }

  /**
   * Returns the FeatureSource of name ftName (returns null if not existing)
   * 
   * @param options
   *          Datastore parameters
   *
   * @return
   * @throws IOException
   * @throws SchemaException
   */
  public static AccumuloFeatureStore getFeatureType(ScatsAnalysisOptions options)
      throws IOException, SchemaException {

    // Verifies that we can see this GeoMesa feature type
    DataStore dataStore = DataStoreFinder.getDataStore(options
        .getAccumuloOptions());
    if (dataStore == null) {
      return null;
    }

    // Creates the FeatureType if not existing
    if (dataStore.getSchema(options.tableName) == null) {
      return null;
    }

    return (AccumuloFeatureStore) dataStore.getFeatureSource(options.tableName);
  }

  /**
   * Creates if not existing a FeatureType in Accumulo, adn drops and re-creates
   * it if the overwrite options is set
   *
   * @param options
   *          Datastore parameters
   *
   * @return
   * @throws IOException
   * @throws SchemaException
   */
  public static void createFeatureType(ScatsAnalysisOptions options)
      throws IOException, SchemaException {

    // Verifies that we can see this Accumulo feature type
    AccumuloDataStore dataStore = (AccumuloDataStore) (DataStoreFinder
        .getDataStore(options.getAccumuloOptions()));
    assert dataStore != null;

    // Drops the Feature Type if the overwrite options is set
    if (options.overwrite && dataStore.getSchema(options.tableName) != null) {
      dataStore.removeSchema(options.tableName);
    }

    // Creates the FeatureType if not existing
    if (dataStore.getSchema(options.tableName) == null) {
      dataStore.createSchema(ScatsFeatureStore.createSchema(options.tableName));
    }
  }
  
}
