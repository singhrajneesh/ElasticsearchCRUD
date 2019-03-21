package reader

import java.io.FileInputStream
import java.util.Properties

import configuration.ConfigurationProp


object ElasticsearchReader {

    /**
    * Reading from Properties File
    */
    val elasticMap = collection.mutable.Map.empty[String, String]
    val prop = new Properties
    val input = new FileInputStream(ConfigurationProp.CONFIG_FOLDER+ConfigurationProp.URI_SEPARATOR+ConfigurationProp.PROPERTIES_FILE_NAME)
    prop.load(input)
    elasticMap.put("ES_HOST", prop.getProperty("ES_HOST"))
    elasticMap.put("ES_PORT", prop.getProperty("ES_PORT"))
    elasticMap.put("INDEX_NAME", prop.getProperty("INDEX_NAME"))
    elasticMap.put("TYPE_NAME", prop.getProperty("TYPE_NAME"))
    elasticMap.put("FLUSH_INTERVAL", prop.getProperty("FLUSH_INTERVAL"))
    elasticMap.put("CONCURRENT_REQUEST", prop.getProperty("CONCURRENT_REQUEST"))
    elasticMap.put("BULKSIZE_IN_MB", prop.getProperty("BULKSIZE_IN_MB"))
    elasticMap.put("BULK_SIZE", prop.getProperty("BULK_SIZE"))

}
