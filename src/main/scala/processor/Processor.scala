package processor

import create.ElasticOperations
import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}
import reader.ElasticsearchReader.elasticMap
import bulkprocessor.EsBulkProcessor
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import play.api.libs.json.Json

import scala.io.Source



object Processor {
  def main(args: Array[String]) {

    /**
      * Reading variables from configuration
      */
   //val hostAddress:Array[String]=elasticMap.getOrElse("ES_HOST","").toString().split(",")
    val hostAddress=elasticMap.getOrElse("ES_HOST","")
    val port=elasticMap.getOrElse("ES_PORT","").toInt
    val indexName:String=elasticMap.getOrElse("INDEX_NAME","")
    val typeName:String=elasticMap.getOrElse("TYPE_NAME","")

    /**
      * Creating Rest Client and BulkProcessor
      */
     val  credentialsProvider:CredentialsProvider = new BasicCredentialsProvider()
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "Welcome123!"))


   /* RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200))
      .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
          return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
      });*/


    val client:RestHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(hostAddress, port, "http"))

      .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
        @Override
        def customizeHttpClient( httpClientBuilder:HttpAsyncClientBuilder):HttpAsyncClientBuilder={
          return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        }
      })
    )
    val esBulkProcessor = new EsBulkProcessor()
    val bulk = esBulkProcessor.esBulkProcess(client)
                                       /* new HttpHost(hostAddress(1), port, "http"),
                                        new HttpHost(hostAddress(2), port, "http"),
                                        new HttpHost(hostAddress(3), port, "http")))*/


    /**
      * Object of class consists all the CRUD operations
      */
    val operation=new ElasticOperations()


    val jsonDocument:XContentBuilder=XContentFactory.jsonBuilder()
    jsonDocument.startObject()
    jsonDocument.field("date","2071")
    jsonDocument.endObject()

    val jsonDocument1:XContentBuilder=XContentFactory.jsonBuilder()
    jsonDocument1.startObject()
    jsonDocument1.field("date","2078")
    jsonDocument1.endObject()

    val lines = Source.fromFile("C:\\Users\\rajsingh8\\Desktop\\ElaticsearchWriting\\Data.json").getLines.mkString
    val JsonFile=Json.parse(lines)
    val JsonString=Json.stringify(JsonFile)

    println(JsonString)

    for(i<-25 to 100)
      {
        operation.write(indexName,typeName,"hello"+i,JsonString,client,bulk)
      }
   //operation.search(indexName,typeName,client)

  }

}
