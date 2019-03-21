package create


import org.elasticsearch.action.bulk.{BulkProcessor, BulkRequest}
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.unit.Fuzziness
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentType}
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.search.builder.SearchSourceBuilder
import play.api.libs.json.JsValue


class ElasticOperations {

  /**
    * Writing JSON in Elasticsearch
    * @param indexName
    * @param typeName
    * @param id
    * @param json
    * @param client
    * @param bulk
    */
  def write(indexName:String, typeName:String, id:String, json:String, client:RestHighLevelClient, bulk:BulkProcessor ){

    val indexRequest:IndexRequest = new IndexRequest(indexName,typeName,id).source(json,XContentType.JSON)
      //.opType("create")

    // val indexResponse:IndexResponse = client.index(indexRequest, RequestOptions.DEFAULT)
    //
     bulk.add(indexRequest)

  }

  /**
    * Updating JSON in Elasticsearch using ID
    * @param indexName
    * @param typeName
    * @param id
    * @param json
    * @param bulk
    */
  def update(indexName:String,typeName:String,id:String, json: JsValue,client: RestHighLevelClient,bulk:BulkProcessor){

   val indexRequest:UpdateRequest = new UpdateRequest(indexName,typeName,id).doc(json,XContentType.JSON)
    indexRequest.docAsUpsert(true)
    bulk.add(indexRequest)

  }

  /**
    * Deleting Document From Elasticsearch
    * @param indexName
    * @param typeName
    * @param id
    * @param client
    * @param bulk
    */

  def deleteDocument(indexName:String,typeName:String,id:String,client:RestHighLevelClient,bulk:BulkProcessor){

   val indexRequest:DeleteRequest = new DeleteRequest(indexName,typeName,id)
    bulk.add(indexRequest)

  }

  /**
    * Searching Elasticsearch
    * @param indexName
    * @param typeName
    * @param client
    */
  def search(indexName:String,typeName:String,client:RestHighLevelClient): Unit = {

    val searchRequest: SearchRequest = new SearchRequest(indexName)
    searchRequest.types(typeName)

    val sourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    sourceBuilder.from(0)
    sourceBuilder.size(5)

    val matchQueryBuilder: QueryBuilder = QueryBuilders.matchQuery("key", "value")
      .fuzziness(Fuzziness.AUTO)
      .prefixLength(3)
      .maxExpansions(10)
    sourceBuilder.query(matchQueryBuilder)
    searchRequest.source(sourceBuilder)

    var searchResponse: SearchResponse = client.search(searchRequest, RequestOptions.DEFAULT)



  }

}
