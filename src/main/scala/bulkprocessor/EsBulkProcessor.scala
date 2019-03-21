package bulkprocessor

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Date, TimeZone}
import java.util.function.BiConsumer

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}

class EsBulkProcessor {

  /**
    * This function is used to load Bulk Data in Elasticsearch
    * @param client
    * @return
    */
  def esBulkProcess(client: RestHighLevelClient): BulkProcessor = {
    val bulkAsync: BiConsumer[BulkRequest, ActionListener[BulkResponse]] = new BiConsumer[BulkRequest, ActionListener[BulkResponse]] {
      override def accept(bulkRequest: BulkRequest, actionListener: ActionListener[BulkResponse]): Unit = {
        client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, actionListener)
      }
    }

    var listener = new BulkProcessor.Listener() {
      /**
        * Run before Bulk Execution
        * @param executionId
        * @param request
        */
      override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
        println(s"No of Request to be processed before bulk ${request.numberOfActions()}")
        val tz1: TimeZone = TimeZone.getTimeZone("UTC")
        val df1: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'") // Quoted "Z" to indicate UTC, no timezone offset
        df1.setTimeZone(tz1)
        val nowAsISO1: String = df1.format(new Date())
        println(nowAsISO1+" "+executionId)
      }

      /**
        * This function will run after the Bulk Execution
        * @param executionId
        * @param request
        * @param response
        */
      override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
        if (response.hasFailures) {
          println(s"Failures Recorded :  ${response.buildFailureMessage()}")
        }
        println(s"No of Request after bulk ${request.numberOfActions()}")
        val tz1: TimeZone = TimeZone.getTimeZone("UTC")
        val df1: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'") // Quoted "Z" to indicate UTC, no timezone offset
        df1.setTimeZone(tz1)
        val nowAsISO1: String = df1.format(new Date())
        println(nowAsISO1+" "+executionId)

      }

      override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
        println(s"No of Request after bulk ${request.numberOfActions()}")
      }
    }

    BulkProcessor.builder(bulkAsync, listener)
      .setBulkActions(3000)
      .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
      .setConcurrentRequests(0)
      .setFlushInterval(TimeValue.timeValueMillis(125))
      .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(60), 3))
      .build()
  }

}
