package processor


import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.{DateTimeException, LocalDate, ZoneId}
import java.util
import java.util.stream.IntStream
import java.util.{Calendar, Date}

import bulkprocessor.EsBulkProcessor
import create.ElasticOperations
import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
object TImeSeriesIndexCreation {


    def main(args: Array[String]) {

      /**
        * Reading variables from configuration
        */


      val indexName: String = "indexName"
      val typeName: String = "typeName"

      /**
        * Creating Rest Client and BulkProcessor
        */
      val client: RestHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")))
      val esBulkProcessor = new EsBulkProcessor()
      val bulk = esBulkProcessor.esBulkProcess(client)


      /**
        * Object of class consists all the CRUD operations
        */
      val operation = new ElasticOperations()

      val filePath = "C:\\D_DRIVE\\Month_Docs.json"
      val newFile = "C:\\D_DRIVE\\properties.conf"
      var path = Paths.get(newFile);
      var allLines = Files.readAllLines(path, StandardCharsets.UTF_8)

      try {
        var start = LocalDate.of(2019, Integer.parseInt(args(0)), 1)
        var stop = LocalDate.of(2019, Integer.parseInt(args(1)), 28)

        var localDates = new util.ArrayList[LocalDate]()
        var localDate = start


        allLines.forEach(newvar => {
        while (localDate.isBefore(stop)) {
          localDates.add(localDate)

          localDate = localDate.plusDays(1)

          val cl: Calendar = Calendar.getInstance
          cl.setTime(Date.from(localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant))
          var weekCount = cl.get(Calendar.WEEK_OF_YEAR)


          IntStream.range(1, 81).forEach(count => {
            operation.write(indexName + "2019_" + "week" + weekCount + "_index", typeName, newvar + "_" + localDate + "_D000000" + count, parseJSONFile(
              filePath, localDate.toString, newvar), client, bulk)
          })
        }
          })

      } catch {
        case e: DateTimeException => println("exception caught: " + e);

      }
      //EsQueryBuilder.searchMultipleIndexes(typeName, client, "indexName1", "indexName2")



      def parseJSONFile(filename: String, date: String, data: String): String = {
        var s = new String(Files.readAllBytes(Paths.get(filename)))

        s = s.replace("2018-07-23", date)
        s = s.replace("string", data)
        return s
      }

    }



}
