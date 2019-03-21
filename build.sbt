name := "ElasticsearchCRUD"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies ++= Seq("org.elasticsearch"%"elasticsearch"%"6.4.0",
                            "org.elasticsearch.client"%"elasticsearch-rest-high-level-client"%"6.4.0",
                            "org.elasticsearch.client"%"transport"%"6.4.0",
  "com.typesafe.play"%"play-json_2.12"%"2.6.9",
  "com.typesafe" % "config" % "1.2.1")

    assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
   // case "properties/Elasticsearch.properties" => MergeStrategy.first
    case x => MergeStrategy.first
  }

