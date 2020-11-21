package cpts415

import java.util

import breeze.linalg.*
import breeze.numerics.constants.g
import org.apache
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SELECT
import org.apache.log4j.{Level, Logger}
import org.apache.{spark, _}
import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.sql
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.graphframes.GraphFrame
import org.graphframes.lib.AggregateMessages.edge
import shapeless.ops.nat.GT.>
import shapeless.ops.nat.LT.<

import scala.{+:, :+}
import scala.xml.NodeSeq.Empty.\

object driver {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)


  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "D:\\360Downloads\\Hadoop\\winutils-master\\hadoop-2.7.1\\")
    /**
     * Start the Spark session
     */
    val spark = SparkSession
      .builder()
      .appName("cpts415-Group5-milestone3")
      .config("spark.some.config.option", "some-value").master("local[*]")
      .getOrCreate()
    spark.sparkContext.addFile("data/d1.csv")
    spark.sparkContext.addFile("data/d3.csv")


    /**
     * Load CSV file into Spark SQL
     */
    // You can replace the path in .csv() to any local path or HDFS path
    var df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("d1.csv"))
    var df1 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("d3.csv"))
    df.createOrReplaceGlobalTempView( "d1")
    df1.createOrReplaceGlobalTempView( "d3")
    //nw_d3.show()

    /**
     * The basic Spark SQL functions
     */



    //build edge dataframe from edgeSrc contain two column :  src (source vertex ID of edge) and dst (destination vertex ID of edge)
    /*var dfSeq = Vector(spark.sql(
      """
        |SELECT videoID,Column10
        |FROM  global_temp.d1
        |""".stripMargin),spark.sql(
      """
        |SELECT videoID,Column11
        |FROM  global_temp.d1
        |""".stripMargin),
      spark.sql("""SELECT videoID,Column""" + "12" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "13" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "14" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "15" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "16" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "17" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "18" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "19" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "20" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "21" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "22" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "23" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "24" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "25" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "26" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "27" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "28" + """ FROM  global_temp.d1 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "29" + """ FROM  global_temp.d3 """.stripMargin),
    )*/


    var dfSeq = Vector(
      spark.sql("""SELECT videoID,Column""" + "1" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "2" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "3" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "4" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "5" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "6" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "7" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "8" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "9" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "10" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "11" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "12" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "13" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "14" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "15" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "16" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "17" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "18" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "19" + """ FROM  global_temp.d3 """.stripMargin),
      spark.sql("""SELECT videoID,Column""" + "20" + """ FROM  global_temp.d3 """.stripMargin),
    )

    val mergeDfSeq = dfSeq.reduce(_ union _)
    var edge = mergeDfSeq.toDF("src","dst").na.drop()
    print(edge.count())

    // vertex dataframe contain a special column named id which specifies unique IDs for each vertex in the graph.
    var vertex =
      spark.sql(
        """
          |SELECT videoID AS id, uploader, age, category, length, rate, views, ratings
          |FROM  global_temp.d3
          |""".stripMargin).na.drop()

    //edge.show()


    /**
     * The GraphFrame function.
     */

    // Create a GraphFrame
    val g = GraphFrame(vertex,edge)

    /**
     * Pattern matching: https://graphframes.github.io/graphframes/docs/_site/user-guide.html#subgraphs
     */
    // show isolated vertices
    //g.dropIsolatedVertices().vertices.show()


    // Select a path based on edges "e" of type "follow"
    // pointing from a younger user "a" to an older user "b".
    val paths = { g.find("(a)-[e]->(b)").filter("a.age < b.age"). filter("a.length > 200").filter("a.rate > 3.2") }
    val e2 = paths.select("e.*")
    e2.toDF().show()


    /**
     * Common graph queries
     */
    // Query: Get in-degree and out-degree of each vertex.
    /*var inDegreesT = g.inDegrees
    var outDegreesT = g.outDegrees
    inDegreesT.createOrReplaceGlobalTempView("inDegree")
    outDegreesT.createOrReplaceGlobalTempView("outDegree")

    inDegreesT.select( "id", "inDegree" ).orderBy(desc("inDegree")).limit(10).show()
    outDegreesT.select( "id", "outDegree" ).orderBy(desc("outDegree")).limit(10).show()

      spark.sql(
        """
          |SELECT AVG(inDegree)
          |FROM  global_temp.inDegree
          |""".stripMargin).show()
      spark.sql(
      """
        |SELECT AVG(outDegree)
        |FROM  global_temp.outDegree
        |""".stripMargin).show()
      spark.sql(
      """
        |SELECT MIN(inDegree)
        |FROM  global_temp.inDegree
        |""".stripMargin).show()
       spark.sql(
      """
        |SELECT MIN(outDegree)
        |FROM  global_temp.outDegree
        |""".stripMargin).show()*/

    //Top K videos
    var graphVertex = g.vertices
    var graphEdge = g.edges
    graphVertex.createOrReplaceGlobalTempView("graphVertex")


    graphVertex.select( "category" ).orderBy(desc("category")).dropDuplicates(). show(10)
    graphVertex.select( "uploader" ).orderBy(desc("uploader")).dropDuplicates().show(10)
    graphVertex.select( "age" ).orderBy(desc("age")).show(10)

    /*
        * other queries
        */



  /*  spark.sql(
      """
        |SELECT id,uploader
        |FROM  global_temp.graphVertex
        |WHERE age >1000 AND age <1300
        |""".stripMargin).show()

    spark.sql(
      """
        |SELECT id,uploader
        |FROM  global_temp.graphVertex
        |WHERE length > 300 AND length < 500
        |""".stripMargin).show()*/





    /**
     * Run PageRank algorithm, find out relativity to that rows same as their corresponding properties
     */
   val results = g.pageRank.resetProbability(0.01).maxIter(20).run()
    results.vertices.select("id", "pagerank", "age", "category", "length", "rate", "views", "ratings").orderBy(desc("pagerank")).show(20)





    /**
     * Stop the Spark session
     */
    spark.stop()
  }
}
