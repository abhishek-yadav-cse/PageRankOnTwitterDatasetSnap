import org.apache.spark.SparkConf
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession


object Sample {

  def main(args: Array[String]): Unit = {


        val conf = new SparkConf().setMaster("local[2]")
        val spark = SparkSession
          .builder
          .appName("SampleApp").config(conf)
          .getOrCreate()
        val sc = spark.sparkContext

        // Load the edges as a graph
        val graph = GraphLoader.edgeListFile(sc, "twitter_combined.txt")

        // Calculating various attributes for all nodes - inDegree, outDegree and Degrees
        val inDegrees = graph.inDegrees
        inDegrees.collect()
        val outDegrees = graph.outDegrees
        outDegrees.collect()
        val degrees = graph.degrees
        degrees.collect()

        // Number of iterations as the argument and compute the pagerank
        val staticPageRank = graph.staticPageRank(3)
        staticPageRank.vertices.collect()
        val pageRank = graph.pageRank(0.001).vertices


        //Printing the top 10 nodes according to pagerank
        println(staticPageRank.vertices.top(10)(Ordering.by(_._2)).mkString("\n"))
        println("Top 10 nodes according to pagerank")

        // Printing the top inDegree nodes
        println(staticPageRank.inDegrees.top(10)(Ordering.by(_._2)).mkString("\n"))
        println("Top InDegree nodes")

        // Printing the top outDegree nodes
        println(staticPageRank.outDegrees.top(10)(Ordering.by(_._2)).mkString("\n"))
        println("Top OutDegree nodes")

        // Printing the top nodes with highest degree
        println(staticPageRank.degrees.top(10)(Ordering.by(_._2)).mkString("\n"))
        println("Top 10 nodes with highest degrees")


    spark.stop()

  }
}