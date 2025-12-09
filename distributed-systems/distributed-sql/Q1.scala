// Here just for Query 1
package ca.uwaterloo.cs451.a6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

class ConfQ1(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[String](required = true)
    val date = opt[String](required = true)
    val text = opt[Boolean]()
    val parquet = opt[Boolean]()

    mutuallyExclusive(text, parquet)
    requireOne(text,parquet)
    // reference: https://scallop.github.io/scallop/api/org/rogach/scallop/ScallopConf.html
    verify()
}

object Q1 {
    def main(argv: Array[String]): Unit = {
        val args = new ConfQ1(argv)
        val conf = new SparkConf().setAppName("Q1")
        val sc = new SparkContext(conf)
        val shipDate = args.date()

        if (args.text()) {
                val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
                // Because l_shipdate is the 10th so
                val count = lineitem.map(_.split('|')).filter(fields => fields(10) == shipDate).count()
                println(s"ANSWER=$count")

        } else if (args.parquet()) {
                // The codes here are according to our hints
                val sparkSession = SparkSession.builder.getOrCreate
                val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
                val lineitemRDD = lineitemDF.rdd
                
                // Using RDD API for filtering and counts
                val count = lineitemRDD.filter(row => row.getString(10) == shipDate).count()
                println(s"ANSWER=$count")
        }

        sc.stop()
    }
}