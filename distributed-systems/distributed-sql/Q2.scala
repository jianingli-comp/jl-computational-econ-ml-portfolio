package ca.uwaterloo.cs451.a6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

class ConfQ2(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[String](required = true)
    val date = opt[String](required = true)
    val text = opt[Boolean]()
    val parquet = opt[Boolean]()

    mutuallyExclusive(text, parquet)
    requireOne(text,parquet)
    // reference: https://scallop.github.io/scallop/api/org/rogach/scallop/ScallopConf.html
    verify()

}

object Q2 {
    def main(argv: Array[String]): Unit = {
        val args = new ConfQ2(argv)
        val conf = new SparkConf().setAppName("Q2")
        val sc = new SparkContext(conf)
        val shipDate = args.date()

        if (args.text()) {
        // for text
                val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
                    .map(_.split('|'))
                    .filter(fields => fields(10) == shipDate)
                    .map(fields => (fields(0).toInt, ()))
                val orders = sc.textFile(args.input() + "/orders.tbl")
                    .map(_.split('|'))
                    .map(fields => (fields(0).toInt, fields(6)))
                val result = lineitem.cogroup(orders).flatMap { case (orderkey, (lineIter, orderIter)) =>
                    if (lineIter.nonEmpty && orderIter.nonEmpty) {
                        orderIter.map(clerk => (clerk, orderkey))
                    } else Iterator()
                }.takeOrdered(20)(Ordering[Int].on(_._2))
                // .takeOrdered(num)(ordering) reference: Holden Karau, A. Konwinski, P. Wendell, 
                // and Matei Zaharia, Learning Spark. Programming with RDDs. Beijing ; 
                // Sebastopol: O’reilly, 2015.

                result.foreach { case (clerk, orderkey) =>
                    println(s"($clerk,$orderkey)")
                }
                
        } else if (args.parquet()) {
        // for parquet
                val sparkSession = SparkSession.builder.getOrCreate
                val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
                val lineitemRDD = lineitemDF.rdd.filter(row => row.getString(10) == shipDate)
                // filtering out everthing else but the ship dates data
                    .map(row => (row.getInt(0),()))
                // Getting the orderkey and use the blank as place holder

                val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
                val ordersRDD = ordersDF.rdd.map(row => (row.getInt(0), row.getString(6)))
                // Getting the orderkey and the clerk with it

                val result = lineitemRDD.cogroup(ordersRDD)
                // rdd.cogroup() reference: Holden Karau, A. Konwinski, P. Wendell, 
                // and Matei Zaharia, Learning Spark. Working with Key/Value Pairs. Beijing ; 
                // Sebastopol: O’reilly, 2015.
                    .flatMap { case (orderkey, (lineIter, orderIter)) =>
                        if (lineIter.nonEmpty && orderIter.nonEmpty) {
                            orderIter.map(clerk => (clerk, orderkey))
                        } else {
                            Iterator()
                        }
                    }.takeOrdered(20)(Ordering[Int].on(_._2))

                result.foreach {
                    case (clerk, orderkey) =>
                    println(s"($clerk,$orderkey)")
                }
                
        }

        sc.stop()
    }
}
