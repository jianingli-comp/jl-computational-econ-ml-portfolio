// Query 3

package ca.uwaterloo.cs451.a6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

class ConfQ3(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[String](required = true)
    val date = opt[String](required = true)
    val text = opt[Boolean]()
    val parquet = opt[Boolean]()

    mutuallyExclusive(text, parquet)
    requireOne(text,parquet)
    // reference: https://scallop.github.io/scallop/api/org/rogach/scallop/ScallopConf.html
    verify()

}

object Q3 {
    def main(argv: Array[String]): Unit = {
        val args = new ConfQ3(argv)
        val conf = new SparkConf().setAppName("Q3")
        val sc = new SparkContext(conf)
        val shipDate = args.date()

        if (args.text()) {
            // deal with parts and suppliers
            val part = sc.textFile(args.input() + "/part.tbl")
                .map(_.split('|'))
                .map(fields => (fields(0).toInt, fields(1))).collectAsMap()
            val partBroadcast = sc.broadcast(part)
            val supplier = sc.textFile(args.input() + "/supplier.tbl")
                .map(_.split('|'))
                .map(fields => (fields(0).toInt, fields(1))).collectAsMap()
            val supplierBroadcast = sc.broadcast(supplier)

            val result = sc.textFile(args.input() + "/lineitem.tbl")
                .map(_.split('|')).filter(fields => fields(10) == shipDate)
                .flatMap {
                    fields =>
                    val l_orderkey = fields(0).toInt
                    val l_partkey = fields(1).toInt
                    val l_suppkey = fields(2).toInt

                    val partMap = partBroadcast.value
                    val supplierMap = supplierBroadcast.value
                    
                    if (partMap.contains(l_partkey) && supplierMap.contains(l_suppkey)) {
                        Some((l_orderkey, partMap(l_partkey), supplierMap(l_suppkey)))
                    } else {
                        None
                    }
                }.takeOrdered(20)(Ordering[Int].on(_._1))

                result.foreach {
                    case (orderkey, p_name, s_name) =>
                    println(s"($orderkey,$p_name,$s_name)")
                }
        } else if (args.parquet()) {
            // deal with lineitem
            val sparkSession = SparkSession.builder.getOrCreate
            val partDF = sparkSession.read.parquet(args.input() + "/part")
            val partMap = partDF.rdd.map(row => (row.getInt(0), row.getString(1))).collectAsMap()
            val partBroadcast = sc.broadcast(partMap)

            val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
            val supplierMap = supplierDF.rdd.map(row => (row.getInt(0), row.getString(1))).collectAsMap()
            val supplierBroadcast = sc.broadcast(supplierMap)
            
            val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
            val result = lineitemDF.rdd.filter(row => row.getString(10) == shipDate).flatMap {
                row =>
                    val l_orderkey = row.getInt(0)
                    val l_partkey = row.getInt(1)
                    val l_suppkey = row.getInt(2)

                    val partMap = partBroadcast.value
                    val supplierMap = supplierBroadcast.value

                    if (partMap.contains(l_partkey) && supplierMap.contains(l_suppkey)) {
                        Some((l_orderkey, partMap(l_partkey), supplierMap(l_suppkey)))
                    } else {
                        None
                    }
            }.takeOrdered(20)(Ordering[Int].on(_._1))

            result.foreach {
                case (orderkey, p_name, s_name) =>
                println(s"($orderkey,$p_name,$s_name)")
            }
        }

        sc.stop()
    }
}