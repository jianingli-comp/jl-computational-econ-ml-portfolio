// Query 4

package ca.uwaterloo.cs451.a6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

class ConfQ4(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[String](required = true)
    val date = opt[String](required = true)
    val text = opt[Boolean]()
    val parquet = opt[Boolean]()

    mutuallyExclusive(text, parquet)
    requireOne(text,parquet)
    // reference: https://scallop.github.io/scallop/api/org/rogach/scallop/ScallopConf.html
    verify()
}

object Q4 {
    def main(argv: Array[String]): Unit = {
        val args = new ConfQ4(argv)
        val conf = new SparkConf().setAppName("Q4")
        val sc = new SparkContext(conf)
        val shipDate = args.date()

        if (args.text()) {
            // broadcasting customers and nations
            val nation = sc.textFile(args.input() + "/nation.tbl").map(_.split('|'))
                .map(fields => (fields(0).toInt, fields(1))).collectAsMap()
            val nationBroadcast = sc.broadcast(nation)
            
            val customer = sc.textFile(args.input() + "/customer.tbl").map(_.split('|'))
                .map(fields => (fields(0).toInt, fields(3).toInt)).collectAsMap()
            val customerBroadcast = sc.broadcast(customer)

            // dealing with lineitem and orders now :D
            val orders = sc.textFile(args.input() + "/orders.tbl").map(_.split('|'))
                .map(fields => (fields(0).toInt, fields(1).toInt))

            val lineitem = sc.textFile(args.input() + "/lineitem.tbl").map(_.split('|'))
                .filter(fields => fields(10) == shipDate).map(fields => (fields(0).toInt, 1))

            // Use cogroup again here, can I not referencing it again? its reference is in Q3
            // It's for instead of join
            val result = lineitem.cogroup(orders).flatMap {
                case (orderkey, (lineIter, orderIter)) =>
                    if (lineIter.nonEmpty && orderIter.nonEmpty) {
                        orderIter.flatMap(customerkey => lineIter.map(count => (customerkey, count)))
                    }
                    else {
                        Iterator()
                    }
                }.map {
                    case (customerkey, count) =>
                    val customerMap = customerBroadcast.value
                    val nationMap = nationBroadcast.value
                    if (customerMap.contains(customerkey)) {
                        val nationkey = customerMap(customerkey)
                        if (nationMap.contains(nationkey)) {
                            (nationkey, nationMap(nationkey), count)
                        }
                        else {
                            (-1, "NA", count)
                        }
                    }
                    else {
                        (-1, "NA", count)
                    }
                }.filter {
                    case (nationkey, nationName, count) =>
                    nationkey != -1
                } // get rid of "NA"'s if there is any
                    .map {
                        case (nationkey, nationName, count) => 
                        ((nationkey, nationName), count)
                    }.reduceByKey(_+_) // count by nations
                    .map {
                        case ((nationkey, nationName), totalCount) =>
                        (nationkey, nationName, totalCount)
                    }.sortBy(_._1) // sort by nationkey

            result.collect().foreach {
                case (nationkey, nationName, count) =>
                println(s"($nationkey,$nationName,$count)")
            } 
        }
        else if (args.parquet()) {
            val sparkSession = SparkSession.builder.getOrCreate

            // broacasting nations and customers
            val nationDF = sparkSession.read.parquet(args.input() + "/nation")
            val nationRDD = nationDF.rdd.map(row => (row.getInt(0), row.getString(1)))
            val nationMap = nationRDD.collectAsMap()
            val nationBroadcast = sc.broadcast(nationMap)

            val customerDF = sparkSession.read.parquet(args.input() + "/customer")
            val customerRDD = customerDF.rdd.map(row => (row.getInt(0), row.getInt(3)))
            val customerMap = customerRDD.collectAsMap()
            val customerBroadcast = sc.broadcast(customerMap)

            // dealing with the lineitems and orders again for parquet
            val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
            val lineitemRDD = lineitemDF.rdd.filter(row => row.getString(10) == shipDate)
                .map(row => (row.getInt(0),1))
            
            val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
            val ordersRDD = ordersDF.rdd.map(row => (row.getInt(0), row.getInt(1)))

            val result = lineitemRDD.cogroup(ordersRDD).flatMap {
                case (orderkey, (lineIter, orderIter)) =>
                    if (lineIter.nonEmpty && orderIter.nonEmpty) {
                        orderIter.flatMap(customerkey => lineIter.map(count => (customerkey, count)))
                    }
                    else {
                        Iterator()
                    }
            }.map {
                case (customerkey, count) =>
                    val customerMap = customerBroadcast.value
                    val nationMap = nationBroadcast.value
                    if (customerMap.contains(customerkey)) {
                        val nationkey = customerMap(customerkey)
                        if (nationMap.contains(nationkey)) {
                            (nationkey, nationMap(nationkey), count)
                        }
                        else {
                            (-1, "NA", count)
                        }
                    }
                    else {
                        (-1, "NA", count)
                    }
            }.filter {
                case (nationkey, nationName, count) =>
                nationkey != -1
            } // same as text, getting rid of "NA"'s if exists
                .map {
                    case (nationkey, nationName, count) => 
                    ((nationkey, nationName),count)
                }.reduceByKey(_ + _).map {
                    case ((nationkey, nationName), totalCount) =>
                    (nationkey, nationName, totalCount)
                }.sortBy(_._1)

            result.collect().foreach {
                case (nationkey, nationName, count) =>
                println(s"($nationkey,$nationName,$count)")
            }

        }
        sc.stop()
    }
}
