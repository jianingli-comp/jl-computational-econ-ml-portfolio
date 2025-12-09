// Query 5
package ca.uwaterloo.cs451.a6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

class ConfQ5(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[String](required = true)
    val text = opt[Boolean]()
    val parquet = opt[Boolean]()

    mutuallyExclusive(text, parquet)
    requireOne(text,parquet)
    // reference: https://scallop.github.io/scallop/api/org/rogach/scallop/ScallopConf.html
    verify()
}

object Q5 {
    def main(argv: Array[String]): Unit = {
        val args = new ConfQ5(argv)
        val conf = new SparkConf().setAppName("Q5")
        val sc = new SparkContext(conf)

        if (args.text()) {
            // broadcasting customers and nations (just US and Canada)
            val nation = sc.textFile(args.input() + "/nation.tbl").map(_.split('|'))
                .filter(fields => fields(1) == "CANADA" || fields(1) == "UNITED STATES")
                .map(fields => (fields(0).toInt, fields(1))).collectAsMap()
                // (n_nationkey, n_name)
            val nationBroadcast = sc.broadcast(nation)
            
            val customer = sc.textFile(args.input() + "/customer.tbl").map(_.split('|'))
                .map(fields => (fields(0).toInt, fields(3).toInt)).collectAsMap()
                // (c_custkey, c_nationkey)
            val customerBroadcast = sc.broadcast(customer)

            // dealing with lineitem and orders, it's basically similar to Q4
            val orders = sc.textFile(args.input() + "/orders.tbl").map(_.split('|'))
                .map(fields => (fields(0).toInt, fields(1).toInt))
                // (o_orderkey, o_custkey)

            val lineitem = sc.textFile(args.input() + "/lineitem.tbl").map(_.split('|'))
                .map(fields => {
                    val shipDate = fields(10) // l_shipdate
                    val yearMonth = if (shipDate.length >= 7) {
                        shipDate.substring(0, 7)
                    }
                    else {
                        "0000-00"
                    }
                    (fields(0).toInt, yearMonth) // (l_orderkey, year-month)
                })

            // cogroup reference in Q3, or later I might paste it here
            val result = lineitem.cogroup(orders).flatMap {
                case (orderkey, (lineIter, orderIter)) =>
                    if (lineIter.nonEmpty && orderIter.nonEmpty) {
                        for {
                            yearMonth <- lineIter
                            customerkey <- orderIter
                        } yield (customerkey, yearMonth)
                    }
                    else {
                        Iterator()
                    }
                }.flatMap {
                    case (customerkey, yearMonth) =>
                    val customerMap = customerBroadcast.value
                    val nationMap = nationBroadcast.value
                    if (customerMap.contains(customerkey)) {
                        val nationkey = customerMap(customerkey)
                        if (nationMap.contains(nationkey)) {
                            Some((nationkey, nationMap(nationkey), yearMonth, 1))
                        }
                        else {
                            None
                        }
                    }
                    else {
                        None
                    }
                }.map {
                    case (nationkey, nationName, yearMonth, count) =>
                    ((nationkey, nationName, yearMonth), count)
                }.reduceByKey(_+_) 
                    .map {
                        case ((nationkey, nationName, yearMonth), totalCount) =>
                        (nationkey, nationName,yearMonth, totalCount)
                    }.sortBy(x => (x._3, x._1)) // sort by nationkey

            result.collect().foreach {
                case (nationkey, nationName, yearMonth, count) =>
                println(s"($nationkey,$nationName,$yearMonth,$count)")
            } 
        }
        else if (args.parquet()) {
            val sparkSession = SparkSession.builder.getOrCreate

            // broacasting nations and customers, only Canada and USA
            val nationDF = sparkSession.read.parquet(args.input() + "/nation")
            val nationRDD = nationDF.rdd
                .filter(row => row.getString(1) == "CANADA" || row.getString(1) == "UNITED STATES")
                .map(row => (row.getInt(0), row.getString(1))) // (n_nationkey, n_name)
            val nationMap = nationRDD.collectAsMap()
            val nationBroadcast = sc.broadcast(nationMap)

            val customerDF = sparkSession.read.parquet(args.input() + "/customer")
            val customerRDD = customerDF.rdd.map(row => (row.getInt(0), row.getInt(3)))
            val customerMap = customerRDD.collectAsMap()
            val customerBroadcast = sc.broadcast(customerMap)

            // dealing with the lineitems and orders again for parquet  
            val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
            val ordersRDD = ordersDF.rdd.map(row => (row.getInt(0), row.getInt(1)))

            val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
            val lineitemRDD = lineitemDF.rdd
                .map(row => {
                    val shipDate = row.getString(10)
                    val yearMonth = if (shipDate != null && shipDate.length >= 7) {
                        shipDate.substring(0, 7)
                    }
                    else {
                        "0000-00"
                    }
                    (row.getInt(0), yearMonth)
                })

            val result = lineitemRDD.cogroup(ordersRDD).flatMap {
                case (orderkey, (lineIter, orderIter)) =>
                    if (lineIter.nonEmpty && orderIter.nonEmpty) {
                        for {
                            yearMonth <- lineIter
                            customerkey <- orderIter
                        } yield (customerkey, yearMonth)
                    }
                    else {
                        Iterator()
                    }
            }.flatMap {
                case (customerkey, yearMonth) =>
                    val customerMap = customerBroadcast.value
                    val nationMap = nationBroadcast.value
                    if (customerMap.contains(customerkey)) {
                        val nationkey = customerMap(customerkey)
                        if (nationMap.contains(nationkey)) {
                            Some((nationkey, nationMap(nationkey), yearMonth, 1))
                        }
                        else {
                            None
                        }
                    }
                    else {
                        None
                    }
            }.map {
                case (nationkey, nationName, yearMonth, count) =>
                ((nationkey, nationName, yearMonth), count)
            }.reduceByKey(_ + _).map {
                    case ((nationkey, nationName, yearMonth), totalCount) =>
                    (nationkey, nationName, yearMonth, totalCount)
                }.sortBy(x => (x._3, x._1)) // sorted by yearMonth and nationkey

            result.collect().foreach {
                case (nationkey, nationName, yearMonth, count) =>
                println(s"($nationkey,$nationName,$yearMonth, $count)")
            }

        }
        sc.stop()
    }
}

