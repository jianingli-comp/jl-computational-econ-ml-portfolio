// Query 7
package ca.uwaterloo.cs451.a6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

class ConfQ7(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[String](required = true)
    val date = opt[String](required = true)
    val text = opt[Boolean]()
    val parquet = opt[Boolean]()

    mutuallyExclusive(text, parquet)
    requireOne(text,parquet)
    // reference: https://scallop.github.io/scallop/api/org/rogach/scallop/ScallopConf.html
    verify()

}

object Q7 {
    def main(argv: Array[String]): Unit = {
        val args = new ConfQ7(argv)
        val conf = new SparkConf().setAppName("Q7")
        val sc = new SparkContext(conf)
        val date = args.date()

        if (args.text()) {
            // deal with customers
            val customers = sc.textFile(args.input() + "/customer.tbl")
                .map(_.split('|'))
                .map(fields => (fields(0).toInt, fields(1))).collectAsMap()
            val customerBroadcast = sc.broadcast(customers)
            
            // deal with orders and lineitem
            val orders = sc.textFile(args.input() + "/orders.tbl")
                .map(_.split('|'))
                .filter(fields => fields(4) < date)
                .map(fields => (fields(0).toInt, fields(1).toInt, fields(4), fields(7).toInt))
                // (o_orderkey, o_custkey, o_orderdate, o_shippriority)

            val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
                .map(_.split('|'))
                .filter(fields => fields(10) > date) // l_shipdate > date
                .map (fields => {
                    val orderkey = fields(0).toInt
                    val extendedPrice = fields(5).toDouble
                    val discount = fields(6).toDouble
                    val revenue = extendedPrice * (1 - discount)
                    (orderkey, revenue)
                }).reduceByKey(_ + _) // aggregate revenue according to orderkey

            val result = orders.map {
                case (orderkey, custkey, orderdate, shippriority) =>
                (orderkey, (custkey, orderdate, shippriority))
            }.cogroup(lineitem) // I love cogroup :)
                .flatMap {
                    case (orderkey, (orderIter, lineIter)) =>
                    if (orderIter.nonEmpty && lineIter.nonEmpty) {
                        for {
                            (custkey, orderdate, shippriority) <- orderIter
                            revenue <- lineIter
                        } yield (custkey, orderkey, revenue, orderdate, shippriority)
                    }
                    else {
                        Iterator()
                    }
                }.map {
                    case (custkey, orderkey, revenue, orderdate, shippriority) =>
                    val customerMap = customerBroadcast.value
                    val custName = if (customerMap.contains(custkey)) {
                        customerMap(custkey)
                    }
                    else {
                        "NA"
                    }
                    (custName, orderkey, revenue, orderdate, shippriority)
                }.takeOrdered(5)(Ordering[Double].reverse.on(_._3)) // the top 5 value of unshipped orders 

                result.foreach {
                    case (custName, orderkey, revenue, orderdate, shippriority) =>
                    println(s"($custName,$orderkey,$revenue,$orderdate,$shippriority)")
                }
        } 
        else if (args.parquet()) {
            val sparkSession = SparkSession.builder.getOrCreate
            // broadcast customers
            val customerDF = sparkSession.read.parquet(args.input() + "/customer")
            val customerRDD = customerDF.rdd.map(row => (row.getInt(0), row.getString(1)))
            val customerMap = customerRDD.collectAsMap()
            val customerBroadcast = sc.broadcast(customerMap)

            // deal with orders and lineitems here
            val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
            val ordersRDD = ordersDF.rdd.filter(row => row.getString(4) < date)
                .map(row => (row.getInt(0), row.getInt(1), row.getString(4), row.getInt(7))) 
                // (o_orderkey, o_custkey, o_orderdate, o_shippriority)

            val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
            val lineitemRDD = lineitemDF.rdd.filter(row => row.getString(10) > date)
                .map(row => {
                    val orderkey = row.getInt(0)
                    val extendedPrice = row.getDouble(5)
                    val discount = row.getDouble(6)
                    val revenue = extendedPrice * (1 - discount)
                    (orderkey, revenue)
                }).reduceByKey(_ + _)

            val result = ordersRDD.map { 
                case (orderkey, custkey, orderdate, shippriority) =>
                (orderkey, (custkey, orderdate, shippriority))
            }.cogroup(lineitemRDD)
                .flatMap { 
                    case (orderkey, (ordIter, liIter)) =>
                        if (ordIter.nonEmpty && liIter.nonEmpty) {
                            for {
                              (custkey, orderdate, shippriority) <- ordIter
                              revenue <- liIter
                            } yield (custkey, orderkey, revenue, orderdate, shippriority)
                        } 
                        else {
                            Iterator()
                        }
                }.map { 
                    case (custkey, orderkey, revenue, orderdate, shippriority) =>
                    val customerMap = customerBroadcast.value
                    val custName = if (customerMap.contains(custkey)) {
                        customerMap(custkey)
                     }
                    else {
                        "NA"
                    }
                    (custName, orderkey, revenue, orderdate, shippriority)
                }.takeOrdered(5)(Ordering[Double].reverse.on(_._3))

            result.foreach { 
                case (custName, orderkey, revenue, orderdate, shippriority) =>
            println(s"($custName,$orderkey,$revenue,$orderdate,$shippriority)")
            }
        }

        sc.stop()
    }
}
