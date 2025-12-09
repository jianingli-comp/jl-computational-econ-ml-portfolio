// Query 6
package ca.uwaterloo.cs451.a6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

class ConfQ6(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[String](required = true)
    val date = opt[String](required = true)
    val text = opt[Boolean]()
    val parquet = opt[Boolean]()

    mutuallyExclusive(text, parquet)
    requireOne(text,parquet)
    // reference: https://scallop.github.io/scallop/api/org/rogach/scallop/ScallopConf.html
    verify()
}

object Q6 {
    def main(argv: Array[String]): Unit = {
        val args = new ConfQ6(argv)
        val conf = new SparkConf().setAppName("Q6")
        val sc = new SparkContext(conf)
        val shipDate = args.date()

        if (args.text()) {
            val result = sc.textFile(args.input() + "/lineitem.tbl").map(_.split('|'))
                .filter(fields => fields(10) == shipDate)
                .map(fields => {
                    val returnFlag = fields(8)
                    val lineStatus = fields(9)
                    val quantity = fields(4).toDouble
                    val extendedPrice = fields(5).toDouble
                    val discount = fields(6).toDouble
                    val tax = fields(7).toDouble

                    val key = (returnFlag, lineStatus)

                    // calculate all the sums
                    val sumQuantity = quantity
                    val sumBasePrice = extendedPrice
                    val sumDiscPrice = extendedPrice * (1 - discount)
                    val sumCharge = extendedPrice * (1 - discount) * (1 + tax)
                    val count = 1L

                    (key, (sumQuantity, sumBasePrice, sumDiscPrice, sumCharge, quantity, extendedPrice, discount, count))
                }).reduceByKey {
                    case ((qty1, base1, disc1, charge1, q1, e1, d1, c1), (qty2, base2, disc2, charge2, q2, e2, d2, c2)) =>
                    (qty1 + qty2, base1 + base2, disc1 + disc2, charge1 + charge2, q1 + q2, e1 + e2, d1 + d2, c1 + c2)
                }.map {
                    case ((returnFlag, lineStatus), 
                    (sumQuantity, sumBasePrice, sumDiscPrice, sumCharge, sumQ, sumE, sumD, count)) =>
                    val avgQty = sumQ / count
                    val avgPrice = sumE / count
                    val avgDisc = sumD / count

                    (returnFlag, lineStatus, sumQuantity, sumBasePrice, sumDiscPrice, sumCharge, avgQty, avgPrice, avgDisc, count)
                }.collect()

            result.foreach {
                case (returnFlag, lineStatus, sumQuantity, sumBasePrice, sumDiscPrice, sumCharge, avgQty, avgPrice, avgDisc, count) =>
                println(s"($returnFlag,$lineStatus,$sumQuantity,$sumBasePrice,$sumDiscPrice,$sumCharge,$avgQty,$avgPrice,$avgDisc,$count)")
            }
        }
        else if (args.parquet()) {
            val sparkSession = SparkSession.builder.getOrCreate

            val result = sparkSession.read.parquet(args.input() + "/lineitem").rdd
                .filter(row => row.getString(10) == shipDate)
                .map(row => {
                    val returnFlag = row.getString(8)
                    val lineStatus = row.getString(9)
                    val quantity = row.getDouble(4)
                    val extendedPrice = row.getDouble(5)
                    val discount = row.getDouble(6)
                    val tax = row.getDouble(7)

                    val key = (returnFlag, lineStatus)

                    val sumQuantity = quantity
                    val sumBasePrice = extendedPrice
                    val sumDiscPrice = extendedPrice * (1 - discount)
                    val sumCharge = extendedPrice * (1 - discount) * (1 + tax)
                    val count = 1L

                    (key, (sumQuantity, sumBasePrice, sumDiscPrice, sumCharge, quantity, extendedPrice, discount, count))

                }).reduceByKey {
                    case ((qty1, base1, disc1, charge1, q1, e1, d1, c1),
                        (qty2, base2, disc2, charge2, q2, e2, d2, c2)) =>
                        (qty1 + qty2, base1 + base2, disc1 + disc2, charge1 + charge2,
                        q1 + q2, e1 + e2, d1 + d2, c1 + c2)
                }.map {
                    case ((returnFlag, lineStatus),
                        (sumQuantity, sumBasePrice, sumDiscPrice, sumCharge, sumQ, sumE, sumD, count)) =>
                        val avgQty = sumQ / count
                        val avgPrice = sumE / count
                        val avgDisc = sumD / count

                        (returnFlag, lineStatus, sumQuantity, sumBasePrice, sumDiscPrice, sumCharge, avgQty, avgPrice, avgDisc, count)
                }.collect()


            result.foreach {
                case (returnFlag, lineStatus, sumQuantity, sumBasePrice, sumDiscPrice, sumCharge, avgQty, avgPrice, avgDisc, count) =>
                println(s"($returnFlag,$lineStatus,$sumQuantity,$sumBasePrice,$sumDiscPrice,$sumCharge,$avgQty,$avgPrice,$avgDisc,$count)")
            }

        }
        sc.stop()
    }
}