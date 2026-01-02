
package ca.uwaterloo.cs451.finalproject.Utils

import org.apache.spark._
import org.apache.spark.graphx._

object GraphLoader {
    def loadFromCSV(sc: SparkContext, filename: String): Graph[Int, Int] = {
        val lines = sc.textFile(filename)
        val data = if (lines.first().contains("src")) {
            lines.mapPartitionsWithIndex {
                (idx, rows) =>
                if (idx == 0) {
                    rows.drop(1)
                }
                else {
                    rows
                }
            }
        }
        else {
            lines
        }
    val edges = data.map { line =>
        val parts = line.split(",")
        val from = parts(0).trim.toLong
        val to = parts(1).trim.toLong
        Edge(from, to, 1)
    }

    val graph = Graph.fromEdges(edges, 1)

    graph
    }
}