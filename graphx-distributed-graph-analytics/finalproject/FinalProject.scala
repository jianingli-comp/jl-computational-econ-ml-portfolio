package ca.uwaterloo.cs451.finalproject

import org.apache.spark._
import org.apache.spark.graphx._
import org.rogach.scallop._
import ca.uwaterloo.cs451.finalproject.Utils.{Timer, GraphLoader}

class FPConf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[String](descr = "Input BA csv file", required = true)
    val partitions = opt[Int](descr = "Number of partitions, default = 8", default = Some(8))
    val source = opt[Long](descr = "source node for reachability, default = 1", default = Some(1))
    val findDead = opt[Boolean](descr = "Want to run dead-node detection?", default = Some(false))
    // conf doesn't accept int 8, so Some(8)
    verify()
}

// It's the first time to really run graphx, so I use try-finally here to
// make sure at least the spark will stop if anything happend

object FinalProject {
    def main(argv: Array[String]): Unit = {
        val conf = new FPConf(argv)
        val filename = conf.input()
        val numPartitions = conf.partitions()

        val sc = new SparkContext(new SparkConf().setAppName("Final_Project"))

        try {
            // 1. Loading BA graph
            val graph = Timer.runAndTime("LoadGraph") {
                GraphLoader.loadFromCSV(sc, s"data/$filename")
            /*  error: type mismatch;
                [INFO]  found   : Int
                [INFO]  required: org.apache.spark.graphx.PartitionStrategy
                [INFO]                            .partitionBy(numPartitions).cache()
            
            Decided to use EdgePartition2D, reasons will be explained in the report detailly
            */
                           .partitionBy(PartitionStrategy.EdgePartition2D, numPartitions).cache()
            }

            // 2. PageRank
            Timer.runAndTime("PageRank") {
                graph.pageRank(0.0001).vertices.count()
            }

            // 3. Connected Components
            Timer.runAndTime("ConnectedComponents") {
                graph.connectedComponents().vertices.count()
            }

            // 4. Triangle Count
            Timer.runAndTime("TriangleCount") {
                graph.triangleCount().vertices.count()
            }

            // 5. Pregel 1: Similar to GraphXExample, add some modification from input
            // allowing user to choose which node they want to use as the start point
            // use node 1 as the starting point
            val srcID = conf.source()

            // Initialize distances in the graph
            // source = true, others = false
            val boolGraph = graph.mapVertices((_, _) => false)
            val initial = boolGraph.mapVertices((id, _) => id == srcID)
            // Pregel
            val result = Timer.runAndTime(s"Pregel 1 reachability from $srcID") {
                initial.pregel(false)(
                // update vertex value
                (id, state, msg) => state || msg,
                triplet => {
                    // only activate the path once, or the pregel will be stucked
                    // it actually got stuck.
                    if (triplet.srcAttr && !triplet.dstAttr) {
                        Iterator((triplet.dstId, true))
                    }
                    else {
                        Iterator.empty
                    }
                },
                (a, b) => a || b
            )}
            val reachable = result.vertices.filter(_._2).count()
            val unreachable = graph.numVertices - reachable

            println(s"Number of reachable nodes from $srcID: $reachable")
            println(s"Number of unreachable nodes from $srcID: $unreachable")
            println("\nSample nodes:")
            result.vertices.take(5).foreach(println)

            // 6. Pregel 2: Dead-node Detection
            if(conf.findDead()) {
                val allNodes = graph.vertices.map(_._1).collect()
                var deadNodes = List[Long]()

                def computeReachable(from: Long): Long = {
                    val init = boolGraph.mapVertices((id, _) => id == from)
                    val result2 = init.pregel(false)(
                        (id, state, msg) => state || msg,
                        triplet => {
                        if (triplet.srcAttr && !triplet.dstAttr){
                            Iterator((triplet.dstId, true))
                        }
                        else {
                            Iterator.empty
                        }
                    },
                    (a, b) => a || b
                    )
                    result2.vertices.filter(_._2).count()
                }

                allNodes.foreach {
                    v =>
                    val r = computeReachable(v)
                    if (r == 1) {
                        deadNodes = v :: deadNodes
                    }
                }

                println(s"Number of dead nodes: ${deadNodes.length}")
                println("Sample dead notes:")
                deadNodes.take(20).foreach(println)
            }



        }
        finally {
            sc.stop()
        }
    }
    
}