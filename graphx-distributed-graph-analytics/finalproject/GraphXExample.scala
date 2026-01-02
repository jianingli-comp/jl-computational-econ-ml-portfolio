package ca.uwaterloo.cs451.finalproject

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphXExample {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("GraphX_Example"))

        // 1. Load a graph
        val graph = GraphLoader.edgeListFile(sc, "data/followers.txt")
        println(s"Vertices = ${graph.numVertices}, Edges = ${graph.numEdges}")
        // See how many Vertices and Edges will be
        /* Result: 
            spark-submit\--class ca.uwaterloo.cs451.finalproject.GraphXExample\target/assignments-1.0.jar
            Vertices = 6, Edges = 8 */

        // 2. VertexRDD and EdgeRDD
        println("\nSample vertices RDD:")
        graph.vertices.take(5).foreach(println)
        println("\nSample edges RDD:")
        graph.edges.take(5).foreach(println)
        /* Result:
        Sample vertices RDD:
        (4,1)
        (6,1)
        (2,1)
        (1,1)
        (3,1)

        Sample edges RDD:
        Edge(1,2,1)
        Edge(2,1,1)
        Edge(4,1,1)
        Edge(6,3,1)
        Edge(7,3,1)   */

        // 3. Graph Operators
        // (a) Property Operator Example: mapVertices
        println("\nProperty Operator: mapVertices")
        val g_mapV = graph.mapVertices ((id, attr) =>
            attr + 1
        )
        g_mapV.vertices.take(5).foreach(println)

        // (b) Structural Operator Example: subgraph
        println("\nStructural Operator: subgraph")
        val g_subg = graph.subgraph(
            vpred = (id, attr) => id % 2 == 0
        )
        println(s"Subgraph vertices = ${g_subg.numVertices}, edges = ${g_subg.numEdges}")
        
        // (c) Join Operator Example: joinVertices
        println("\nJoin Operator: joinVertices")
        val example: RDD[(VertexId, Int)] = sc.parallelize(Seq(
            (1L, 100),
            (2L, 200),
            (3L, 300)
        ))
        val g_joinV = graph.joinVertices(example)(
            (id, oldAttr, newAttr) => newAttr
        )
        g_joinV.vertices.take(5).foreach(println)

        /* Result:
        Property Operator: mapVertices
        (4,2)
        (6,2)
        (2,2)
        (1,2)
        (3,2)

        Structural Operator: subgraph
        Subgraph vertices = 3, edges = 0

        Join Operator: joinVertices
        (4,1)
        (6,1)
        (2,200)
        (1,100)
        (3,300)
        */


        // 4. PageRank
        val pagerank = graph.pageRank(0.0001).vertices
        println("\nPageRank sample 5:")
        pagerank.take(5).foreach(println)
        /* Result:
        PageRank sample 5:
        (4,0.15007622780470478)
        (6,0.7017164142469724)
        (2,1.3907556008752426)
        (1,1.4596227918476916)
        (3,0.9998520559494657) */

        // 5. Connected Components
        val cc = graph.connectedComponents().vertices
        println("\nConnected Components 5 sample:")
        cc.take(5).foreach(println)
        /* Result:
        Connected Components 5 sample:
        (4,1)
        (6,3)
        (2,1)
        (1,1)
        (3,3)  */

        // 6. Triangle Count
        val tricount = graph.triangleCount().vertices
        println("\nTriangle Count 5 sample:")
        tricount.take(5).foreach(println)
        /* Result:
        Triangle Count 5 sample:
        (4,0)
        (6,1)
        (2,0)
        (1,0)
        (3,1)
        */

        // 7. Pregel API 1
        println("\nPregel API 1:")
        // use node 1 as the starting point
        val srcID = 1L

        // Initialize distances in the graph
        // source = true, others = false
        val initialGraph = graph.mapVertices{ case (id, _) => false }
        val initial = initialGraph.mapVertices((id, _) => id == srcID)
        // Pregel
        val result = initial.pregel(false)(
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
        )
        val reachable = result.vertices.filter(_._2).count()
        val unreachable = graph.numVertices - reachable

        println(s"Number of reachable nodes from $srcID: $reachable")
        println(s"Number of unreachable nodes from $srcID: $unreachable")
        println("\n5 Sample nodes:")
        result.vertices.take(5).foreach(println)
        /* Result:
        Pregel API:
*/

        //  8. Pregel API 2
        println("\nPregel API 2:")

        val allNodes = graph.vertices.map(_._1).collect()
        var deadNodes = List[Long]()

        val boolGraph = graph.mapVertices((id, _) => false)

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
                println("Dead notes:")
                deadNodes.foreach(n => println(s"\ndead node: $n"))



        sc.stop()
    }
}