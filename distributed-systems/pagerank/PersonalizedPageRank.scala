/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package ca.uwaterloo.cs451.a4

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark._
import org.rogach.scallop._


object PersonalizedPageRank {
  val log = Logger.getLogger(getClass().getName())
  val DAMPING = 0.85f

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, iterations)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val iterations = opt[Int](descr = "number of iterations", required = true, validate = (_ > 0))
    val partitions = opt[Int](descr = "number of partitions (0 = determine from input)", required = false, default = Some(0))
    // add sources node input argument
    val sources = opt[List[Int]](descr = "source nodes list", required = true)
    verify()
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of partitions: " + args.partitions())
    log.info("Numer of iterations: " + args.iterations())
    // I think the log info for source nodes can be modify better but I will just leave as this
    log.info("Source nodes list: " + args.sources())
    val conf = new SparkConf().setAppName("PersonalizedPageRank")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    val iterations = args.iterations()

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    val textFile = sc.textFile(args.input())
    // if number of partitions argument is not specified, use the same number as the input file
    val partitions = if (args.partitions() > 0) args.partitions() else textFile.getNumPartitions
    // add the source nodes and their counts
    val sourceList = args.sources()
    val sourceNodeCount = sourceList.size


// Change format for Input as adjacency list
    val adjList = textFile.map(line => {
       val parts = line.split("\\t+") // there looks like tab between each element, we will check
       val node = parts(0).toInt
       val lists = parts.slice(1,parts.length).map(_.toInt).toList 
       // .slice() reference: https://www.geeksforgeeks.org/scala/scala-iterator-slice-method-with-example/
       (node, lists)}).distinct.partitionBy(new HashPartitioner(partitions)).cache()
    
    val N = adjList.count

    // only source nodes get mass of 1/m, where m is the number of source nodes
    // other nodes get mass 0
    var ranks = adjList.map {
      case (node, neighbors) => 
      if (sourceList.contains(node)){
        (node, 1.0f / sourceNodeCount)
      }
      else {
        (node, 0.0f)
      }
    }.cache()

    for (i <- 1 to iterations) {
      // we calculate the contributions from the neighbour lists of nodes
      val contribs = adjList.join(ranks).values.flatMap{
        case (lists, rank) =>
          if (lists.nonEmpty){ // if the list is not empty then calculate
            val size = lists.size
            lists.map(list => (list, rank / size))
          }
          else { // if the list is empty = dead ends, so empty contributions
            Iterator.empty
            }
        }.reduceByKey(_ + _, partitions)

      // After several trys, goes back to deadend again
      // As r = L*b + J is the correct equations while no dead ends,
      // where J can be calculate as (1-b)/N
      // Then, we have r = L*b + (1-b)/N
      // With dead ends, we shall modify to
      // J_d = [(1-b)+b*sum(rank(deadend))]/N

      val deadendMass = adjList.filter(_._2.isEmpty).join(ranks)
        .map(_._2._2).sum()
      
      val totalJumpMass = (1.0f - DAMPING) + DAMPING * deadendMass

      // The mass shall be distributed to sources nodes from dead ends
      val jumpPerNodeforSource = (totalJumpMass / sourceNodeCount).toFloat

      ranks = contribs.leftOuterJoin(adjList).mapPartitions({
      //.leftOuterJoin() reference: B. Chambers and M. Zaharia, Spark : the definitive guide : big data processing made simple. CHAPTER 8. JOINS. Sebastapol, Ca: O’reilly Media, 2018.
      //.mapPartitions reference: B. Chambers and M. Zaharia, Spark : the definitive guide : big data processing made simple. CHAPTER 12. RESILIENT DISTRIBUTED DATASETS (RDDS). Sebastapol, Ca: O’reilly Media, 2018.
        iter => iter.map {
          case (node, (contrib, _)) =>
          val baseContrib = DAMPING * contrib
          if (sourceList.contains(node)) {
            (node, baseContrib + jumpPerNodeforSource) // source nodes get the deadend mass
          }
          else {
            (node, baseContrib) // for other nodes
          }
        }
      }, preservesPartitioning = true).cache()
    }

    // print out the top 20 nodes
    val top20 = ranks.top(20)(Ordering.by(_._2))
    //.top() reference: LMRZero, Spark核心之top、take和takeOrdered, https://blog.csdn.net/qq_16669583/article/details/90738109 (in Chinese), accessed 2025-10-30.
    //.top() reference: B. Chambers and M. Zaharia, Spark : the definitive guide : big data processing made simple. CHAPTER 12. RESILIENT DISTRIBUTED DATASETS (RDDS). Sebastapol, Ca: O’reilly Media, 2018.
    top20.foreach {
      case (node, rank) => println(s"$node\t$rank")
    }

    ranks.saveAsTextFile(args.output())
    sc.stop()
  }
}

