package ca.uwaterloo.cs451.a5

import org.apache.spark._
import scala.util._
import scala.collection._
import org.rogach.scallop._
import org.apache.hadoop.fs._

// Compare to TrainSpamClassifier, we use back scallop here because there is one more path to put in
// Might change TrainSpamClassifier to scallop back though, still thinking

class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val model = opt[String](descr = "trained model path", required = true)
    verify()
}

object ApplySpamClassifier {
    def main(argv: Array[String]){
        val args = new ApplySpamClassifierConf(argv)
        val conf = new SparkConf().setAppName("TrainSpamClassifier")
        val sc = new SparkContext(conf)

        // Clearing the existing Directory
        val outputDir = new Path(args.output())
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

        // Loading the trained model
        val trained = sc.textFile(args.model()).map{ line =>
            val trimmed = line.trim().stripPrefix("(").stripSuffix(")")
            val parts = trimmed.split(",")
            (parts(0).toInt, parts(1).toDouble)
        }.collectAsMap() 
        // .collectAsMap() source: Holden Karau, A. Konwinski, P. Wendell, 
        // and Matei Zaharia, Learning Spark. Working With Key/Value Pairs. Beijing ; 
        // Sebastopol: O’reilly, 2015.

        // Broadcast the model for convience and less processing time
        val broadcastModel = sc.broadcast(trained)

        val predictions = sc.textFile(args.input()).map { line =>
            val tokens = line.split("\\s+")
            val docid = tokens(0)
            val label = tokens(1)
            val features = tokens.drop(2).map(_.toInt)

            def spamminess(features: Array[Int]) : Double = {
                var score = 0d
                val modelMap = broadcastModel.value
                features.foreach(f => if (modelMap.contains(f)) score += modelMap(f))
                score
            }

            val score = spamminess(features)
            val prediction = if (score > 0) "spam" else "ham"
            s"($docid,$label,$score,$prediction)"
        }

        predictions.saveAsTextFile(args.output())

        sc.stop()



    }
}
