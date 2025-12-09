package ca.uwaterloo.cs451.a5

import org.apache.spark._
import scala.collection._
import scala.util._
import org.rogach.scallop._
import org.apache.hadoop.fs._

class ApplyEnsembleSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val model = opt[String](descr = "models", required = true)
    val method = opt[String](descr = "method average or vote", required = true)
    verify()
}

object ApplyEnsembleSpamClassifier {
    def main(argv: Array[String]) {
        val args = new ApplyEnsembleSpamClassifierConf(argv)
        val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
        val sc = new SparkContext(conf)

        // Clearing existing output Directory
        val outputDir = new Path(args.output())
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

        // As ScallopConfs are not serializable, we get them before manipulate the RDD
        val inputPath = args.input()
        val outputPath = args.output()
        val modelFiles = args.model()
        val method = args.method()

        // Easiest the best, don't try loops, not working :(
        val partitions = List("part-00000", "part-00001", "part-00002")

        // Load all models
        val models = partitions.map { filename =>
            val modelPath = modelFiles + "/" + filename
            sc.textFile(modelPath).map(line => {
                val trimmed = line.trim().stripPrefix("(").stripSuffix(")")
                val parts = trimmed.split(",")
                (parts(0).toInt, parts(1).toDouble)
            }).collectAsMap()
        // .collectAsMap() source: Holden Karau, A. Konwinski, P. Wendell, 
        // and Matei Zaharia, Learning Spark. Working With Key/Value Pairs. Beijing ; 
        // Sebastopol: O’reilly, 2015.

        }

        // Broadcast Models
        val broadcastModels = models.map(model => sc.broadcast(model)).toArray

        // Get predictions
        val predictions = sc.textFile(inputPath).map(line => {
            val tokens = line.split("\\s+")
            val docid = tokens(0)
            val label = tokens(1)
            val features = tokens.drop(2).map(_.toInt)
            // Calculate the scores of each model
            val scores = broadcastModels.map { broadcastModel =>
                var modelScore = 0.0
                features.foreach(f => {
                    if (broadcastModel.value.contains(f)) {
                        modelScore += broadcastModel.value(f)
                    }
                })
                modelScore
            }

            // Process chosen method for ensemble technique
            // for score average method first
            val (finalScore, prediction) = if (method == "average") {
                val totalScore = scores.sum
                val averageScore = totalScore / scores.length
                (averageScore, if (averageScore > 0) "spam" else "ham")
            } else {
                // and then here is the voting method
                val spamCount = scores.count(score => score > 0)
                val hamCount = scores.length - spamCount
                val voteScore = spamCount - hamCount
                (voteScore.toDouble, if (spamCount > hamCount) "spam" else "ham")
            }
            s"($docid,$label,$finalScore,$prediction)"
        })
        predictions.saveAsTextFile(outputPath)
        sc.stop()

    }
}