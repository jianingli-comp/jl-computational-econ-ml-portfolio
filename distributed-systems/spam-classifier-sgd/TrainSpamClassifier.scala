package ca.uwaterloo.cs451.a5

import org.apache.spark._
import scala.util._
import scala.collection._
import org.rogach.scallop._
import org.apache.hadoop.fs._

class TrainSpamConf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[String](descr = "input path", required = true)
    val model = opt[String](descr = "model path", required = true)
    val shuffle = opt[Boolean](descr = "shuffle true or false", required = false, default = Some(false))
    verify()
}

object TrainSpamClassifier {
  def main(argv: Array[String]) {
    val args = new TrainSpamConf(argv)
    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    // Clearing existing model directory

    val modelDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(modelDir, true)
    
    // Read training data with single partition
    var textFile = sc.textFile(args.input(), 1)
    
    // Shuffle if requested
    if (args.shuffle()) {
      textFile = textFile.map(line => (Random.nextDouble(), line)).sortByKey().map(_._2)
    }
  
    val trained = textFile.map(line => {
      val tokens = line.split("\\s+")
      val docid = tokens(0)
      val isSpam = tokens(1) == "spam"
      val features = tokens.drop(2).map(_.toInt)
      (0, (docid, isSpam, features))
    }).groupByKey(1).flatMap { case (key, records) =>
         // w is the weight vector (make sure the variable is within scope)
        val w = mutable.Map[Int, Double]() // become mutable map for later calculation
        // This is the main learner:
        val delta = 0.002

        // Scores a document based on its list of features.
        def spamminess(features: Array[Int]) : Double = {
        var score = 0d
          features.foreach(f => if (w.contains(f)) score += w(f))
          score
          }
        // For each instance...
        records.foreach { record =>
          val isSpam = record._2  // label
          val features = record._3  // feature vector of the training instance

          // Update the weights as follows:
          val score = spamminess(features)
          val prob = 1.0 / (1 + math.exp(-score))
          val labelValue = if (isSpam) 1.0 else 0.0

          features.foreach(f => {
            if (w.contains(f)) {
              w(f) += (labelValue - prob) * delta
            } else {
              w(f) = (labelValue - prob) * delta
            }
          })

        }

      w.map { case (feature, weight) => s"($feature,$weight)" }
      }

    trained.saveAsTextFile(args.model())
    sc.stop()
    
  }

}
