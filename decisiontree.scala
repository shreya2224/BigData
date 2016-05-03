import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

object decisiontree 
{
def main(args: Array[String])
{
  System.setProperty("hadoop.home.dir", "C:/cygwin64/home/ShreyaK/hadoop-2.4.1/")
    val sc = new SparkContext("local", "Asn1", "C:/cygwin64/home/ShreyaK/spark-1.3.1-bin-hadoop2.6",Nil,Map(), Map())
    val input1 = sc.textFile("C:/cygwin64/home/ShreyaK/spark-1.3.1-bin-hadoop2.6/hw3datasetnew/glass_fulldata.txt")
    
 val numClasses = 6
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 100

 val parsedata = input1.map{ line => 
      val parts = line.split(",").map(_.toDouble)
      val feature_vec = Vectors.dense(parts.init)
      val label = parts.last
      LabeledPoint(label, feature_vec)
    }
    
    
  val splits = parsedata.randomSplit(Array(0.6, 0.4), seed = 11L)
  val training = splits(0)
  val test = splits(1)

  val model = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo, impurity,
  maxDepth, maxBins)

val labelAndPreds = test.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
  
 //val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / test.count
//println("Training Error = " + trainErr)
 
 val accuracy = 1.0 * labelAndPreds.filter(x => x._1 == x._2).count().toDouble / test.count()
 System.out.println("Accuracy of Decision Trees :" + accuracy*100 + "%") 
  
}
}