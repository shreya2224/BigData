import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object NBayes 
{
def main(args: Array[String])
{
  System.setProperty("hadoop.home.dir", "C:/cygwin64/home/ShreyaK/hadoop-2.4.1/")
  val sc = new SparkContext("local", "Asn1", "C:/cygwin64/home/ShreyaK/spark-1.3.1-bin-hadoop2.6",Nil,Map(), Map())
  val input1 = sc.textFile("C:/cygwin64/home/ShreyaK/spark-1.3.1-bin-hadoop2.6/hw3datasetnew/glass_fulldata.txt")
  
  val parsedData = input1.map { line =>
  val parts = line.split(',')
  LabeledPoint(parts(10).toDouble, Vectors.dense(parts.init.map(_.toDouble)))
  }
  
  val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
  val training = splits(0)
  val test = splits(1)
  
  val model = NaiveBayes.train(training, lambda = 1.0)
  
  val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
  val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
  
  System.out.println("Accuracy of Naive Bayes: " + accuracy*100 +"%")
  
}
}