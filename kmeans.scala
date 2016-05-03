import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import breeze.linalg._
import org.apache.spark.HashPartitioner
import org.apache.spark._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.util.MLUtils


object kmeans 
{

  def main(args: Array[String])
{
  System.setProperty("hadoop.home.dir", "C:/cygwin64/home/ShreyaK/hadoop-2.4.1/")
   val sc = new SparkContext("local", "Asn1", "C:/cygwin64/home/ShreyaK/spark-1.3.1-bin-hadoop2.6",Nil,Map(), Map())
   System.out.println("OK")
  val input1 = sc.textFile("C:/cygwin64/home/ShreyaK/spark-1.3.1-bin-hadoop2.6/hw3datasetnew/itemusermat.txt")
  val input2 = sc.textFile("C:/cygwin64/home/ShreyaK/spark-1.3.1-bin-hadoop2.6/hw3datasetnew/movies.dat")
  
  val parsedata = input1.map{
    line =>
      Vectors.dense(line.split(" ").map(_.toDouble))
  }
  
  val model = KMeans.train(parsedata, 10, 100)
  
  val clusterCenters = model.clusterCenters map (_.toArray)
  
  val predictions = input1.map{
    _.split(" ").map(_.toDouble)}.map{rdd => (rdd(0), model.predict(Vectors.dense(rdd)))}
  
  
  val spl = input2.map(line=>line.split("::"))
  val movie_input = spl.map(line=>(line(0).toDouble,(line(1),line(2))))
  
  
 // val extration = predictions.map(line=>(line._2,line._1)).groupByKey().mapValues( x => x.toList(2))  --- not used
  
  val extration = predictions.map(line=>(line._1,line._2))
  
  val tab_join = movie_input.join(extration).map(x=>(x._1,x._2))
  
   tab_join.map(x=>(x._2._1,x._2._2))
  
      for(ele <- 0 to 2)
     {
       System.out.println("Culster: " + ele)
      val fil = tab_join.map(x=>(x._1,(x._2._1,x._2._2))).filter(x=>x._2._2==ele).take(5).foreach(println)
     
     }   
    
}
  
}