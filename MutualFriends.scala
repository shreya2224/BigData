import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._


object Assignment_ans2
{
  def main(args: Array[String])
  {
    //System.setProperty("hadoop.home.dir", "C:/cygwin64/home/ShreyaK/hadoop-2.4.1/")
    //val sc = new SparkContext("local", "Asn1", "C:/cygwin64/home/ShreyaK/spark-1.3.1-bin-hadoop2.6",Nil,Map(), Map())
    System.setProperty("hadoop.home.dir", "d:\\winutil\\")
     
      val conf = new SparkConf().setAppName("Recommend friends").setMaster("local") 
      val sc = new SparkContext(conf)
    val input = sc.textFile("hdfs://cshadoop1/socNetData/networkdata/LivJrnl.txt")
   
    val user1 = readLine("Enter user1")
    val user2 = readLine("Enter user2")
   
    
    //System.out.println("user1: " + user1 +" "+"user2: "+user2);
    
    val split_comp_user1 = input.filter(_.split("	")(0)==user1)
    val split_comp_user2 = input.filter(_.split("	")(0)==user2)
    
    val user_tup1 = split_comp_user1.map(line => line.split("	"))
    val user_tup2 = split_comp_user2.map(line1 => line1.split("	"))
    
   // val thekeys1 = user_tup1.map(line => (line(0), line(1).substring(0,line(1).length))).collect()
  //  val thekeys2 = user_tup2.map(line1 => (line1(0), line1(1).substring(0,line1(1).length))).collect()
    
  //  val keyvalpair1 = user_tup1.map(line => (line(0), line(1).substring(0,line(1).length))).values.collect()
    //val keyvalpair2 = user_tup2.map(line1 => (line1(0), line1(1).substring(0,line1(1).length))).values.collect()
   
    val keyvalpair1 = user_tup1.map(line => line(1))
    val keyvalpair2 = user_tup2.map(line => line(1))
    
    val frnd_list1 = keyvalpair1.toArray()(0).split(",").toSet
    val frnd_list2 = keyvalpair2.toArray()(0).split(",").toSet
    
    System.out.println("Mutual friends are: ");
    val mutal_friends = frnd_list1.intersect(frnd_list2).foreach(println)
    
  
    
    
    
  }
}
