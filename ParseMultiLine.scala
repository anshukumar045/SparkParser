package com.anshu
// Multine line record split by user defined delimiters "\n\n"
/*
U|APPLE
T|EAT
A|MONDAY
H|SLEEP

U|ORANGE
T|DONTEAT
A|TUEDAY
H|WALK

*/
import org.apache.log4j._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object parse extends App{
  val spark = SparkSession
      .args()
      .appName("Parse Multi Line")
      .getOrCreate()
  
  // Set the logs for ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)
      
  val sc = spark.sparkContext
  val conf = new Configuration
  conf.set("textinputformat.record.delimiter", "\n\n")
  
  val datafile = "/mapr/xxxx/yyyy/input.txt"
  
  // Read the text file nased on user defined delimiter
  val data = sc.newAPIHadoopFile(datafile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf).map(_._2.toString)
  println(data.first())
  
  // Define the datatype
  case class UFruit(utype: String, name: String)
  case class TFruit(ttype: String, activity: String)
  case class EFruit(etype: String, ecnt: String)
  case class AFruit(etype: String, acnt: String)
  case class Fruit(U: UFruit, T: TFruit, E: EFruit, A: AFruit)
  // Map the data type with case class
  val df = data.map(_.split('\n').map(_.split('|')).flatMap(e => e)).map(e => Fruit(UFruit(e(0),e(1)), TFruit(e(2),e(3)), EFruit(e(4),e(5)), AFruit(e(6),e(7)))).toDF()
  df.show(false)
  
  df.select("U.utype").show()
  spark.stop()
}
