package com.anshu
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

object parse extends App{
  val spark = SparkSession
      .args()
      .appName("Parse Multi Line")
      .getOrCreate()
      
  val sc = spark.sparkContext
    val conf = new Configuration
    conf.set("textinputformat.record.delimiter", "\n\n")
  val datafile = "/mapr/xxxx/yyyy/input.txt"
  val data = sc.newAPIHadoopFile(datafile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf).map(_._2.toString)
  println(data.first())
  spark.stop()
}
