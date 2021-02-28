package org.ashok.learnspark

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.hadoop.fs.{FileSystem, Path}




case class videospec(id:String,duration:Int,height:Int, Width:Int,codec:String, category:String);

object SparkCoreUseCase {
  
  def findMetrics(iRDD:org.apache.spark.rdd.RDD[videospec]):(Double,Int,Int) =
  {
    val specRDD = iRDD.map { x => x.duration }
    
    (specRDD.sum(),specRDD.max(),specRDD.min())
    
  }
 
  def main(args:Array[String]){
    
    //define spark configuration object
	  val conf = new SparkConf().setAppName("Local-sparkcore").setMaster("local[2]")
	  
   //define spark context object
    val sc = new SparkContext(conf)
    
    //Set the logger level to error
    sc.setLogLevel("ERROR")
    
    //UC1: Creating File RDD
    val fileRDD = sc.textFile("file:////home/hduser/install/WORKOUTS/6_SPARK_COMPLETE/6_SPARK_COMPLETE/SCALA_SPARK_WORKOUTS/youtube_videos.tsv")
    
    println("---actual data in file----")
    
    fileRDD.take(2).foreach ( println )
    
    val header = fileRDD.first()
    
    println("---data without header----")
    
    //UC2: Remove Header
    val dataRDD = fileRDD.filter(row => row!=header)
    
    dataRDD.take(2).foreach ( println )
    
    //UC3: split data and apply case class
    val splitRDD = dataRDD.map(r => r.split("\t")).map(r => videospec(r(0),r(1).toInt,r(4).toInt,r(5).toInt,r(8),r(9)))
    
    //UC4: print top 10 records
    println("---print top 10 records----")
    splitRDD.take(10).foreach(println)
    
    //UC5: Get only Music category
    val musicRDD = splitRDD.filter { x => x.category.toLowerCase().trim()=="music" }
	  println("---print top 10 music records----")
    musicRDD.take(10).foreach(println)
    
	  /********* Below are to random check samples using seed value*************/
	  // takeSample(withReplacement, num, seed)
	  /*println("---print top 3 music records----")
	  musicRDD.takeSample(true, 3, 5).foreach { x => println(x) }
	  
	  println("---print top 6 music records----")
	  musicRDD.takeSample(true, 6, 1).foreach { x => println(x) }
	  
	  println("---print top 5 music records----")
	  musicRDD.takeSample(true, 5, 3).foreach { x => println(x) }
	  */
	  /**************************************************************************/
	  
	  //UC6: Get only long Duration videos. duration greater than 100.
	  val longDurRDD = splitRDD.filter { x => x.duration > 100 }
	  println("---print top 5 long duration records----")
    longDurRDD.take(5).foreach(println)
	  
	   /********* Below are to random check samples using seed value*************/
    /*println("---print top 6 long dur records----")
	  longDurRDD.takeSample(true, 6, 1).foreach { x => println(x) }
	  
	  println("---print top 5 long dur records----")
	  longDurRDD.takeSample(true, 5, 3).foreach { x => println(x) }
	  */
	   /**************************************************************************/
    
    
    //UC7: Union music with longdur rdd
    val music_longDur = musicRDD.union(longDurRDD)
    
    println("---print top 5 music and long dur records----")
    music_longDur.takeSample(true, 5, 5).foreach { x => println(x) }
    
	  
    //UC8: ReOrdered mapColsRDD records
	  val mapColsRDD = splitRDD.map { x => (x.id, x.category, x.codec, x.duration) }
    println("---print top 5 reordered records----")
    mapColsRDD.takeSample(true, 5, 5).foreach { x => println(x) }
    
    
    //UC9: Select only duration from mapcolsrdd and find max duration
    
    val maxDuration = mapColsRDD.map(x => x._4).max()
    
    println("----------Max Duration-----------")
    
    println(s"Max Duration is : $maxDuration" )
    
    //UC10: Select only codec from mapcolsrdd, convert to upper case and print distinct of it in the screen.
    val distinctCodecRDD = mapColsRDD.map(x => x._3.toUpperCase()).distinct()
    println("----------Distinct CODEC-----------")
    distinctCodecRDD.foreach { x => println(x) }
    
    //UC11: repartition to 4
    val file4partRDD = splitRDD.repartition(4)
    
    println("------showing current partition------------")
    println("current partition is " + file4partRDD.getNumPartitions)
    
    //UC12: Persist with 2 replica
    file4partRDD.persist(StorageLevel.MEMORY_AND_DISK_2)
    
    val comedyCatRDD = file4partRDD.filter { x => x.category.trim().toLowerCase() == "comedy" }
    
    //UC13: Calculate and print the overall total, max, min duration for Comedy category
    val comedyCatMetrics = findMetrics(comedyCatRDD)
    
    println("--------------total, max, min duration for Comedy category------------------------")
    comedyCatMetrics.productIterator.foreach { x => println(x) }
    
    //UC14.1: Codecwise count
    println("--------------Codecwise count----------------------")
    val codecCount = file4partRDD.map { x => x.codec.toUpperCase() }.countByValue()
    codecCount.foreach(println)
    
    //UC14.2 Min duration of each Codec
    println("--------------Min duration of each Codec----------------------")
    val minCodeCount = file4partRDD.map { x => (x.codec,x.duration) }.reduceByKey((a,i) => if(a < i) a else i).collect()
    minCodeCount.foreach(println)
    
    //UC15 distinct category of videos
    println("--------------------------distinct category of videos--------------------------")
    val distVideoCat = file4partRDD.map { x => x.category }.distinct()
    distVideoCat.foreach { x => println(x) }
    
    //UC16 get only id, duration, height and width sorted by duration
    println("----------------get only id, duration, height and width sorted by duration---------------------------")
    val finalRDD = file4partRDD.map { x => (x.id, x.duration, x.height, x.Width) }.sortBy(x => x._2)
    
    finalRDD.collect().take(10).foreach(println)
    
    //unpersist the RDD. false means no need to block the flow until spark clears the data in memory and disc.
    file4partRDD.unpersist(false)
    
    //UC17: Write content to HDFS as | delimited
    println("-----------------------------Write content to HDFS-------------------------")
    //import org.apache.hadoop.fs.{FileSystem, Path}
    
    val outputRDD = finalRDD.map(row => (row._1,row._2,row._3,row._4).productIterator.mkString("|"))
    
    val fs = FileSystem.get(new java.net.URI("hdfs://localhost:54310"),sc.hadoopConfiguration)
	  val outPath = new Path("/user/hduser/YT_DATA")
    
    //true - delete recursively
    if (fs.exists(outPath)){
		  fs.delete(outPath, true)
	  }
    
    outputRDD.coalesce(1).saveAsTextFile("hdfs://localhost:54310/user/hduser/YT_DATA")
    
    // Number of files written to HDFS will be equal to number of partitions. if we did repartitions as 4, then 4 files will be written to HDFS.
    
    println("-------------------THE END------------------------------")
    
    
  }
  
}