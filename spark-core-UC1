SPARK CORE - usecase
====================
Data Preparation:

	hadoop fs -mkdir /user/hduser/videolog
	hadoop fs -put youtube_videos.tsv /user/hduser/videolog/

1. create a hdfs filerdd from the file in a location /user/hduser/videolog/youtube_videos.tsv

	val fileRDD = sc.textFile("hdfs:///user/hduser/videolog/youtube_videos.tsv")

2. split the rows using tab ("\t") delimiter
	
	val tabRDD = fileRDD.map(x=>x.split("\t"))

3. Remove the header record by filtering the first column value does not contains "id" into an rdd splitrdd

	val splitRDD = tabRDD.filter(x=>x(0)!="id")
	
alternate way for the above 3

1. data cleansing

	val fileRDD = sc.textFile("hdfs:///user/hduser/videolog/youtube_videos.tsv")

	val header = fileRDD.first()

	val dataRDD = fileRDD.filter(row => row != header)

	val splitRDD = dataRDD.map(x=>x.split("\t"))	


4. display only first 10 rows in the screen from splitrdd.

	splitRDD.take(10)

5. filter only Music category data from splitrdd into an rdd called music

	val musicRDD = splitRDD.filter(row => row(9).toLowerCase == "music")
	
	verify the quality of filtered data by takesample 

	musicRDD.takeSample(true,3,20)
	musicRDD.takeSample(true,3,11)
	musicRDD.takeSample(true,2,1)


6. filter only duration>100 data from splitrdd into an rdd called longdur

	val longdurationRDD = splitRDD.filter(row => row(1).toInt > 100)

	verify the quality of filtered data by takesample 

	longdurationRDD.takeSample(true,1,3)
	longdurationRDD.takeSample(true,1,2)
	longdurationRDD.takeSample(true,1,13)

7. Union music with longdur rdd then convert to tuple and get only the distinct records into an rdd music_longdur

	val ml_distinctRDD = musicRDD.union(longdurationRDD)..map(row => (row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7),row(8),row(9),row(10))).distinct()
	
	
8. Select only id, duration, codec and category by re ordering the fields like id,category,codec,duration into an rdd mapcolsrdd

	val ml_mapcolsRDD = ml_distinctRDD.map(row => (row._1,row._10,row._9,row._3))


9. Select only duration from mapcolsrdd and find max duration by using max fuction.

	ml_mapcolsRDD.map(row => row._4).max()


10. Select only codec from mapcolsrdd, convert to upper case and print distinct of it in the screen.

	ml_mapcolsRDD.map(row => row._3.toUpperCase()).distinct().foreach(println)


11. Create an rdd called filerdd4part from filerdd created in step1 by increasing the number of partitions to 4 (Execute this step anywhere in the code where ever appropriate)

	val filerdd4part = splitRDD.repartition(4)

	check the partitions:	
	
	filerdd4part.getNumPartitions
	res32: Int = 4


12. Persist the filerdd4part data into memory and disk with replica of 2, (Execute this step anywhere in the code where ever appropriate)


	filerdd4part.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

	OR

	import org.apache.spark.storage.StorageLevel
	filerdd4part.persist(StorageLevel.MEMORY_AND_DISK)


13. Calculate and print the overall total, max, min duration for Comedy category
	
	val cntComedy = filerdd4part.filter(row => row(9).toLowerCase == "comedy").count()

	val totComedy = filerdd4part.filter(row => row(9).toLowerCase == "comedy").map(row => row(1).toInt).sum()
	val maxComedy = filerdd4part.filter(row => row(9).toLowerCase == "comedy").map(row => row(1).toInt).max()
	val minComedy = filerdd4part.filter(row => row(9).toLowerCase == "comedy").map(row => row(1).toInt).min()

	
14. Print the codec wise count and minimum duration not by using min function.

	val codecCount = filerdd4part.map(row => row(8)).countByValue()
	
	o/p: codecCount.foreach(println)
	
	(mpeg4,25264)
	(h264,74996)
	(vp8,42687)
	(none,2)
	(flv1,25337)

	val codeMinDuration = filerdd4part.map(row => (row(8),row(1).toInt)).reduceByKey((a,i) => if (a > i) i else a).collect
	o/p: Array((none,228), (h264,1), (flv1,1), (vp8,1), (mpeg4,1))

	
	tips: val codeMaxDuration = filerdd4part.map(row => (row(8),row(1).toInt)).reduceByKey((a,i) => if (a > i) a else i).collect


15. Print the distinct category of videos

	filerdd4part.map(row => row(9)).distinct.foreach(println)

16. Print only the id, duration, height and width sorted by duration.

	val finalRDD = filerdd4part.map(row => (row(0),row(1).toInt,row(4),row(5))).sortBy(row => row._2).collect()

17. Store the step 16 result in a hdfs location in a single file with data delimited as

	val outputRDD = finalRDD.map(row => (row._1,row._2,row._3,row._4).productIterator.mkString("|"))

	import org.apache.hadoop.fs.{FileSystem, Path}


	val fs = FileSystem.get(new java.net.URI("hdfs://localhost:54310"),sc.hadoopConfiguration)
	val outPath = new Path("/user/hduser/YT_DATA")


	if (fs.exists(outPath)){
		fs.delete(outPath, true)
	}

	sc.parallelize(outputRDD).coalesce(1)..saveAsTextFile("hdfs://localhost:54310/user/hduser/YT_DATA")

	OR

	val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:54310"),sc.hadoopConfiguration)
	fs.delete(new org.apache.hadoop.fs.Path("/user/hduser/YT_DATA"),true)
	
	outputRDD.saveAsTextFile("hdfs://localhost:54310/user/hduser/YT_DATA")


	o/p: 

	hadoop fs -ls /user/hduser/YT_DATA
	21/02/04 17:19:09 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
	Found 2 items
	-rw-r--r--   1 hduser supergroup          0 2021-02-04 17:19 /user/hduser/YT_DATA/_SUCCESS
	-rw-r--r--   1 hduser supergroup    3994644 2021-02-04 17:19 /user/hduser/YT_DATA/part-00000
