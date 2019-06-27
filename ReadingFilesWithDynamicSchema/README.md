Spark Submit command to run the job:

	This program will take 2 arguments:
	
		1. The path where the data files and the schema file exists (Either it may be Local, HDFS or S3)
		2. The name of the Schema file
	
	Example :
	
	1. spark-submit --master local --class com.bigdata.spark.ReadFilesWithDynamicSchema readingfileswithdynamicschema_2.11-0.1.jar.jar /users/hadoop/input1/ schema1.txt
	
	2. spark-submit --master local --class com.bigdata.spark.ReadFilesWithDynamicSchema readingfileswithdynamicschema_2.11-0.1.jar.jar /users/hadoop/input2/ schema2.txt