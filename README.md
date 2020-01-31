__Docker__

Run with:

`docker run --rm -it --network dockerhadoopsparkworkbench_default --env-file ./hadoop.env -e SPARK_MASTER=spark://spark-master:7077 --volume  /home/henryp/Code/Scala/MyCode/SSSPlayground/target/:/example bde2020/spark-base:2.4.0-hadoop2.8-scala2.12 /spark/bin/spark-submit --class=uk.co.odinconsultants.sssplayground.windows.ConsumeKafkaMain --master spark://spark-master:7077  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0  /example/SSSPlayground-1.0-SNAPSHOT-jar-with-dependencies.jar 10.107.222.63:2181 test_topic /streaming_test 10000`

Check writing to HDFS with:

`docker exec -it dockerhadoopsparkworkbench_datanode_1  hadoop fs -ls /streaming`

