import org.apache.spark.sql.{Row, SparkSession}

object FlightStatComputation {

  def main(args: Array[String]) {
    //created a session
    val sparkSession = SparkSession.builder.master("local[*]").appName("FlightStat").getOrCreate();
    val sqlc = sparkSession.sqlContext
    val flightdata = sqlc.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/home/hadoop/HADOOP_SHARE/sparkdata/2008.csv")
    flightdata.registerTempTable("stat");
    val distinctflights = sqlc.sql("select distinct UniqueCarrier from stat order by UniqueCarrier")
//    Unique flights
    distinctflights.collect().foreach(println)

    val uniqueNumflights = sqlc.sql("select distinct(UniqueCarrier), count(UniqueCarrier)  as cnt from stat group by UniqueCarrier order by cnt desc")
//    Unique flights:FlightCount
    uniqueNumflights.collect().foreach(println)

    val popularflight = sqlc.sql("select uc from (select distinct(UniqueCarrier) as uc, count(UniqueCarrier)  as cnt from stat group by UniqueCarrier order by cnt desc limit 1)a")
    //    Unique flights:FlightCount
    popularflight.collect().foreach(println)


    val lastQues = sqlc.sql("select Year,Month,DayofMonth,DayOfWeek,flightNum from stat limit 2")
    //    Unique flights:FlightCount
    lastQues.collect().foreach(println)

    //res.ssaveAsTextFile("file:///home/hadoop/HADOOP_SHARE/sparkdata/out2008/1")

    sparkSession.stop()
  }

}


