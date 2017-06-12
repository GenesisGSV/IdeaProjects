import org.apache.spark.sql.Encoders.bean
import org.apache.spark.sql.types.{StructField, StructType,StringType}
import org.apache.spark.sql.{Encoders, Row, SparkSession}

object FlightStat {

  def main(args: Array[String]) {
    //created a session
    val sparkSession = SparkSession.builder.master("local[*]").appName("FlightStat").getOrCreate();
    val sc = sparkSession.sparkContext
    val sqlc = sparkSession.sqlContext
    val csv = sc.textFile("/home/hadoop/HADOOP_SHARE/sparkdata/2008.csv")
    val rows = csv.map(line=>line.split(",").map(_.trim))
    val header = rows.first()
    val data = rows.filter(rows=>rows!=header)
    val rdd = data.map(row=>Row(row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7),row(8),row(9),row(10)))
    import sparkSession.implicits._

    val schema = new StructType()
                    .add(StructField("Year",StringType,true))
                    .add(StructField("Month",StringType,true))
                    .add(StructField("DayofMonth",StringType,true))
                    .add(StructField("DayOfWeek",StringType,true))
                    .add(StructField("DepTime",StringType,true))
                    .add(StructField("CRSDepTime",StringType,true))
                    .add(StructField("ArrTime",StringType,true))
                    .add(StructField("CRSArrTime",StringType,true))
                    .add(StructField("UniqueCarrier",StringType,true))
                    .add(StructField("FlightNum",StringType,true))

    val df = sqlc.createDataFrame(rdd,schema)
    println(df.count())
    df.show(2)

    //res.ssaveAsTextFile("file:///home/hadoop/HADOOP_SHARE/sparkdata/out2008/1")

    sparkSession.stop()
  }

}


