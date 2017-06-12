import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object FlightStatComp {

  case class DataFlight(name:String,web:String)

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder.master("local[*]").appName("FlightStatComp").getOrCreate()
    val sparkContext = sparkSession.sparkContext
    import sparkSession.implicits._

    val df = sparkSession.read.option("header","true").option("inferSchema","true").csv("/home/hadoop/HADOOP_SHARE/sparkdata/data.csv")
    val ds = sparkSession.read.option("header","true").option("inferSchema","true").csv("/home/hadoop/HADOOP_SHARE/sparkdata/data.csv").as[DataFlight]


    df.show(5)
    ds.show(5)

    val selectedDF = df.select("web")
    val selectedDS = ds.map(_.web)

    println("DataFrame:" + selectedDF.queryExecution.optimizedPlan.numberedTreeString)
    println("DataSet:" + selectedDS.queryExecution.optimizedPlan.numberedTreeString)

    sparkSession.stop()
  }

}


