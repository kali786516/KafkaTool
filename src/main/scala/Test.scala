import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by kalit_000 on 9/2/17.
  */
object Test {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)


    val spark =SparkSession.builder
      .master("local[*]")
      .appName("test")
      .config("spark.driver.memory","2g")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    println("test")


  }







}
