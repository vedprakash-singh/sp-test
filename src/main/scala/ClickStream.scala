import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
object ClickStream {
  val timeout1: Long = 30 * 60 // 30 MINUTES
  val timeout2: Long = 2 * 60 * 60 // 2 HOURS
  val outputPath: String = "enriched_clickstream.parquet"
  val inputTableName: String = "test"

  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Clickstream Problem")
    .getOrCreate()

  def tmoSessList(tmo: Long) = udf{ (uid: String, tsList: Seq[String], tsDiffs: Seq[Long]) =>
    def sid(n: Long) = s"$uid-$n"
    val sessList = tsDiffs.foldLeft( (List[String](), 0L, 0L) ){ case ((ls, j, k), i) =>
      if (i == 0 || j + i >= tmo)
        (sid(k + 1) :: ls, 0L, k + 1)
      else
        (sid(k) :: ls, j + i, k)
    }._1.reverse
    tsList zip sessList
  }

  val windowSpec: WindowSpec = Window.partitionBy("userid").orderBy("timestamp")

  val df: DataFrame = sparkSession.sql(s"SELECT * FROM $inputTableName")

  val df1: DataFrame = df.withColumn("ts_diff",
    unix_timestamp(col("timestamp")) - unix_timestamp(lag(col("timestamp"), 1)
      .over(windowSpec))
  ).
    withColumn("ts_diff", when(row_number.over(windowSpec) === 1 || col("ts_diff") >= timeout1, 0L)
      .otherwise(col("ts_diff"))
    )

  val df2: DataFrame = df1.groupBy("userid").agg(
    collect_list(col("timestamp")).as("ts_list"), collect_list(col("ts_diff")).as("ts_diffs")
  ).withColumn("tmo_sess_id",
    explode(tmoSessList(timeout2)(col("userid"), col("ts_list"), col("ts_diffs")))
  ).select(col("userid"), col("tmo_sess_id._1").as("timestamp"), col("tmo_sess_id._2").as("sessionid"))

  df2.writeStream.foreachBatch((batchDF: DataFrame, batchId: Long) => batchDF.write.save(s"$outputPath/$batchId"))

}
