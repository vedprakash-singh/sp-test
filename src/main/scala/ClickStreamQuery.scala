import ClickStream.inputTableName
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ClickStreamQuery {

  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Clickstream Problem")
    .getOrCreate()
  val df: DataFrame = sparkSession.sql(s"SELECT * FROM $inputTableName")

  def getNumberOfSessionsInADay(dataFrame: DataFrame): Long = {
    dataFrame.select("userid").distinct().count()
  }

  def timeSpentByUserInADay(dataFrame: DataFrame): Long = {
    val userWindow = Window.partitionBy(col("user_id")).orderBy(col("timestamp"))
    val prevActive = lag(col("is_active"), 1).over(userWindow)
    val newSession = col("is_active") && (prevActive.isNull || not(prevActive))
    val withInd = dataFrame.withColumn("new_session", newSession)
    val session = when(col("is_active"), sum(col("new_session").cast("long")).over(userWindow))

    val withSession = withInd.withColumn("session", session)
    val userSessionWindow = userWindow.partitionBy(col("user_id"), col("session"))
    val sessionBeginHour = when(
      col("is_active"),
      min(col("hour_of_day")).over(userSessionWindow)
    )
    val sessionLength = when(
      col("is_active"),
      col("hour_of_day") + 1 - sessionBeginHour
    ).otherwise(0)

    val userDF = withSession
      .withColumn("session_begin_hour", sessionBeginHour)
      .withColumn("session_length", sessionLength)
      .drop("new_session")
      .drop("session")
    userDF.agg(sum("session_length")).head.getAs[Long](0)
  }
}
