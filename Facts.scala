import java.sql.Timestamp
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.udf


object Facts {
  case class Weather(date: Timestamp,  authority: String, id:Int, rowNum: BigInt)
  case class WeatherMinMax(dateMs: Long, minDateMs: Long, maxDateMs: Long,  authority: String, id:Int)
  case class FactsRaw(authority2: String, date: Timestamp, dateMs:Long, dateId:Int, hour:Int, vehicleTypeId : Int, value: Int)

  def main(args: Array[String]): Unit = {
    val inputDirectory = args(0)
    val logger = LogManager.getLogger("projekt")
    logger.setLevel(Level.INFO)

    val conf: SparkConf = new SparkConf().
      setMaster("local").
      setAppName("facts")
    val spark: SparkSession = SparkSession.builder().
      config(conf).
      enableHiveSupport().
      getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val getDateIdUdf = udf(getDateId)
    val getFakeDateUdf = udf(getFakeDate)
    val getHashUdf = udf(getHash)




    spark.sql("use traffic")
    val weatherIdDict =  spark.sql("select id, weather from weathers").rdd.map(x => (x.getString(1), x.getInt(0))).collect().toMap


    val weatherFile = spark.read.textFile(inputDirectory + "weather.txt")
    import org.apache.spark.sql.functions._

    val weather = weatherFile.map(row => {

      val splitted = row.split(" ")
        val s = splitted(6).split("/")
        val hourSplitted = splitted(8).split(":")
      val year = s(2)
      val month = s(1)
      val day = s(0)
      var hour = "00:00"
        var r = s(2) + s(1) + s(0)
        if (hourSplitted.length >= 2) {
          hour = hourSplitted(0)+":"+hourSplitted(1)
        }
      hour = hour+":00"
        (Timestamp.valueOf(year+"-"+month+"-"+day+" "+hour), splitted(4), weatherIdDict.get(mapWeatherLine(row)).get)
      }).toDF("date",  "authority", "id").withColumn("rowNum", monotonically_increasing_id()).distinct().orderBy("authority","date").as[Weather]

//zjoinowane pogody

    val w1 = weather
    val w2 = weather.withColumnRenamed("authority", "authority2").withColumnRenamed("rowNum", "rowNum2").withColumnRenamed("date", "dateLow").withColumnRenamed("id", "id2")
    val w3 = weather.withColumnRenamed("authority", "authority3").withColumnRenamed("rowNum", "rowNum3").withColumnRenamed("date", "dateHigh").withColumnRenamed("id", "id3")

    val timestamp_avg = (startTime: Timestamp, endTime: Timestamp) => {
      (startTime.getTime() + endTime.getTime())/2
    }

    val withLowerDate = w1.join(w2, w1("authority") ===w2("authority2") && w1("rowNum") - 1 === w2("rowNum2") && w1("date") =!= w2("dateLow")).select("authority", "date", "dateLow", "rowNum", "id")
    val withBothDates = withLowerDate.join(w3, withLowerDate("authority") ===w3("authority3") && withLowerDate("rowNum") + 1 === w3("rowNum3") && w1("date") =!= w3("dateHigh")).select("authority", "date", "dateLow", "dateHigh", "id")
      .map(row => {
        (row.getTimestamp(1).getTime,timestamp_avg(row.getTimestamp(1), row.getTimestamp(2)), timestamp_avg(row.getTimestamp(1), row.getTimestamp(3)),  row.getString(0), row.getInt(4))
      }).toDF("dateMs", "minDateMs", "maxDateMs", "authority", "id" ).as[WeatherMinMax]

    val northEnglandMainData = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv(inputDirectory + "mainDataNorthEngland.csv")
    val scotlandMainData = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv(inputDirectory + "mainDataScotland.csv")
    val southEnglandMainData = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv(inputDirectory + "mainDataSouthEngland.csv")
    val mainData = northEnglandMainData
      .union(scotlandMainData).union(southEnglandMainData)

    val dataOnlyNeededColumns = mainData
      .select($"count_point_id", $"local_authoirty_ons_code", substring($"count_date",0,10).as("count_date"), $"hour".cast("string"), $"pedal_cycles",$"two_wheeled_motor_vehicles",$"cars_and_taxis",
        $"buses_and_coaches",$"lgvs",$"all_hgvs",$"all_motor_vehicles")
      .map(row => {
        var dateText = ""
        val d = row.getAs[String](2)
        val h = row.getAs[String](3)

        if(h.length < 2) dateText = d + " 0" + h + ":00:00"
        else dateText = d + " " + h + ":00:00"
        if(dateText.length != 19)
          dateText = "1970-01-01 00:00:00"
        val tsDate = Timestamp.valueOf(dateText)
        val dateId = getDateId(tsDate)

        ( row.getString(1), tsDate, tsDate.getTime, dateId, h.toInt,
          Array((1,row.getInt(4)),(2,row.getInt(5)),(3,row.getInt(6)),(4,row.getInt(7)),(5,row.getInt(8)),(6,row.getInt(9)),(7,row.getInt(10))))
      }).toDF("authority", "date", "dateMs", "dateId", "hour","vehicles").withColumn("zipped", explode($"vehicles"))
      .select($"authority", $"date", $"dateMs", $"dateId", $"hour", $"zipped._1", $"zipped._2")
      .toDF( "authority2", "date","dateMs", "dateId", "hour", "vehicleTypeId", "value").distinct().as[FactsRaw]

    val factsWithWeather = dataOnlyNeededColumns.join(withBothDates, dataOnlyNeededColumns("authority2") === withBothDates("authority") &&
//       abs(dataOnlyNeededColumns("dateMs") - withBothDates("dateMs")) < 172800000 &&
       dataOnlyNeededColumns("dateMs") >= withBothDates("minDateMs") && dataOnlyNeededColumns("dateMs") < withBothDates("maxDateMs"))



    val toSave = factsWithWeather.select("hour", "authority", "id", "vehicleTypeId", "value", "dateId")
      .toDF("hour", "local_authority_ons_code", "weather_id",  "vehicle_type_id", "count", "date_id")

    toSave.printSchema()
    toSave.take(15).foreach(x => logger.error(x))
                      //[8,E08000003,-1014741045,4,27,1581783096]
      toSave.write.mode("append").insertInto("facts")

  }

  def getDateId: Date => Int = (date: Date) => {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DATE)).hashCode()
  }

  def getFakeDate: (Timestamp, Int) => Long = (date: Timestamp, hour: Int) => {
    var formattedDate = date.toString.substring(0, 10).
      replace("-", "")
    if (hour < 10) {
      formattedDate = formattedDate + "0"
    }
    (formattedDate + hour + "00").toLong
  }

  def getHash: (Int, Long, String) => Int = (countPointId: Int, date: Long, authority: String) => {
    (countPointId, date, authority).hashCode()
  }

  def mapWeatherLine(line: String): String = {
    val splitted = line.split(" ")
    var weather = ""
    for (i <- 15 until splitted.size) {
      weather = weather + splitted(i) + " "
    }
    weather.substring(0, weather.length - 1)
  }

  def getClosestWeatherId(lowerWeatherDate: Long, lowerWeatherId: Int, higherWeatherDate: Long, higherWeatherId: Int, referenceDate: Long): Int = {
    if (referenceDate - lowerWeatherDate > higherWeatherDate - referenceDate)
      if (higherWeatherDate - referenceDate > 50000) {
        -1
      } else {
        higherWeatherId
      }
    else {
      if (referenceDate - lowerWeatherDate > 50000) {
        -1
      } else {
        lowerWeatherId
      }
    }
  }
}
