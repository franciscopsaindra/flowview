package es.indra.telco.platforms.bigdata.flowview


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec
import org.apache.spark.streaming.Time
import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.util.Properties
import org.apache.log4j.{LogManager, Level}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import scala.collection.mutable.ArrayBuffer

import slick.driver.DerbyDriver.api._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import slick.jdbc.GetResult._

import org.apache.spark.SparkContext._

object CDRStats {
  
  //LAST_UPDATE, START_TIME, SESSION_ID, STATE, TELEPHONE, LOGIN, NAS_PORT, NAS_IP_ADDRESS, LAST_DOWNLOADED_BYTES, LAST_UPLOADED_BYTES, LAST_DURATION_SECONDS, USER_IP_ADDRESS, LASTSERVER, TERMINATION_CAUSE

  case class CDR(
      lastUpdate: String, 
      startDate: String, 
      sessionId: String, 
      state: String, 
      phone: String, 
      login: String, 
      nasPort: String, 
      nasIPAddress: String, 
      bytesDown: Long, 
      bytesUp: Long, 
      duration: Long,
      userIPAddress: String,
      lastServer: String,
      terminationCause: String,
      dslam: String
  )
      
  case class Session(
      phone: String,
      userIPAddress: String,
      nasIPAddress: String,
      dslam: String,
      lastServer: String
  )

  def getDslam(nasport: String, nasIPAddress: String, sessionId: String): String = {
    if(sessionId.contains("atm")){
      nasIPAddress.trim() + "/" + ((nasport.toLong & 16777215) >> 16) 
      
    } else {
      nasIPAddress.trim() + "/" + ((nasport.toLong & 16777215) >> 12) 
    }
  }
  
  // DELETE
  def appendToFile(fileName: String, text: String) = {
    val pw = new PrintWriter(new FileWriter(fileName, true))
    try{ pw.println(text)} finally pw.close()
  }
  
  /**
   * 
   */
  def main(args: Array[String]): Unit = {
    
    val logger = LogManager.getRootLogger();
    
    // Read configuration
    val configUrl = getClass.getResource("/analyzer.properties")
    val source = scala.io.Source.fromURL(configUrl)
    val properties = new Properties()
    properties.load(source.bufferedReader())
    
    val batchSeconds = properties.getProperty("batchSeconds", "5").toInt
    val shortStopThresholdSeconds = properties.getProperty("shortStopThresholdSeconds", "120").toInt
    val minCDRThreshold = properties.getProperty("minCDRThreshold", "10").toInt
    val totalCDRIncreaseThreshold = properties.getProperty("totalCDRIncreaseThreshold", "1.5").toFloat
    val totalCDRDecreaseThreshold = properties.getProperty("totalCDRDecreaseThreshold", "1.5").toFloat
    val shortStopCDRRatioThreshold = properties.getProperty("shortStopCDRRatioThreshold", "0.3").toFloat
    val startCDRRatioThreshold = properties.getProperty("startCDRRatioThreshold", "0.2").toFloat
    val applyThresholds = properties.getProperty("applyThresholds", "false").toBoolean
    val inputDirectory = if(args.length > 0) args(0) else properties.getProperty("inputDirectory")
    
    val databaseURL = properties.getProperty("databaseURL")
    val databaseDriver = properties.getProperty("databaseDriver")
    
    val tmpDir = System.getProperty("java.io.tmpdir")

    val conf = new SparkConf().setMaster("local[2]").setAppName("Flowview analyzer")
    val ssc = new StreamingContext(conf, Seconds(batchSeconds))
    //ssc.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "\r\n\r\n");
    
    if(!new File(inputDirectory).exists()){
      printf("%s directory not found\n", inputDirectory)
      return
    }

    logger.info("CDR Analyzer started")
    
    // Definition of Sessions table
    // timeMillis, bras, dslam, cdrRate, cdrRateChange, startCDRRatio, shortStopCDRRatio
    class Sessions(tag: Tag) extends Table[(Long, String, String, Int)](tag, "SESSIONS") {
      def timeMillis = column[Long]("TIMEMILLIS")
      def bras = column[String]("BRAS")  // Column names must be capitalized
      def dslam = column[String]("DSLAM")
      def sessions = column[Int]("SESSIONS")

      // Every table needs a * projection with the same type as the table's type parameter
      def * = (timeMillis, bras, dslam, sessions)
    }
    
    // Definition of CDR table
    // timeMillis, bras, dslam, cdrRate, cdrRateChange, startCDRRatio, shortStopCDRRatio
    class CdrStats(tag: Tag) extends Table[(Long, String, String, Float, Float, Float, Float)](tag, "CDRSTATS") {
      def timeMillis = column[Long]("TIMEMILLIS")
      def bras = column[String]("BRAS")
      def dslam = column[String]("DSLAM")
      def cdrRate = column[Float]("CDRRATE")
      def cdrRateChange = column[Float]("CDRRATECHANGE")
      def startCDRRatio = column[Float]("STARTCDRRATIO")
      def shortStopCDRRatio = column[Float]("SHORTSTOPCDRRATIO")

      // Every table needs a * projection with the same type as the table's type parameter
      def * = (timeMillis, bras, dslam, cdrRate, cdrRateChange, startCDRRatio, shortStopCDRRatio)
    }
         
    // Stream of raw CDR
    val cdrStream = ssc.textFileStream(inputDirectory).flatMap(line => {
      if(line.count(_ == ',') == 13){
        val lineItems = line.split(",")
        val terminationCause = if(lineItems.length >= 14) lineItems(13).trim() else "none"
        val cdr = CDR(lineItems(0).trim(), lineItems(1).trim(), lineItems(2).trim(), lineItems(3).trim(), lineItems(4).trim(), lineItems(5).trim(), lineItems(6).trim(), lineItems(7).trim(), lineItems(8).trim().toLong, lineItems(9).trim().toLong, lineItems(10).trim().toLong, lineItems(11).trim(), lineItems(12).trim(), terminationCause, getDslam(lineItems(6), lineItems(7), lineItems(2)))
        Seq((cdr.sessionId, cdr))
      }
      else Seq()
    })
    
    // Stream of current session states
    // sessionStates key = sessionId, value = [Session]
    // If no new value for the key, cdrSeq will be empty --> return the same state (not changed)
    // If there are new values, order by date, get the oldest and return something if the session is open, None if session is closed
    val activeSessions = cdrStream.updateStateByKey((cdrSeq, currState: Option[Session]) => {
      if(cdrSeq.isEmpty) currState
      else{
        val lastCDR = cdrSeq.sortWith(_.lastUpdate > _.lastUpdate)(0)
        if (lastCDR.state == "A") Some(Session(lastCDR.phone, lastCDR.userIPAddress, lastCDR.nasIPAddress, lastCDR.dslam, lastCDR.lastServer))
        else None
      }
    })
    
    // Stream of aggregated sessions
    val aggrSessions = activeSessions
    .map(session => {
      ((session._2.nasIPAddress, session._2.dslam), 1)
    })
    .reduceByKey((a, b) => (a+b))
    
    // Store aggregated sessions
    aggrSessions.foreachRDD((rdd, time) => {
        rdd.foreachPartition(partitionRDD => {
          // Connections should be cached
          val db = Database.forURL(databaseURL, driver = databaseDriver)
          val itemsToInsert: ArrayBuffer[(Long, String, String, Int)] = ArrayBuffer()
          partitionRDD.foreach( item => {
              itemsToInsert += ((time.milliseconds, item._1._1, item._1._2, item._2))
            }
          )
          val result = db.run(TableQuery[Sessions] ++= itemsToInsert)
          result.onFailure {case e => println(e)}
          result.onComplete(_ => db.close())
        })
        
        // Delete old records from the driver
        val db = Database.forURL(databaseURL, driver = databaseDriver)
        val deleteAction = sqlu"delete from SESSIONS where TIMEMILLIS < ${time.milliseconds}"
        val result = db.run(deleteAction)
        result.onComplete(_ => db.close())
    })
    
    // Stream of number of CDR received per topology element ((nasIPAddress, dslam), (<totalcdr>, <start>, <shortstops>, <longstops>))
    val aggrCdrStream = cdrStream.map(item => 
            (
                (item._2.nasIPAddress, item._2.dslam),
                (
                  1, 
                  if(item._2.state == "A") 1 else 0,  
                  if(item._2.state == "C" && item._2.duration < shortStopThresholdSeconds) 1 else 0,
                  if(item._2.state == "C" && item._2.duration >= shortStopThresholdSeconds) 1 else 0
                )
            )).reduceByKey((values1, values2) => (values1._1 + values2._1, values1._2 + values2._2, values1._3 + values2._3, values1._4 + values2._4))
      .mapValues(item => (item._1.toFloat, item._2.toFloat, item._3.toFloat, item._4.toFloat))
   
   // State is last cdrRate 4tuple (<totalcdr>, <start>, <shortstops>, <longstops>)
   // Returned value is (<timestamp>, <totalCDRRate>, <totalCDRRateChange>, <startCDRRatio>, <shortStopCDRRatio>)
   def updateLastCDRRateState(time: Time, topologyElement: (String, String), cdrVals: Option[(Float, Float, Float, Float)], state: State[(Float, Float, Float, Float)]): Option[(Long, String, String, Float, Float, Float, Float)] = {
     // New state is last received data
     val lastState = state.getOption().getOrElse[(Float, Float, Float, Float)]((0, 0, 0, 0))
     
     // Update state
     val currentCDRVals = cdrVals.getOrElse[(Float, Float, Float, Float)]((0, 0, 0, 0))
     state.update(currentCDRVals)
     
     // Get current stats
     
     // Drop or increase of CDRRate %1
     // Number between -1 and infinity. Normal value is 0
     val totalCDRRateChange = if(lastState._1 > 0 && lastState._1 > minCDRThreshold) ((currentCDRVals._1 - lastState._1)/ lastState._1) else 0
     
     // Ratio of short CDR to total CDR
     // Number between 0 and 1. Normal value is 0
     val shortStopCDRRatio = if(currentCDRVals._1 > 0 && currentCDRVals._1 > minCDRThreshold) (currentCDRVals._3 / currentCDRVals._1) else 0
     
     // Ratio of start to total CDR
     // Number between 0 and 1. Normal value depends on the interim interval. Typically 0.7
     val startCDRRatio = if(currentCDRVals._1 > 0 && currentCDRVals._1 > minCDRThreshold) (currentCDRVals._2 / currentCDRVals._1) else 1
     
     // Calculate output
     val currentTimestamp = time
     
     if(
           !applyThresholds ||
           totalCDRRateChange <= totalCDRDecreaseThreshold ||
           totalCDRRateChange >= totalCDRIncreaseThreshold ||
           shortStopCDRRatio >= shortStopCDRRatioThreshold ||
           startCDRRatio <= startCDRRatioThreshold
         )
     Some(time.milliseconds, topologyElement._1, topologyElement._2, currentCDRVals._1/batchSeconds, totalCDRRateChange, startCDRRatio, shortStopCDRRatio)
     else None
   }
   
   // New stream containing stats
   val cdrStats = aggrCdrStream.mapWithState(StateSpec.function(updateLastCDRRateState _))
   
   // Persist
   cdrStats.foreachRDD { rdd => 
     rdd.foreachPartition { partitionRDD => 
          val db = Database.forURL(databaseURL, driver = databaseDriver)
          val insertActions: ArrayBuffer[DBIO[Int]] = ArrayBuffer()
          partitionRDD.foreach(
              item => {
                val insertRow: DBIO[Int] = sqlu"insert into cdrstats (timeMillis, bras, dslam, cdrRate, cdrRateChange, startCDRRatio, shortStopCDRRatio) values (${item._1}, ${item._2}, ${item._3}, ${item._4}, ${item._5}, ${item._6}, ${item._7})"
                insertActions += insertRow
              }
          )
          val result = db.run(DBIO.sequence(insertActions.toSeq))
          result.onFailure {case e => println(e)}
          result.onComplete(_ => db.close())
     }
   }
   
   ssc.checkpoint(tmpDir + "/sparkcheckpoint")
    
   ssc.start()
   ssc.awaitTermination()
  }
}