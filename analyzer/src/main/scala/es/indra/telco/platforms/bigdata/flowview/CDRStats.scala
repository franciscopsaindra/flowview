package es.indra.telco.platforms.bigdata.flowview


import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec
import java.io.File
import java.util.Properties
import org.apache.log4j.{LogManager, Level}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import scala.collection.mutable.ArrayBuffer

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
      bytesDown: String, 
      bytesUp: String, 
      duration: String,
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
      nasIPAddress.trim() + "/" + ((nasport.toInt & 16777215) >> 16) 
      
    } else {
      nasIPAddress.trim() + "/" + ((nasport.toInt & 16777215) >> 12) 
    }
  }
  
  def calcMean(values: Iterable[Float]): Float = {values.reduce((a, b) => (a+b)) / values.count(value => true)}
  
  def calcVariance(values: Iterable[Float], mean: Float): Float = { values.map(value => (value - mean) * (value - mean)).reduce((a, b) => (a + b)) / values.count(value => true) }
  
  
  /**
   * 
   */
  def main(args: Array[String]): Unit = {
    
    // Read configuration
    val configUrl = getClass.getResource("/analyzer.properties")
    val source = scala.io.Source.fromURL(configUrl)
    val properties = new Properties()
    properties.load(source.bufferedReader())
    
    val batchSeconds = properties.getProperty("batchSeconds", "5").toInt
    val windowSize = properties.getProperty("windowSize", "3").toInt
    val eventTimeoutMillis = properties.getProperty("eventTimeoutMillis", "86400000").toLong
    val sigmaThreshold = properties.getProperty("sigmaThreshold", "1.3").toFloat
    val inputDirectory = if(args.length > 0) args(0) else properties.getProperty("inputDirectory")
    val outputDirectory = if(args.length > 1) args(1) else properties.getProperty("outputDirectory")
    
    val logger = LogManager.getRootLogger();
    
    if(!new File(inputDirectory).exists()){
      printf("%s directory not found\n", inputDirectory)
      return
    }
    
    if(!new File(outputDirectory).exists()){
      printf("%s directory not found", outputDirectory)
      return
    }
    
    val tmpDir = System.getProperty("java.io.tmpdir")
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("Flowview analyzer")
    val ssc = new StreamingContext(conf, Seconds(batchSeconds))
    //ssc.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "\r\n\r\n");
    
    logger.info("CDR Analyzer started")
    
    // Stream of raw CDR
    val cdrStream = ssc.textFileStream(inputDirectory).map(line => {
      val lineItems = line.split(",")
      val cdr = CDR(lineItems(0).trim(), lineItems(1).trim(), lineItems(2).trim(), lineItems(3).trim(), lineItems(4).trim(), lineItems(5).trim(), lineItems(6).trim(), lineItems(7).trim(), lineItems(8).trim(), lineItems(9).trim(), lineItems(10).trim(), lineItems(11).trim(), lineItems(12).trim(), lineItems(13).trim(), getDslam(lineItems(6), lineItems(7), lineItems(2)))
      (cdr.sessionId, cdr)
    })
    
    // Stream of current session states
    // sessionStates key = sessionId, value = [Session]
    // If no new value for the key, cdrSeq will be empty --> return the same state (not changed)
    // If there are new values, order by date, get the oldest and return something if the session is open, None if session is closed
    val sessionStates = cdrStream.updateStateByKey((cdrSeq, currState: Option[Session]) => {
      if(cdrSeq.isEmpty) currState
      else{
        val lastCDR = cdrSeq.sortWith(_.lastUpdate > _.lastUpdate)(0)
        if (lastCDR.state == "A") Some(Session(lastCDR.phone, lastCDR.userIPAddress, lastCDR.nasIPAddress, lastCDR.dslam, lastCDR.lastServer))
        else None
      }
    })
    
    sessionStates.foreachRDD(rdd => {
      // Number of sessions per topology element ((nasIPAddress, dslam), <number of sessions>)
      // item._2 is a session object
      val aggrSessions = rdd.map(item => ((item._2.nasIPAddress, item._2.dslam), 1)).reduceByKey((a, b) => (a+b))
      //aggrSessions.saveAsTextFile(outputDirectory + "/sessions/sessions" + currentTimestamp)
      aggrSessions.saveAsTextFile(outputDirectory + "/sessions")
    })
    
    // Stream of number of CDR received per topology element ((nasIPAddress, dslam), <number of cdr>)
    val aggrCdrStream = cdrStream.map(item => ((item._2.nasIPAddress, item._2.dslam), 1)).reduceByKey((a, b) => (a+b))
      .mapValues(_.toFloat/batchSeconds)
    
    // Stream of ((nasIPAddress, dslam), (meanCDRRate, stdevCDRRate))
    val aggrCdrRateStreamStatsTmp = aggrCdrStream.window(Seconds(batchSeconds * windowSize)).mapValues(item => item.toFloat)
      .groupByKey.mapValues(values => (calcMean(values), Math.sqrt(calcVariance(values, calcMean(values))).toFloat))
      
    // Stream of (nasIPAddress, dslam), (Option[CDRRate], Option[(meanCDRRate, stdevCDRRate)])
    val aggrCdrRateStreamStats = aggrCdrStream.fullOuterJoin(aggrCdrRateStreamStatsTmp)
    // Stream of ((nasIPAddress, dslam), CDRRate, meanCDRRate, stdevCDRRate, numberOfSigmas)
      .map(item => {
        val networkElement = item._1
        val currentCDRRate = item._2._1.getOrElse[Float](0)
        val stats = item._2._2.getOrElse[(Float, Float)]((0, 0))
        val meanCDRRate = stats._1
        val stdevCDRRate = stats._2
        val numberOfSigmas = if(stdevCDRRate > 0) (currentCDRRate - meanCDRRate)/stdevCDRRate else 0
        
        (networkElement, (currentCDRRate, meanCDRRate, stdevCDRRate, numberOfSigmas))
      })
      
   aggrCdrRateStreamStats.foreachRDD(rdd => {
       rdd.saveAsTextFile(outputDirectory + "/cdrRate")
   })
   
   // Returned items and states are ((nasip, dslam), ArrayBuffer[(timestamp, sigma)])
   val updateEventsState = (topologyElement: (String, String), cdrRateStats: Option[(Float, Float, Float, Float)], state: State[ArrayBuffer[(Long, Float)]]) => {
     val currentTimestamp = new java.util.Date().getTime()
      
      // Add element if necessary: 3-sigma event
      val myCdrRateStats = cdrRateStats.getOrElse[(Float, Float, Float, Float)]((0, 0, 0, 0))
      if(Math.abs(myCdrRateStats._4)  > sigmaThreshold){
        val currentState = state.getOption().getOrElse(ArrayBuffer())
        
        // Check that event is not just following a previous one
        if(currentState.size > 1){
          val prevEvent = currentState.last
          if((currentTimestamp - prevEvent._2) < (windowSize * batchSeconds * 1100) && Math.abs(myCdrRateStats._4) > Math.abs(prevEvent._2)) currentState.update(currentState.size-1, (currentTimestamp, myCdrRateStats._4))
          else currentState.append((currentTimestamp, myCdrRateStats._4))
        }
        else currentState.append((currentTimestamp, myCdrRateStats._4))
        
        state.update(currentState)
      }
     
      if(state.exists()){
        // Remove elements timed out
        while(!state.get().isEmpty && state.get().head._1 < currentTimestamp - eventTimeoutMillis) state.update(state.get().tail)
        // Return state
        Some((topologyElement, state.get()))
      } else 
        // State has not been set
        None

   }
    
   val eventsStates = aggrCdrRateStreamStats.mapWithState(StateSpec.function(updateEventsState)).stateSnapshots()
   eventsStates.foreachRDD(rdd => {
       rdd.saveAsTextFile(outputDirectory + "/events")
   })
   
   
   
   ssc.checkpoint(tmpDir + "/sparkcheckpoint")
    
   ssc.start()
   ssc.awaitTermination()
  }
}