package es.indra.telco.platforms.bigdata.flowview


import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import java.io.File
import org.apache.log4j.{LogManager, Level}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

import org.apache.spark.SparkContext._


object CDRStats {
  
  val batchSeconds = 5  // Seconds in a batch
  val windowSize = 3    // Number of batches per window
  
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
  
  def calcMean(values: Iterable[Double]): Double = {values.reduce((a, b) => (a+b)) / values.count(value => true)}
  
  def calcVariance(values: Iterable[Double], mean: Double): Double = { values.map(value => (value - mean) * (value - mean)).reduce((a, b) => (a + b)) / values.count(value => true) }
  
  
  /**
   * 
   */
  def main(args: Array[String]): Unit = {
    
    if(args.length != 2){
      printf("Usage ./cdrstats.sh <input-directory> <output-directory>\n")
      return
    }
    
    val logger = LogManager.getRootLogger();
    
    val inputDirectory = args(0)
    if(!new File(inputDirectory).exists()){
      printf("%s directory not found\n", inputDirectory)
      return
    }
    
    val outputDirectory = args(1)
    if(!new File(outputDirectory).exists()){
      printf("%s directory not found", outputDirectory)
      return
    }
    
    val tmpDir = System.getProperty("java.io.tmpdir")
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("Flowview analyzer")
    val ssc = new StreamingContext(conf, Seconds(batchSeconds))
    //ssc.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "\r\n\r\n");
    
    logger.warn("CDR Analyzer started")
    
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
      val currentTimestamp = new java.util.Date().getTime()
      //aggrSessions.saveAsTextFile(outputDirectory + "/sessions/sessions" + currentTimestamp)
      aggrSessions.saveAsTextFile(outputDirectory + "/sessions")
    })
    
    // Stream of number of CDR received per topology element ((nasIPAddress, dslam), <number of cdr>)
    val aggrCdrStream = cdrStream.map(item => ((item._2.nasIPAddress, item._2.dslam), 1)).reduceByKey((a, b) => (a+b))
      .mapValues(_.toDouble/batchSeconds)
    
    // Stream of ((nasIPAddress, dslam), (meanCDRRate, stdevCDRRate))
    val aggrCdrRateStreamStats = aggrCdrStream.window(Seconds(batchSeconds * windowSize)).mapValues(item => item.toDouble)
      .groupByKey.mapValues(values => (calcMean(values), Math.sqrt(calcVariance(values, calcMean(values)))))
      
    // Stream of (nasIPAddress, dslam), (Option[CDRRate], Option[(meanCDRRate, stdevCDRRate)])
    val aggrCdrRateStreamStatsFull = aggrCdrStream.fullOuterJoin(aggrCdrRateStreamStats)
    // Stream of (nasIPAddress, dslam), CDRRate, meanCDRRate, stdevCDRRate, CDRRateDerivative)
      .map(item => {
        val networkElement = item._1
        val currentCDRRate = item._2._1.getOrElse[Double](0)
        val stats = item._2._2.getOrElse[(Double, Double)]((0, 0))
        val meanCDRRate = stats._1
        val stdevCDRRate = stats._2
        (networkElement, currentCDRRate, meanCDRRate, stdevCDRRate, (currentCDRRate - meanCDRRate)/(batchSeconds * windowSize))
      })
      
   aggrCdrRateStreamStatsFull.foreachRDD(rdd => {
       rdd.saveAsTextFile(outputDirectory + "/cdrRate")
   })
   
    ssc.checkpoint(tmpDir + "/sparkcheckpoint")
    
    ssc.start()
    ssc.awaitTermination()
  }
}