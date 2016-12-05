package es.indra.telco.platforms.bigdata.flowview


import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import java.io.File
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions


object CDRStats {
  val batchSeconds = 5
  val windowSize = 3
  
  //START_DTM_UTC, LAST_UPDATE, SESSION_ID, STATE, TELEPHONE, LOGIN, NAS_PORT, NAS_IP_ADDRESS, LAST_DOWNLOADED_BYTES, LAST_UPLOADED_BYTES, LAST_DURATION_SECONDS, USER_IP_ADDRESS, LASTSERVER, TERMINATION_CAUSE

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
      dslam: String)
      
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
  
  def main(args: Array[String]): Unit = {
    
    if(args.length != 2){
      printf("Usage ./cdrstats.sh <input-directory> <output-directory>\n")
      return
    }
    
    val inputDirectory = args(0)
    if(!new File(inputDirectory).exists()){
      printf("%s directory not found", inputDirectory)
      return
    }
    val outputDirectory = args(1)
    if(!new File(outputDirectory).exists()){
      printf("%s directory not found", outputDirectory)
      return
    }
    
    val tmpDir = System.getProperty("java.io.tmpdir")
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("CDR Streaming")
    val ssc = new StreamingContext(conf, Seconds(batchSeconds))
    //ssc.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "\r\n\r\n");
    
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
      val aggrSessions = rdd.map(item => ((item._2.nasIPAddress, item._2.dslam), 1)).reduceByKey((a, b) => (a+b))
      val currentTimestamp = new java.util.Date().getTime()
      //aggrSessions.saveAsTextFile(outputDirectory + "/sessions/sessions" + currentTimestamp)
      aggrSessions.saveAsTextFile(outputDirectory + "/sessions")
    })
    
    // Stream of number of CDR received per topology element ((nasIPAddress, dslam), <number of cdr>)
    val aggrCdrStream = cdrStream.map(item => ((item._2.nasIPAddress, item._2.dslam), 1)).reduceByKey((a, b) => (a+b))
    aggrCdrStream.foreachRDD(rdd => {
      val currentTimestamp = new java.util.Date().getTime()
      //rdd.saveAsTextFile("outputDirectory + "/cdrRate/cdrRate" + currentTimestamp)
      rdd.saveAsTextFile(outputDirectory + "/cdrRate/cdrRate")
    })
    
    // Stream of number of CDR received per topology element per second ((nasIPAddress, dslam), <cdr rate>)
    val aggrCdrStreamMean = cdrStream.window(Seconds(batchSeconds*windowSize))
    .map(item => ((item._2.nasIPAddress, item._2.dslam), 1)).reduceByKey((a, b) => (a+b))
    .map(item => (item._1, item._2/windowSize))
    
    // Stream of derivative of number of CDR received per topology element per second ((nasIPAddress, dslam), <cdr rate-rate>)
    val aggrCdrStreamDerivative = aggrCdrStream.fullOuterJoin(aggrCdrStreamMean)
    .map(item => (item._1, (item._2._1.getOrElse(0)-item._2._2.getOrElse(0)).toFloat/(batchSeconds*windowSize)))
    aggrCdrStreamDerivative.foreachRDD(rdd => {
      val currentTimestamp = new java.util.Date().getTime()
      // rdd.saveAsTextFile(outputDirectory + "/cdrRateDerivative/cdrRateDerivative" + currentTimestamp)
      rdd.saveAsTextFile(outputDirectory + "/cdrRateDerivative/cdrRateDerivative")
    })
    
    ssc.checkpoint(tmpDir + "/sparkcheckpoint")
    
    ssc.start()
    ssc.awaitTermination()
  }
}