package es.indra.telco.platforms.bigdata.flowview

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

object StreamCDR {
  
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

  def getDslam(nasport: String, nasIPAddress: String, sessionId: String): String = {
    if(sessionId.contains("atm")){
      nasIPAddress.trim() + "/" + ((nasport.toInt & 16777215) >> 16) 
      
    } else {
      nasIPAddress.trim() + "/" + ((nasport.toInt & 16777215) >> 12) 
    }
    
  }
      
  def main(args: Array[String]) = {
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("CDR Streaming")
    val ssc = new StreamingContext(conf, Seconds(5))
    //ssc.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "\r\n\r\n");
    
    val ss = SparkSession.builder().config(conf).getOrCreate()
    import ss.implicits._
    
    val cdrs = ssc.textFileStream("/tmp/streaminput").map(line => {
      val lineItems = line.split(",")
      val cdr = CDR(lineItems(0).trim(), lineItems(1).trim(), lineItems(2).trim(), lineItems(3).trim(), lineItems(4).trim(), lineItems(5).trim(), lineItems(6).trim(), lineItems(7).trim(), lineItems(8).trim(), lineItems(9).trim(), lineItems(10).trim(), lineItems(11).trim(), lineItems(12).trim(), lineItems(13).trim(), getDslam(lineItems(6), lineItems(7), lineItems(2)))
      (cdr.sessionId, cdr)
    })
 
    // sessionStates key = sessionId, value = (phone, userIpAddress, nasIPAddress, dslam, lastServer)
    // If no new value for the key, cdrSeq will be empty --> return the same state (not changed)
    // If there are new values, order by date, get the oldest and return something if the session is open, None if session is closed
    val sessionStates = cdrs.updateStateByKey((cdrSeq, currState: Option[(String, String, String, String, String)]) => {
      if(cdrSeq.isEmpty) currState
      else{
        val lastCDR = cdrSeq.sortWith(_.lastUpdate > _.lastUpdate)(0)
        if (lastCDR.state == "A") Some((lastCDR.phone, lastCDR.userIPAddress, lastCDR.nasIPAddress, lastCDR.dslam, lastCDR.lastServer))
        else None
      }
    })
    
    // Empty DataFrame
    // var currentSessionsDF: DataFrame = Seq.empty[(String, Int)].toDF("k", "v")
    
    sessionStates.foreachRDD(rdd => {
      rdd.toDF().groupBy($"_2._4").count().show()
      //rdd.groupBy(s => (s._2._4, s._2._3, s._2._5)).foreach(println)
      //currentDF.createOrReplaceTempView("sessions")
      //ss.sql("select count(*), _2._3 from sessions group by _2._3").show(10)
    })
    
    ssc.checkpoint("/tmp/sparkcheckpoint")
    
    ssc.start()
    ssc.awaitTermination()
    
  /*
    
    // Print output header
    val writer = new PrintWriter(new File("C:/code/psa/workspace/sparkpm/src/test/resources/c.js" ))
    writer.write("var flows=[");
    
    // Print lines
    dslamLinks.collect().foreach(x => writer.printf("[\"%s\", \"%s\", %s], ", x.getAs[String](0), x.getAs[String](1), x.getAs[Integer](2)))
    radiusLinks.collect().foreach(x => writer.printf("[\"%s\", \"%s\", %s], ", x.getAs[String](0), x.getAs[String](1), x.getAs[Integer](2)))

    // Print output trailer
    writer.write("[]];")
    writer.close()
    * */

    
    println("\nFinished!")
    
  }
}