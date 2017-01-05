package es.indra.telco.platforms.flowview.web

import org.slf4j.LoggerFactory
import org.scalatra._
import slick.driver.DerbyDriver.api._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import slick.jdbc.GetResult._

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

class FlowViewMainServlet(db: Database) extends WebStack {
  
  val logger = LoggerFactory.getLogger(getClass)

  get("/") {
    logger.debug("Requested /cdrStats/cdrRateChange")
    
    <html>
      <body>
        <h1>Hello, world!</h1>
        Say <a href="hello-scalate">hello to Scalate</a>.
      </body>
    </html>
  }
  
  get("/cdrStats/cdrRateChange") {
    
    logger.debug("Requested /cdrStats/cdrRateChange")
    contentType= "text/json"
    
    val currentTime = new java.util.Date().getTime()
    
    val cdrStats = TableQuery[CdrStats]
    
    val statsQuery = cdrStats.map(stat => (stat.timeMillis, stat.dslam, stat.cdrRateChange))
    val stats = Await.result(db.run(statsQuery.result), Duration(5, "seconds"))
    val maxTime = stats.map(item => item._1).max
    
    // Build map of dslamIds
    val dslamMap = scala.collection.mutable.Map[String, Int]()
    var i = 0
    for(dslam <- stats.map(item => item._2).distinct){
      dslamMap(dslam) = i
      i = i + 1
    }
    val dslamMapString = (for(mapEntry <- dslamMap) yield s""""${mapEntry._2}":"${mapEntry._1}"""").mkString(",")
    
    // Build events array
    val events = scala.collection.mutable.ArrayBuffer[String]()
    stats.map(stat => {
      if(stat._3 < -0.5) events += s"[${(currentTime - stat._1) / 1000}, ${dslamMap(stat._2)}, null, null, null]"
      else if(stat._3 < -0.25) events += s"[${(currentTime - stat._1) / 1000}, null, ${dslamMap(stat._2)}, null, null]"
      else if(stat._3 > 0.25) events += s"[${(currentTime - stat._1) / 1000}, null, null, ${dslamMap(stat._2)}, null]"
      else if(stat._3 > 0.5) events += s"[${(currentTime - stat._1) / 1000}, null, null, null, ${dslamMap(stat._2)}]"
    })
    // Add false item for offset
    events += s"[${(currentTime - maxTime) / 1000}, 0, 0, 0, 0]"
    val eventsString = "[" + events.mkString(",") + "]"
    
    logger.debug(s"""
    {
      "dslamMap": { ${dslamMapString}},
      "events": ${eventsString}
    }
    """)
    
    s"""
    {
      "dslamMap": { ${dslamMapString}},
      "events": ${eventsString}
    }
    """
   
  }
}

