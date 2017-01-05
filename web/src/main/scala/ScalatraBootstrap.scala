import es.indra.telco.platforms.flowview.web._
import org.scalatra._
import javax.servlet.ServletContext

import org.slf4j.LoggerFactory
import slick.driver.DerbyDriver.api._

class ScalatraBootstrap extends LifeCycle {
  
  val logger = LoggerFactory.getLogger(getClass)
  val db = Database.forConfig("mydb")
  
  override def init(context: ServletContext) {
    context.mount(new FlowViewMainServlet(db), "/*")
  }
  
  override def destroy(context: ServletContext) {
    super.destroy(context)
    db.close()
  }
}
