package logging

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.core.config.Configurator

object MyLoggerFactory {
  def getLogger(clazz: Class[_]): Logger = {
    Configurator.initialize(null, "datamart/src/main/resources/log4j2.xml")
    val loggerName = s"MyApp.${clazz.getSimpleName}"
    LogManager.getLogger(loggerName)
  }
}

