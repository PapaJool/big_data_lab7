package config

import com.typesafe.config.{Config, ConfigFactory}

object ConfigLoader {
  private val config: Config = ConfigFactory.load()

  object spark {
    val appName: String = config.getString("spark.app_name")
    val deployMode: String = config.getString("spark.deploy_mode")
    val driverMemory: String = config.getString("spark.driver_memory")
    val executorMemory: String = config.getString("spark.executor_memory")
    val driverCore: Int = config.getInt("spark.driver_cores")
    val executorCore: Int = config.getInt("spark.executor_cores")
    val mysqlConnector: String = config.getString("spark.mysql_connector")
    val logger: String = config.getString("spark.logger")
    val configS: String = config.getString("spark.config")
  }

  object mysql {
    val username: String = config.getString("mysql.username")
    val password: String = config.getString("mysql.password")
    val host: String = config.getString("mysql.host")
    val port: Int = config.getInt("mysql.port")
    val database: String = config.getString("mysql.database")
  }
}
