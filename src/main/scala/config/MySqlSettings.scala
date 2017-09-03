package config

import com.typesafe.config.ConfigFactory

/**
  * Created by kalit_000 on 9/2/17.
  */
object MySqlSettings {
  private val config=ConfigFactory.load()

  object MySqlConnection {
    private val mysqlconnectionvalues=config.getConfig("mysql")
    lazy val username=mysqlconnectionvalues.getString("jdbcUsername")
    lazy val password=mysqlconnectionvalues.getString("jdbcPassword")
    lazy val hostname=mysqlconnectionvalues.getString("jdbcHostname")
    lazy val port=mysqlconnectionvalues.getInt("jdbcPort")
    lazy val database=mysqlconnectionvalues.getString("jdbcDatabase")
  }

}
