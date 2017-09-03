package utils

import config.MySqlSettings

/**
  * Created by kalit_000 on 9/2/17.
  */
object DBUtils {

  val mysql = MySqlSettings.MySqlConnection
  val mysql_username = mysql.username
  val mysql_password = mysql.password
  val mysql_hostname = mysql.hostname
  val mysql_port = mysql.port
  val mysql_db = mysql.database
  val mysqlurl = s"jdbc:mysql://${mysql_hostname}:${mysql_port}/${mysql_db}?user=${mysql_username}&password=${mysql_password}"
  val mysqldriver="com.mysql.jdbc.Driver"



}
