package com.beehave.api

import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.CommonFilters
import com.twitter.finatra.http.routing.HttpRouter

object ServerMain extends Server

class Server extends HttpServer {
  override def configureHttp(router: HttpRouter): Unit = {
    router
      .filter[CommonFilters]
      .add[LoggingController]
  }

  def test() = {
    val mysqlClient = MysqlClient()
    mysqlClient.insertStudent("Sean Grayson", "K")
    mysqlClient.insertStudentEvent("test event", 1)
    println(mysqlClient.getStudent(1))
  }
}
