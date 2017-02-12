package com.beehave.api

import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller

import scala.util.parsing.json.JSON

class LoggingController extends Controller {

  private val mysqlClient = MysqlClient()

  get("/test") { request: Request =>
    "Hello World"
  }

  get("/student") { request: Request =>
    val id = request.getParam("id").toInt
    mysqlClient.getStudent(id)
  }

  post("/student") { request: Request =>
    val map = JSON.parseFull(request.getContentString)
      .get.asInstanceOf[Map[String, String]]
    val name = map.get("name").get
    val grade = map.get("grade").get
    mysqlClient.insertStudent(name, grade)
  }

  post("/event") { request: Request =>
    val map = JSON.parseFull(request.getContentString)
      .get.asInstanceOf[Map[String, String]]
    val label = map.get("label").get
    val studentId = map.get("id").get.toInt
    mysqlClient.insertStudentEvent(label, studentId)
  }
}
