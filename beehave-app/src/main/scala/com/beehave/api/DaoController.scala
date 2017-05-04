package com.beehave.api

import javax.inject.Inject

import com.beehave.api.mysql._
import com.beehave.api.util.ConfigParser
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller

class DaoController @Inject() (config: ConfigParser) extends Controller {

  private val mysqlClient = MysqlClient(config)

  get("/test") { request: Request =>
    "Hello World"
  }

  get("/student") { request: Request =>
    val id = request.getParam("id").toInt
    val deviceId = request.getParam("deviceId")
    mysqlClient.getStudent(id, deviceId)
  }

  get("/students") { request: Request =>
    val deviceId = request.getParam("deviceId")
    mysqlClient.getStudents(deviceId)
  }

  post("/student") { request: Request =>
    mysqlClient.write(
      StudentViews.fromJson(request.getContentString))
  }

  get("/teacher") { request: Request =>
    val id = request.getParam("id").toInt
    mysqlClient.getTeacher(id)
  }

  post("/teacher") { request: Request =>
    mysqlClient.insert(
      Teachers.fromJson(request.getContentString))
  }

  post("/behavior") { request: Request =>
    mysqlClient.insert(
      Behaviors.fromJson(request.getContentString))
  }

  get("/behavior") { request: Request =>
    mysqlClient.getBehaviors()
  }

  post("/behavior_event") { request: Request =>
    mysqlClient.insert(
      BehaviorEvents.fromJson(request.getContentString))
  }

  get("behavior_events") { request: Request =>
    val studentId = request.getParam("studentId").toInt
    mysqlClient.getBehaviorEventsForStudent(studentId)
  }

  //TODO: add ability to search behaviors by date range
}
