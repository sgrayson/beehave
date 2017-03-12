package com.beehave.api

import com.beehave.api.mysql._
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller

class DaoController extends Controller {

  private val mysqlClient = MysqlClient()

  get("/test") { request: Request =>
    "Hello World"
  }

  get("/student") { request: Request =>
    val id = request.getParam("id").toInt
    mysqlClient.getStudent(id)
  }

  get("/students") { request: Request =>
    mysqlClient.getStudents()
  }

  post("/student") { request: Request =>
    mysqlClient.insert(
      Students.fromJson(request.getContentString))
  }

  get("/teacher") { request: Request =>
    val id = request.getParam("id").toInt
    mysqlClient.getTeacher(id)
  }

  post("/teacher") { request: Request =>
    mysqlClient.insert(
      Teachers.fromJson(request.getContentString))
  }

//  get("/class") { request: Request =>
//    val id = request.getParam("ids").toInt
//    mysqlClient.getClasses(id)
//  }

  post("/class") { request: Request =>
    mysqlClient.insert(
      Classes.fromJson(request.getContentString))
  }

  post("/behavior") { request: Request =>
    mysqlClient.insert(
      Behaviors.fromJson(request.getContentString))
  }

  post("/behavior_event") { request: Request =>
    mysqlClient.insert(
      BehaviorEvents.fromJson(request.getContentString))
  }

  //TODO: add ability to search behaviors by date range

//  post("/event") { request: Request =>
//    val map = JSON.parseFull(request.getContentString)
//      .get.asInstanceOf[Map[String, String]]
//    val label = map.get("label").get
//    val studentId = map.get("id").get.toInt
//    mysqlClient.insertStudentEvent(label, studentId)
//  }
}
