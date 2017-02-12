package com.beehave.api

import java.sql.{Connection, DriverManager}
import com.twitter.util.{Closable, Future, Time}

object MysqlClient {
  def apply() = {
    val url = "jdbc:mysql://localhost:3306/beehave"
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "root"
    val password = "DarkKnight2124661512"

    Class.forName(driver)
    new MysqlClient(
      DriverManager.getConnection(url, username, password))
  }
}

class MysqlClient(conn: Connection) extends Closable {

  def insertStudentEvent(label: String, studentId: Int) = {
    val stmt = conn.prepareStatement(
      "INSERT INTO events (label, student_id) " +
        s"VALUES ('${label}', ${studentId})")
    stmt.execute()
  }

  def insertStudent(name: String, grade: String) = {
    val stmt = conn.prepareStatement(
      "INSERT INTO students (name, grade) " +
        s"VALUES ('${name}', '${grade}')")
    stmt.execute()
  }

  def getStudent(id: Int) = {
    val result = conn.createStatement()
      .executeQuery(s"SELECT name, grade FROM students WHERE id = ${id}")
    if (result.next) {
      s"Student(name: ${result.getString("name")}, grade: ${result.getInt("grade")})"
    } else {
      "No student found"
    }
  }

  def close(deadline: Time) = {
    conn.close()
    Future.Unit
  }
}
