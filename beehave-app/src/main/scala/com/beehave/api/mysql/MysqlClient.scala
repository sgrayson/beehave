package com.beehave.api.mysql

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

  // Students
  def insert(student: Students) = {
    Statement()
      .prepareInsert(student)
      .write(conn)
  }
  def getStudent(id: Int): Option[Students] = {
    Statement()
      .prepareQuery(Students.tableName)
      .where(id)
      .query(conn, Students.fromSql)
      .headOption
      .map(_.asInstanceOf[Students])
  }
  def getStudents(): Seq[Students] = {
    Statement()
      .prepareQuery(Students.tableName)
      .query(conn, Students.fromSql)
      .map(_.asInstanceOf[Students])
  }

  // Teachers
  def insert(teacher: Teachers) = {
    Statement()
      .prepareInsert(teacher)
      .write(conn)
  }
  def getTeacher(id: Int): Option[Teachers] = {
    Statement()
      .prepareQuery(Teachers.tableName)
      .where(id)
      .query(conn, Teachers.fromSql)
      .headOption
      .map(_.asInstanceOf[Teachers])
  }

  // Classes
  def insert(cls: Classes) = {
    Statement()
      .prepareInsert(cls)
      .write(conn)
  }
  def getClass(id: Int): Option[Classes] = {
    Statement()
      .prepareQuery(Classes.tableName)
      .where(id)
      .query(conn, Classes.fromSql)
      .headOption
      .map(_.asInstanceOf[Classes])
  }

  // Behaviors
  def insert(behavior: Behaviors) = {
    Statement()
      .prepareInsert(behavior)
      .write(conn)
  }

  def close(deadline: Time) = {
    conn.close()
    Future.Unit
  }
}
