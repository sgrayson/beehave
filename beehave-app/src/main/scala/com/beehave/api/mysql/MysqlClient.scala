package com.beehave.api.mysql

import java.sql.{Connection, DriverManager}

import com.beehave.api.util.ConfigParser
import com.twitter.util.{Closable, Future, Time}

object MysqlClient {

  val BaseName = "mysql"

  def apply(config: ConfigParser) = {
    val configMap = config.configMap.get(BaseName)

    val host = configMap.get("host")
    val port = configMap.get("port")
    val database = configMap.get("database")
    val user = configMap.get("user")
    val password = configMap.get("password")

    val url = s"jdbc:mysql://${host}:${port}/${database}"
    val driver = "com.mysql.cj.jdbc.Driver"

    Class.forName(driver)
    new MysqlClient(
      DriverManager.getConnection(url, user, password))
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
  def getTeachers(ids: Seq[Int]): Seq[Teachers] = {
    Statement()
      .prepareQuery(Teachers.tableName)
      .where(ids)
      .query(conn, Teachers.fromSql)
      .map(_.asInstanceOf[Teachers])
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
  def getClasses(ids: Seq[Int]): Seq[Classes] = {
    Statement()
      .prepareQuery(Classes.tableName)
      .where(ids)
      .query(conn, Classes.fromSql)
      .map(_.asInstanceOf[Classes])
  }

  // Behaviors
  def insert(behavior: Behaviors) = {
    Statement()
      .prepareInsert(behavior)
      .write(conn)
  }
  def getBehavior(id: Int): Option[Behaviors] = {
    Statement()
      .prepareQuery(Behaviors.tableName)
      .where(id)
      .query(conn, Behaviors.fromSql)
      .headOption
      .map(_.asInstanceOf[Behaviors])
  }
  def getBehaviors(): Seq[Behaviors] = {
    Statement()
      .prepareQuery(Behaviors.tableName)
      .query(conn, Behaviors.fromSql)
      .map(_.asInstanceOf[Behaviors])
  }

  //Behavior Events
  def insert(behaviorEvent: BehaviorEvents) = {
    Statement()
      .prepareInsert(behaviorEvent)
      .write(conn)
  }
  def getBehaviorEventsForStudent(id: Int): Seq[BehaviorEvents] = {
    Statement()
      .prepareQuery(BehaviorEvents(studentId = Some(id)))
      .query(conn, BehaviorEvents.fromSql)
      .map(_.asInstanceOf[BehaviorEvents])
  }

  def close(deadline: Time) = {
    conn.close()
    Future.Unit
  }
}
