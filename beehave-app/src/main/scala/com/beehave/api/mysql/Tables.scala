package com.beehave.api.mysql

import java.sql.{Connection, Date, ResultSet, Timestamp}

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

// TODO(BIG): add support for joins!
// TODO: Make these classes more compact

// Students Table
object Students {
  val tableName = "students"

  def fromJson(jsonString: String): Students = {
    val map = JSON.parseFull(jsonString)
      .get.asInstanceOf[Map[String, String]]
    Students(
      name = map.get("name"),
      age = map.get("age").map(_.toInt),
      grade = map.get("grade"),
      sex = map.get("sex"),
      ethnicity = map.get("ethnicity"),
      primaryDisability = map.get("primaryDisability"),
      secondaryDisability = map.get("secondaryDisability"),
      schedule = map.get("schedule"),
      parentName = map.get("parentName"),
      parentEmail = map.get("parentEmail"),
      parentPhone = map.get("parentPhone"))
  }

  def fromSql(resultSet: ResultSet): Seq[Students] = {
    val students = ListBuffer[Students]()
    while (resultSet.next) {
      val student = Students(
        name = Option(resultSet.getString("name")),
        age = Option(resultSet.getInt("age")),
        grade = Option(resultSet.getString("grade")),
        sex = Option(resultSet.getString("sex")),
        ethnicity = Option(resultSet.getString("ethnicity")),
        primaryDisability = Option(resultSet.getString("primary_disability")),
        secondaryDisability = Option(resultSet.getString("secondary_disability")),
        schedule = Option(resultSet.getString("schedule")),
        parentName = Option(resultSet.getString("parent_name")),
        parentEmail = Option(resultSet.getString("parent_email")),
        parentPhone = Option(resultSet.getString("parent_phone")))
      students.append(student)
    }
    students
  }
}
case class Students(
   name: Option[String] = None,
   age: Option[Int] = None,
   grade: Option[String] = None,
   sex: Option[String] = None,
   ethnicity: Option[String] = None,
   primaryDisability: Option[String] = None,
   secondaryDisability: Option[String] = None,
   //TODO: schedule will need to be Map[Int,String] => (Period, Class)
   schedule: Option[String] = None,
   parentName: Option[String] = None,
   parentEmail: Option[String] = None,
   parentPhone: Option[String] = None) extends Table {

  override val tableName = "students"

  override val map = Map[String, Option[String]](
    "name" -> string(name),
    "age" -> int(age),
    "grade" -> string(grade),
    "sex" -> string(sex),
    "ethnicity" -> string(ethnicity),
    "primary_disability" -> string(primaryDisability),
    "secondary_disability" -> string(secondaryDisability),
    "schedule" -> string(schedule),
    "parent_name" -> string(parentName),
    "parent_email" -> string(parentEmail),
    "parent_phone" -> string(parentPhone))
}

// Teachers Table
object Teachers {
  val tableName = "teachers"

  def fromJson(jsonString: String): Teachers = {
    val map = JSON.parseFull(jsonString)
      .get.asInstanceOf[Map[String, String]]
    Teachers(
      name = map.get("name"),
      email = map.get("email"),
      phone = map.get("phone"))
  }

  def fromSql(resultSet: ResultSet): Seq[Teachers] = {
    val teachers = ListBuffer[Teachers]()
    while (resultSet.next) {
      val teacher = Teachers(
        name = Option(resultSet.getString("name")),
        email = Option(resultSet.getString("email")),
        phone = Option(resultSet.getString("phone")))
      teachers.append(teacher)
    }
    teachers
  }
}
case class Teachers(
   name: Option[String] = None,
   email: Option[String] = None,
   phone: Option[String] = None) extends Table {

  override val tableName = "teachers"

  override val map = Map[String, Option[String]](
    "name" -> string(name),
    "email" -> string(email),
    "phone" -> string(phone))
}

// Classes Table
object Classes {
  val tableName = "classes"

  def fromJson(jsonString: String): Classes = {
    val map = JSON.parseFull(jsonString)
      .get.asInstanceOf[Map[String, String]]
    Classes(
      name = map.get("name"),
      subject = map.get("subject"),
      location = map.get("location"),
      days = map.get("days"),
      startTime = map.get("startTime"),
      endTime = map.get("endTime"))
  }

  def fromSql(resultSet: ResultSet): Seq[Classes] = {
    val classes = ListBuffer[Classes]()
    while (resultSet.next) {
      val cls = Classes(
        name = Option(resultSet.getString("name")),
        subject = Option(resultSet.getString("subject")),
        location = Option(resultSet.getString("location")),
        days = Option(resultSet.getString("days")),
        startTime = Option(resultSet.getString("startTime")),
        endTime = Option(resultSet.getString("endTime")))
      classes.append(cls)
    }
    classes
  }
}
case class Classes(
   name: Option[String] = None,
   subject: Option[String] = None,
   location: Option[String] = None,
   days: Option[String] = None,
   startTime: Option[String] = None,
   endTime: Option[String] = None,
   teacher: Option[Teachers] = None) extends Table {

  override val tableName = "classes"

  override val map = Map[String, Option[String]](
    "name" -> string(name),
    "subject" -> string(subject),
    "location" -> string(location),
    "days" -> string(days),
    "startTime" -> string(startTime),
    "endTime" -> string(endTime))
}

object Behaviors {
  val tableName = "behaviors"

  //TODO
  def fromJson(jsonString: String): Behaviors = {
    Behaviors()
  }

  def fromSql(resultSet: ResultSet): Seq[Behaviors] = {
    val behaviors = ListBuffer[Behaviors]()
    while (resultSet.next) {
      val behavior = Behaviors(
        name = Option(resultSet.getString("name")),
        timestamp = Option(resultSet.getTimestamp("timestamp")),
        location = Option(resultSet.getString("location")))
      behaviors.append(behavior)
    }
    behaviors
  }
}
case class Behaviors(
   name: Option[String] = None,
   timestamp: Option[Timestamp] = None,
   location: Option[String] = None) extends Table {

  override val tableName = "behaviors"

  override val map = Map[String, Option[String]](
    "name" -> string(name),
    "timestamp" -> timestamp(timestamp),
    "location" -> string(location))
}

trait Table {
  // these must be overidden
  val map = Map[String, Option[String]]()
  val foreignKeys = Map[String, Table]()
  val tableName = "test"

  def getKeyValuesTuple = {
    val keys = map.keys.filter(map(_).isDefined).toSeq
    val values = keys.map(map(_).get)
    (keys, values)
  }

  def getKeyValuesMap: Map[String, String] = map
    .filter { case (_, value) => value.isDefined }
    .map { case (key, value) => (key, value.get) }

  def string(str: Option[String]) = str.map {s => s"'${s}'" }

  def int(n: Option[Int]) = n.map(_.toString)

  //TODO: need to test these
  def timestamp(ts: Option[Timestamp]) = ts.map(_.getTime.toString)
  def date(dt: Option[Date]) = dt.map(_.toString)
}

/**
  * TODO: Divide this class out into parts, and add ability for
  * joins to take in another Statement
  *
  * Query and Write methods will convert parts into sql string to execute
  */
object Statement {
  def apply() = new Statement()
}
class Statement {
  var statement: Option[String] = None

  def prepareInsert(table: Table) = {
    val (k, v) = table.getKeyValuesTuple
    val columns = k.reduce(_ + ", " + _)
    val values = v.reduce(_ + ", " + _)
    val insertStmt = s"INSERT INTO ${table.tableName} (${columns}) VALUES (${values})"
    assign(insertStmt)
    this
  }

  def prepareUpdate(table: Table) = {
    val keyValues = table.getKeyValuesMap
      .map{ case (key, value) => key + " = " + value }
      .reduce(_ + ", " + _)
    val updateStmt = s"UPDATE ${table.tableName} SET ${keyValues}"
    assign(updateStmt)
    this
  }

  def prepareQuery(tableName: String) = {
    val query = s"SELECT * FROM ${tableName}"
    assign(query)
    this
  }

  //TODO: simple where clause for now
  def where(id: Int) = {
    val whereClause = s" WHERE id = ${id}"
    append(whereClause)
    this
  }

  def where(ids: Seq[Int]) = {
    val whereClause = s" WHERE id IN (${ids.mkString(",")})"
    append(whereClause)
    this
  }

  def query(conn: Connection, f: (ResultSet) => Seq[Table]): Seq[Table] = {
    val results = statement.map { stmt =>
      val resultSet = conn.createStatement().executeQuery(stmt)
      f(resultSet)
    }
    reset
    results.getOrElse(Seq[Table]())
  }

  def write(conn: Connection) = {
    statement.map(conn.prepareStatement(_).execute())
    reset
  }

  private def assign(stmt: String) = statement = Some(stmt)

  private def append(stmt: String) = statement = Some(statement.get + stmt)

  private def reset() = statement = None
}
