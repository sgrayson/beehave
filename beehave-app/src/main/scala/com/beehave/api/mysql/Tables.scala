package com.beehave.api.mysql

import java.sql.{Connection, Date, ResultSet, Timestamp}

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

// TODO: Make these classes more compact

// StudentViews Table
object StudentViews {
  val tableName = "student_views"

  def fromJson(jsonString: String): StudentViews = {
    val map = JSON.parseFull(jsonString)
      .get.asInstanceOf[Map[String, String]]
    StudentViews(
      deviceId = map.get("deviceId"),
      name = map.get("name"),
      age = map.get("age").map(_.toInt),
      grade = map.get("grade"),
      imageUrl = map.get("imageUrl"),
      sex = map.get("sex"),
      ethnicity = map.get("ethnicity"),
      targetBehavior = map.get("targetBehavior"),
      replacementBehavior = map.get("replacementBehavior"),
      primaryDisability = map.get("primaryDisability"),
      secondaryDisability = map.get("secondaryDisability"))
  }

  def fromSql(resultSet: ResultSet): Seq[StudentViews] = {
    val students = ListBuffer[StudentViews]()
    while (resultSet.next) {
      val student = StudentViews(
        id = Option(resultSet.getInt("id")),
        deviceId = Option(resultSet.getString("device_id")),
        name = Option(resultSet.getString("name")),
        age = Option(resultSet.getInt("age")),
        grade = Option(resultSet.getString("grade")),
        imageUrl = Option(resultSet.getString("image_url")),
        sex = Option(resultSet.getString("sex")),
        ethnicity = Option(resultSet.getString("ethnicity")),
        targetBehavior = Option(resultSet.getString("target_behavior")),
        replacementBehavior = Option(resultSet.getString("replacement_behavior")),
        primaryDisability = Option(resultSet.getString("primary_disability")),
        secondaryDisability = Option(resultSet.getString("secondary_disability")))
      students.append(student)
    }
    students
  }
}
case class StudentViews(
   id: Option[Int] = None,
   deviceId: Option[String] = None,
   name: Option[String] = None,
   imageUrl: Option[String] = None,
   age: Option[Int] = None,
   grade: Option[String] = None,
   sex: Option[String] = None,
   targetBehavior: Option[String] = None,
   replacementBehavior: Option[String] = None,
   ethnicity: Option[String] = None,
   primaryDisability: Option[String] = None,
   secondaryDisability: Option[String] = None) extends Table {

  override val tableName = StudentViews.tableName

  override val map = Map[String, Option[String]](
    "id" -> int(id),
    "device_id" -> string(deviceId),
    "name" -> string(name),
    "image_url" -> string(imageUrl),
    "age" -> int(age),
    "grade" -> string(grade),
    "sex" -> string(sex),
    "ethnicity" -> string(ethnicity),
    "target_behavior" -> string(targetBehavior),
    "replacement_behavior" -> string(replacementBehavior),
    "primary_disability" -> string(primaryDisability),
    "secondary_disability" -> string(secondaryDisability))
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
        id = Option(resultSet.getInt("id")),
        name = Option(resultSet.getString("name")),
        email = Option(resultSet.getString("email")),
        phone = Option(resultSet.getString("phone")))
      teachers.append(teacher)
    }
    teachers
  }
}
case class Teachers(
   id: Option[Int] = None,
   name: Option[String] = None,
   email: Option[String] = None,
   phone: Option[String] = None) extends Table {

  override val tableName = Teachers.tableName

  override val map = Map[String, Option[String]](
    "id" -> int(id),
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
      endTime = map.get("endTime"),
      teacherId = map.get("teacherId").map(_.toInt))
  }

  def fromSql(resultSet: ResultSet): Seq[Classes] = {
    val classes = ListBuffer[Classes]()
    while (resultSet.next) {
      val cls = Classes(
        id = Option(resultSet.getInt("id")),
        name = Option(resultSet.getString("name")),
        subject = Option(resultSet.getString("subject")),
        location = Option(resultSet.getString("location")),
        days = Option(resultSet.getString("days")),
        startTime = Option(resultSet.getString("startTime")),
        endTime = Option(resultSet.getString("endTime")),
        teacherId = Option(resultSet.getInt("teacherId")))
      classes.append(cls)
    }
    classes
  }
}
case class Classes(
   id: Option[Int] = None,
   name: Option[String] = None,
   subject: Option[String] = None,
   location: Option[String] = None,
   days: Option[String] = None,
   startTime: Option[String] = None,
   endTime: Option[String] = None,
   teacherId: Option[Int] = None) extends Table {

  override val tableName = Classes.tableName

  override val map = Map[String, Option[String]](
    "id" -> int(id),
    "name" -> string(name),
    "subject" -> string(subject),
    "location" -> string(location),
    "days" -> string(days),
    "start_time" -> string(startTime),
    "end_time" -> string(endTime),
    "teacher_id" -> int(teacherId))
}

object Behaviors {
  val tableName = "behaviors"

  def fromJson(jsonString: String): Behaviors = {
    val map = JSON.parseFull(jsonString)
      .get.asInstanceOf[Map[String, String]]
    Behaviors(
      name = map.get("name"),
      thumbnail = map.get("thumbnail"))
  }

  def fromSql(resultSet: ResultSet): Seq[Behaviors] = {
    val behaviors = ListBuffer[Behaviors]()
    while (resultSet.next) {
      val behavior = Behaviors(
        id = Option(resultSet.getInt("id")),
        name = Option(resultSet.getString("name")),
        thumbnail = Option(resultSet.getString("thumbnail")))
      behaviors.append(behavior)
    }
    behaviors
  }
}
case class Behaviors(
   id: Option[Int] = None,
   name: Option[String] = None,
   thumbnail: Option[String] = None) extends Table {

  override val tableName = Behaviors.tableName

  override val map = Map[String, Option[String]](
    "id" -> int(id),
    "name" -> string(name),
    "thumbnail" -> string(thumbnail))
}

object BehaviorEvents {
  val tableName = "behavior_events"

  def fromJson(jsonString: String): BehaviorEvents = {
    val map = JSON.parseFull(jsonString)
      .get.asInstanceOf[Map[String, String]]
    BehaviorEvents(
      name = map.get("name"),
      location = map.get("location"),
      increment = map.get("increment").map(_.toInt),
      studentViewId = map.get("studentViewId").map(_.toInt))
  }

  def fromSql(resultSet: ResultSet): Seq[BehaviorEvents] = {
    val behaviors = ListBuffer[BehaviorEvents]()
    while (resultSet.next) {
      val behavior = BehaviorEvents(
        timestamp = Option(resultSet.getTimestamp("timestamp")),
        name = Option(resultSet.getString("name")),
        location = Option(resultSet.getString("location")),
        increment = Option(resultSet.getInt("increment")),
        studentViewId = Option(resultSet.getInt("student_view_id")))
      behaviors.append(behavior)
    }
    behaviors
  }
}
case class BehaviorEvents(
   timestamp: Option[Timestamp] = None,
   name: Option[String] = None,
   location: Option[String] = None,
   increment: Option[Int] = None,
   studentViewId: Option[Int] = None) extends Table {

  override val tableName = BehaviorEvents.tableName

  override val map = Map[String, Option[String]](
    "name" -> string(name),
    "location" -> string(location),
    "increment" -> int(increment),
    "student_view_id" -> int(studentViewId))
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

  def prepareQuery(table: Table) = {
    val keyValues = table.getKeyValuesMap
      .map{ case (key, value) => key + " = " + value }
      .reduce(_ + " AND " + _)
    val query = s"SELECT * FROM ${table.tableName}" +
      s" WHERE ${keyValues}"
    assign(query)
    this
  }

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
