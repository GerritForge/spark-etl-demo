package uk.co.pragmasoft.experiments.bigdata.spark.dbimport

import java.io.{InputStreamReader, BufferedReader}

import org.apache.commons.lang.StringUtils
import scopt.OptionParser
import slick.jdbc.StaticQuery
import scala.annotation.tailrec


object SqlCommandReader {

  @tailrec
  def readNextNonEmpyLine(sqlFileReader: BufferedReader): Option[String] = {
    val nextLine = sqlFileReader.readLine()
    if(nextLine == null) {
      None
    } else {
      if(!StringUtils.isEmpty(nextLine)) {
        Some(nextLine)
      } else {
        readNextNonEmpyLine(sqlFileReader)
      }
    }
  }

  def readNonEmptyLines(sqlFileReader: BufferedReader): Stream[String] = {
    readNextNonEmpyLine(sqlFileReader) match {
      case None => Stream.empty[String]

      case Some(line) => Stream.cons(line, readNonEmptyLines(sqlFileReader))
    }
  }

  def nextSqlCommand(lines: Iterator[String]): Option[String] = {
    val untilNextTerminator = lines.takeWhile( line => line.trim.endsWith(";") )

    if(untilNextTerminator.hasNext) {
      Some( untilNextTerminator.fold("")( _ + _ )  )
    } else {
      None
    }
  }

  def sqlCommandStream(linesIterator: Iterator[String]): Stream[String] = {
    nextSqlCommand(linesIterator) match {
      case None => Stream.empty[String]

      case Some(head) => Stream.cons(head, sqlCommandStream(linesIterator))
    }
  }

  def extractSqlCommands(sqlFileReader: BufferedReader): Stream[String] = {
    sqlCommandStream( readNonEmptyLines(sqlFileReader).iterator )
  }
}

object InitDBParams {
  case class Config(
                     dbUser: String = "system",
                     dbPwd: String = "oracle",
                     fileRootPath: String = "data/cdc",
                     dbServerConnection: String = "@//192.168.59.103:49161",
                     dbName: String = "xe"
                     )

  val optionParser = new OptionParser[Config]("CDC DB Import") {

    opt[String]("dbServerConnection") action { (arg, config) =>
      config.copy(dbServerConnection = arg)
    } text "Connection to the DB: format @//host(:port)"

    opt[String]("dbUser") action { (arg, config) =>
      config.copy(dbUser = arg)
    } text "DB User"

    opt[String]("dbPwd") action { (arg, config) =>
      config.copy(dbPwd = arg)
    } text "DB Password"

    opt[String]("dbName") action { (arg, config) =>
      config.copy(dbName = arg)
    } text "SSID of the DB to connect to"

  }
}

object InitDB extends App {
  import InitDBParams._

  val parsedArgsMaybe = optionParser.parse(args, Config())

  require(parsedArgsMaybe.isDefined, "Invalid arguments")

  val parsedArgs = parsedArgsMaybe.get

  import parsedArgs._

  val dbConnectionUrl = s"jdbc:oracle:thin:$dbServerConnection/$dbName"
  import com.typesafe.slick.driver.oracle.OracleDriver.api._

  val db = Database.forURL(dbConnectionUrl, dbUser, dbPwd)

  val sqlFileReader = new BufferedReader( new InputStreamReader(getClass.getClassLoader.getResourceAsStream("OracleDB.sql")) )
  val commandStream = SqlCommandReader.extractSqlCommands( sqlFileReader )

  db.withSession { implicit session =>

    commandStream foreach { commandStr =>
      println(s"Executing sql command '$commandStr'")
      StaticQuery.updateNA(commandStr).execute
    }
  }

  sqlFileReader.close()

}
