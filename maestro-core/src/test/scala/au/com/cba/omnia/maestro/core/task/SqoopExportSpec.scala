//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.core.task

import java.util.UUID

import scala.io.Source

import com.twitter.scalding.{Args, CascadeJob}

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.ThermometerSpec

class SqoopExportSpec extends ThermometerSpec { def is = s2"""
  Sqoop Export Cascade test
  =========================

  Export data from HDFS to DB appending to existing rows $endToEndExportWithAppendTest
  Export data from HDFS to DB deleting all existing rows $endToEndExportWithDeleteTest

"""
  val connectionString = "jdbc:hsqldb:mem:sqoopdb"
  val username         = "sa"
  val password         = ""
  val userHome         = System.getProperty("user.home")
  val resourceUrl      = getClass.getResource("/sqoop")
  val newCustomers     = Source.fromFile(s"${resourceUrl.getPath}/sales/books/customers/export/new-customers.txt").getLines().toList
  val oldCustomers     = Source.fromFile(s"${resourceUrl.getPath}/sales/books/customers/old-customers.txt").getLines().toList

  val argsMap = Map(
    "exportDir"        -> s"$dir/user/sales/books/customers/export",
    "mapRedHome"       -> s"$userHome/.ivy2/cache",
    "connectionString" -> connectionString,
    "username"         -> username,
    "password"         -> password
  )

  def endToEndExportWithAppendTest = {
    val table = s"table_${UUID.randomUUID.toString.replace('-', '_')}"
    CustomerExport.tableSetup(connectionString, username, password, table, Option(oldCustomers))

    withEnvironment(path(resourceUrl.toString)) {
      withArgs(argsMap ++ Map("exportTableName"  -> table, "deleteFromTable" -> "false"))(new SqoopExportCascade(_)).run
      CustomerExport.tableData(connectionString, username, password, table) must containTheSameElementsAs(newCustomers ++ oldCustomers)
    }
  }

  def endToEndExportWithDeleteTest = {
    val table = s"table_${UUID.randomUUID.toString.replace('-', '_')}"
    CustomerExport.tableSetup(connectionString, username, password, table, Option(oldCustomers))

    withEnvironment(path(resourceUrl.toString)) {
      withArgs(argsMap ++ Map("exportTableName"  -> table, "deleteFromTable" -> "true"))(new SqoopExportCascade(_)).run
      CustomerExport.tableData(connectionString, username, password, table) must containTheSameElementsAs(newCustomers)
    }
  }
}

class SqoopExportCascade(args: Args) extends CascadeJob(args) with Sqoop {
  val exportDir        = args("exportDir")
  val exportTableName  = args("exportTableName")
  val mappers          = 1
  val connectionString = args("connectionString")
  val username         = args("username")
  val password         = args("password")
  val mapRedHome       = args("mapRedHome")
  val deleteFromTable  = args("deleteFromTable").toBoolean

  /**
   * hadoopMapRedHome is set for Sqoop to find the hadoop jars. This hack would be necessary ONLY in a
   * test case.
   */
  val exportOptions = ParlourExportDsl().hadoopMapRedHome(mapRedHome)

  override val jobs = Seq(sqoopExport(exportDir, exportTableName, connectionString, username, password, '|', "", exportOptions, deleteFromTable)(args))
}




