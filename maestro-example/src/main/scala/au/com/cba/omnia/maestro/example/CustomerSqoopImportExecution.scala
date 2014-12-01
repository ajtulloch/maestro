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

package au.com.cba.omnia.maestro.example

import java.io.File

import org.joda.time.{DateTime, DateTimeZone}

import au.com.cba.omnia.maestro.api.exec.MaestroExecution
import au.com.cba.omnia.maestro.example.thrift.Customer

import au.com.cba.omnia.parlour.ParlourImportOptions

/** Please be aware that the Execution API is being actively developed/modified and
  * hence not officially supported or ready for production use yet.
  */
object CustomerSqoopImportExecution extends MaestroExecution[Customer] {
  /** Configuration for `CustomerSqoopImportExecution` */
  case class SqoopImportConfig[T <: ParlourImportOptions[T]](config: Config, options: T) {
    val maestro = MaestroConfig(
      conf      = config,
      source    = "sales",
      domain    = "books",
      tablename = "customer_import"
    )
    val load    = maestro.load(
      none      = "null"
    )

    // TODO will be moved into config classes shortly
    // TODO some hiveQuery config not moved here, will also move into config classes shortly
    val timePath      = maestro.loadTime.toString("yyyy/MM/dd")
    val connString    = maestro.args("jdbc")
    val username      = maestro.args("db-user")
    val password      = Option(System.getenv("DBPASSWORD")).getOrElse("")
    val importOptions = createSqoopImportOptions(connString, username, password, maestro.tablename, '|', "", None, options).splitBy("id")
    val catTable      = HiveTable("customer", "by_cat",  Partition.byField(Fields.Cat))
  }

  /** Create an example customer sqoop import execution */
  def execute[T <: ParlourImportOptions[T]](
    options: T
  ): Execution[CustomerImportStatus] = for {
    conf               <- Execution.getConfig.map(SqoopImportConfig(_, options))
    (path, sqoopCount) <- sqoopImport(conf.maestro.hdfsRoot, conf.maestro.source, conf.maestro.domain, conf.timePath, conf.importOptions)
    (pipe, loadInfo)   <- load[Customer](conf.load)(List(path))
    loadSuccess        <- loadInfo.continueWithStats
    hiveCount          <- viewHive(conf.catTable)(pipe)
  } yield (CustomerImportStatus(sqoopCount, loadSuccess.written, hiveCount))
}

case class CustomerImportStatus(sqoopCount: Long, loadCount: Long, hiveCount: Long)
