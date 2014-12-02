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

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl

/** Please be aware that the Execution API is being actively developed/modified and
  * hence not officially supported or ready for production use yet.
  */
object CustomerSqoopImportExecution extends MaestroExecution[Customer] {
  def execute(
    hdfsRoot: String, 
    connectionString: String, 
    username: String, 
    password: String,
    options: ParlourImportDsl
  ): Execution[CustomerImportStatus] = {
    val source     = "sales"
    val domain     = "books"
    val table      = "customer_import"
    val errors     = s"${hdfsRoot}/errors/${domain}"
    val timeSource = TimeSource.now()
    val filter     = RowFilter.keep
    val cleaners   = Clean.all(Clean.trim, Clean.removeNonPrintables)
    val validators = Validator.all[Customer]()
    val timePath   = DateTime.now(DateTimeZone.UTC).toString(List("yyyy","MM", "dd") mkString File.separator)
    val catTable   = HiveTable("customer", "by_cat", Partition.byField(Fields.Cat))

    val importOptions = createSqoopImportOptions(connectionString, username, 
      password, table, '|', "", None, options).splitBy("id")

    for {
      (path, sqoopCount) <- sqoopImport(hdfsRoot, source, domain, timePath, importOptions)
      (pipe, loadInfo)   <- load[Customer]("|", List(path), errors, timeSource, cleaners, validators, filter, "null")
      if loadInfo.continue
      loadCount = loadInfo.asInstanceOf[LoadSuccess].written
      hiveCount          <- viewHive(catTable)(pipe)
    } yield (CustomerImportStatus(sqoopCount, loadCount, hiveCount))
  }
}

case class CustomerImportStatus(sqoopCount: Long, loadCount: Long, hiveCount: Long)
