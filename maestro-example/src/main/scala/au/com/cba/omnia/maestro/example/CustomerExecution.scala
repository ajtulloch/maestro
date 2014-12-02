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

import org.apache.hadoop.hive.conf.HiveConf.ConfVars._

import au.com.cba.omnia.maestro.api.exec.MaestroExecution
import au.com.cba.omnia.maestro.example.thrift.{Account, Customer}

object CustomerExecution extends MaestroExecution[Customer] {
  def execute(hdfsRoot: String, localRoot: String, archiveRoot: String): Execution[(LoadInfo, Long)] = {
    val source      = "customer"
    val domain      = "customer"
    val tablename   = "customer"
    val errors      = s"${hdfsRoot}/errors/${domain}"
    val timeSource  = TimeSource.fromDirStructure
    val validators  = Validator.all[Customer]()
    val filter      = RowFilter.keep
    val cleaners    = Clean.all(
      Clean.trim,
      Clean.removeNonPrintables
    )
    val dateTable =
      HiveTable(domain, "by_date", Partition.byDate(Fields.EffectiveDate) )
    val catTable  =
      HiveTable(domain, "by_cat", Partition.byField(Fields.Cat))

    for {
      uploadInfo <- upload(source, domain, tablename, "{table}_{yyyyMMdd}.txt", localRoot, archiveRoot, hdfsRoot)

      if uploadInfo.continue
      (pipe, loadInfo) <- load[Customer](
        "|", uploadInfo.files, errors, timeSource,
        cleaners, validators, filter, "null"
      )

      if loadInfo.continue
      (count1, _) <- viewHive(dateTable)(pipe).zip(viewHive(catTable)(pipe))
      _           <- hiveQuery(
        "test", catTable,
        Map(HIVEMERGEMAPFILES -> "true"),
        s"INSERT OVERWRITE TABLE ${catTable.name} PARTITION (partition_cat) SELECT id, name, acct, cat, sub_cat, -10, effective_date, cat AS partition_cat FROM ${dateTable.name}",
        s"SELECT COUNT(*) FROM ${catTable.name}"
      )
    } yield (loadInfo, count1)
  }
}
