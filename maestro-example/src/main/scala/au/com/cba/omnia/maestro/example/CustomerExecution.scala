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

/** Please be aware that the Execution API is being actively developed/modified and
  * hence not officially supported or ready for production use yet.
  */
object CustomerExecution extends MaestroExecution[Customer] {
  /** Configuration for a customer execution example */
  case class CustomerConfig(config: Config) {
    val maestro = MaestroConfig(
      conf      = config,
      source    = "customer",
      domain    = "customer",
      tablename = "customer"
    )
    val load    = maestro.load(
      none      = "null"
    )

    // TODO will be moved into config classes shortly
    // TODO some hiveQuery config not moved here, will also move into config classes shortly
    val filePattern = "{table}_{yyyyMMdd}.txt"
    val localRoot   = maestro.args("local-root")
    val archiveRoot = maestro.args("archive-root")
    val dateTable   = HiveTable(maestro.domain, "by_date", Partition.byDate(Fields.EffectiveDate))
    val catTable    = HiveTable(maestro.domain, "by_cat",  Partition.byField(Fields.Cat))
  }

  /** Create an example customer execution */
  def execute: Execution[(LoadInfo, Long)] = for {
    conf       <- Execution.getConfig.map(CustomerConfig(_))
    uploadInfo <- upload(conf.maestro.source, conf.maestro.domain, conf.maestro.tablename, conf.filePattern, conf.localRoot, conf.archiveRoot, conf.maestro.hdfsRoot)

    if uploadInfo.continue
    (pipe, loadInfo) <- load[Customer](conf.load)(uploadInfo.files)

    if loadInfo.continue
    (count1, _) <- viewHive(conf.dateTable)(pipe) zip viewHive(conf.catTable)(pipe)
    _           <- hiveQuery(
      "test", conf.catTable,
      Map(HIVEMERGEMAPFILES -> "true"),
      s"INSERT OVERWRITE TABLE ${conf.catTable.name} PARTITION (partition_cat) SELECT id, name, acct, cat, sub_cat, -10, effective_date, cat AS partition_cat FROM ${conf.dateTable.name}",
      s"SELECT COUNT(*) FROM ${conf.catTable.name}"
    )
  } yield (loadInfo, count1)
}
