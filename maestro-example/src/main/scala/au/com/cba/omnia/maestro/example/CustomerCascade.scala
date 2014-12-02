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

import scalaz.{Tag => _, _}, Scalaz._

import java.util.UUID

import com.twitter.scalding._, TDsl._

import org.apache.hadoop.hive.conf.HiveConf.ConfVars._

import au.com.cba.omnia.permafrost.hdfs.{Error, Ok}

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.example.thrift.{Account, Customer}

class CustomerCascade(args: Args) extends MaestroCascade[Customer](args) {
  val hdfsRoot    = args("hdfs-root")
  val source      = "customer"
  val domain      = "customer"
  val tablename   = "customer"
  val localRoot   = args("local-root")
  val archiveRoot = args("archive-root")
  val errors      = s"${hdfsRoot}/errors/${domain}"
  val validators  = Validator.all[Customer]()
  val filter      = RowFilter.keep
  val cleaners    = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )
  val dateTable =
    HiveTable(domain, "by_date", Partition.byDate(Fields.EffectiveDate), args.optional("by_date"))
  val catTable  =
    HiveTable(domain, "by_cat", Partition.byField(Fields.Cat), args.optional("by_cat"))
  val conf = configuration(args)

  upload(source, domain, tablename, "{table}_{yyyyMMdd}.txt", localRoot, archiveRoot, hdfsRoot, conf) match {
    case Ok(_)    => {}
    case Error(e) => throw new Exception(s"Failed to upload file to HDFS: $e")
  }

  val inputs = Guard.expandPaths(s"${hdfsRoot}/source/${source}/${domain}/${tablename}/*/*/*")

  val jobs = Seq(
    new UniqueJob(args) {
      load[Customer](
        "|", inputs, errors,
        timeFromPath(".*/([0-9]{4})/([0-9]{2})/([0-9]{2}).*".r),
        cleaners, validators, filter, "null"
      ) |>
      (viewHive(dateTable) _ &&&
        viewHive(catTable)
      )
    },
    hiveQuery(
      args, "test",
      dateTable, Some(catTable),
      Map(HIVEMERGEMAPFILES -> "true"),
      s"INSERT OVERWRITE TABLE ${catTable.name} PARTITION (partition_cat) SELECT id, name, acct, cat, sub_cat, -10, effective_date, cat AS partition_cat FROM ${dateTable.name}",
      s"SELECT COUNT(*) FROM ${catTable.name}"
    )
  )
}

class TransformCustomerCascade(args: Args) extends Maestro[Customer](args) {
  val env           = args("env")
  val domain        = "customer"
  val inputs        = Guard.expandPaths(s"${env}/source/${domain}/*")
  val errors        = s"${env}/errors/${domain}"
  val customerView  = s"${env}/view/warehouse/${domain}/customer"
  val accountView   = s"${env}/view/warehouse/${domain}/account"
  def accountFields = Macros.mkFields[Account]

  val transformer   =
    Macros.mkTransform[Customer, Account](
      ('id,           (c: Customer) => c.acct),
      ('customer,     (c: Customer) => c.id ),
      ('balance,      (c: Customer) => c.balance / 100),
      ('balanceCents, (c: Customer) => c.balance % 100)
    )

  val cleaners      = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )

  val validators    = Validator.all(
    Validator.of(Fields.SubCat, Check.oneOf("M", "F")),
    Validator.by[Customer](_.acct.length == 4, "Customer accounts should always be a length of 4")
  )

  val filter        = RowFilter.keep
  val timeSource    = Maestro.timeFromPath(""".*(\d{4})-(\d{2})-(\d{2}).*""".r)

  val in = load[Customer]("|", inputs, errors, timeSource, cleaners, validators, filter, "null")

  view(Partition.byDate(Fields.EffectiveDate), customerView)(in)

  in.map(transformer.run) |>
  view(Partition.byDate(accountFields.EffectiveDate), accountView)
}
