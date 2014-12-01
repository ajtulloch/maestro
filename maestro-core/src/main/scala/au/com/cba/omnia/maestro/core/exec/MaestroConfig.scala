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

package au.com.cba.omnia.maestro.core.exec

import java.sql.Timestamp

import org.joda.time.{DateTime, DateTimeZone}

import com.twitter.scrooge.ThriftStruct

import com.twitter.scalding.{Args, Config, Mode, TupleSetter}
import com.twitter.scalding.typed.TypedPipe

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl

import au.com.cba.omnia.maestro.core.codec.{Decode, Tag}
import au.com.cba.omnia.maestro.core.clean.Clean
import au.com.cba.omnia.maestro.core.filter.RowFilter
import au.com.cba.omnia.maestro.core.split.Splitter
import au.com.cba.omnia.maestro.core.time.TimeSource
import au.com.cba.omnia.maestro.core.validate.Validator

case class MaestroConfig(
  conf: Config,
  args: Args,
  source: String,
  domain: String,
  tablename: String,
  hdfsRoot: String,
  loadTime: DateTime
) {
  val dirStructure = s"${source}/${domain}/${tablename}"

  /** Produces a load config for this job. */
  def load[A <: ThriftStruct : Decode : Tag : Manifest](
    errors: String          = s"""$hdfsRoot/errors/$dirStructure/${loadTime.toString("yyyy-MM-dd-hh-mm-ss")}""",
    splitter: Splitter      = Splitter.delimited("|"),
    timeSource: TimeSource  = TimeSource.fromDirStructure,
    loadWithKey: Boolean    = false,
    filter: RowFilter       = RowFilter.keep,
    clean: Clean            = Clean.default,
    none: String            = "",
    validator: Validator[A] = Validator.all[A](),
    errorThreshold: Double  = 0.05
  ): LoadConfig[A] = LoadConfig(
    errors, splitter, timeSource, loadWithKey, filter, clean, none,
    validator, errorThreshold
  )
}

object MaestroConfig {
  def apply(conf: Config, source: String, domain: String, tablename: String): MaestroConfig = {
    val args = conf.getArgs
    MaestroConfig(conf, args, source, domain, tablename, args.required("hdfs-root"), DateTime.now)
  }
}
