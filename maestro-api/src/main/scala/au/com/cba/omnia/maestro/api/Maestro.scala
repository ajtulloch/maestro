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

package au.com.cba.omnia.maestro.api

import com.twitter.scrooge.ThriftStruct

import com.twitter.scalding.{Args, Job, CascadeJob}

import au.com.cba.omnia.maestro.macros.MacroSupport

import au.com.cba.omnia.maestro.core.task._
import au.com.cba.omnia.maestro.core.args.Config
import au.com.cba.omnia.maestro.core.hdfs.OldGuardFunctions
import au.com.cba.omnia.maestro.core.time.OldTimeSourceFunctions

/**
  * Parent class for a more complex maestro job that needs to use cascades. For example, to run hive
  * queries.
  */
abstract class MaestroCascade[A <: ThriftStruct](args: Args) extends CascadeJob(args) with MacroSupport[A] {
  /** Don't run any cascading jobs if the job list is empty. */
  override def run =
    if (jobs.isEmpty) true
    else super.run

  override def validate {
    /* scalding validate class chokes on cascades: don't call super.validate */

    val jobNames = jobs.map(_.name)
    if (jobNames.toSet.size != jobNames.length) {
      throw new Exception(s"Cascade jobs do not have unique names. The names are: ${jobNames.mkString(", ")}")
    }
  }
}

/** Parent class for a simple maestro job that does not need to use cascades or run hive queries.*/
abstract class Maestro[A <: ThriftStruct](args: Args) extends Job(args) with MacroSupport[A]

object Maestro
    extends Load
    with View
    with Query
    with Upload
    with Sqoop
    with Config
    with OldTimeSourceFunctions
    with OldGuardFunctions
