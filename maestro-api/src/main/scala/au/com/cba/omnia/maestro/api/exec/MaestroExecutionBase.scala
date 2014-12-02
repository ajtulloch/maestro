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

package au.com.cba.omnia.maestro.api.exec

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.maestro.core.task._
import au.com.cba.omnia.maestro.core.args.Config

trait MaestroExecutionBase
  extends UploadExecution
  with LoadExecution
  with ViewExecution
  with QueryExecution
  with SqoopExecution
  with Config {

  type HiveTable[A <: ThriftStruct, B] = au.com.cba.omnia.maestro.core.hive.HiveTable[A, B]
  type Partition[A, B] = au.com.cba.omnia.maestro.core.partition.Partition[A, B]
  type TimeSource      = au.com.cba.omnia.maestro.core.time.TimeSource
  type Clean           = au.com.cba.omnia.maestro.core.clean.Clean
  type Validator[A]    = au.com.cba.omnia.maestro.core.validate.Validator[A]
  type RowFilter       = au.com.cba.omnia.maestro.core.filter.RowFilter
  type Decode[A]       = au.com.cba.omnia.maestro.core.codec.Decode[A]
  type Tag[A]          = au.com.cba.omnia.maestro.core.codec.Tag[A]
  type Field[A, B]     = au.com.cba.omnia.maestro.core.data.Field[A, B]
  type Splitter        = au.com.cba.omnia.maestro.core.split.Splitter
  type Transform[A, B] = au.com.cba.omnia.maestro.core.transform.Transform[A, B]
  type Execution[A]    = com.twitter.scalding.Execution[A]

  type LoadInfo        = au.com.cba.omnia.maestro.core.task.LoadInfo
  type LoadFailure     = au.com.cba.omnia.maestro.core.task.LoadFailure
  type LoadSuccess     = au.com.cba.omnia.maestro.core.task.LoadSuccess

  type UploadInfo      = au.com.cba.omnia.maestro.core.task.UploadInfo

  val HiveTable     = au.com.cba.omnia.maestro.core.hive.HiveTable
  val Partition     = au.com.cba.omnia.maestro.core.partition.Partition
  val HivePartition = au.com.cba.omnia.maestro.core.partition.HivePartition
  val TimeSource    = au.com.cba.omnia.maestro.core.time.TimeSource
  val Clean         = au.com.cba.omnia.maestro.core.clean.Clean
  val Validator     = au.com.cba.omnia.maestro.core.validate.Validator
  val Check         = au.com.cba.omnia.maestro.core.validate.Check
  val RowFilter     = au.com.cba.omnia.maestro.core.filter.RowFilter
  val Guard         = au.com.cba.omnia.maestro.core.hdfs.Guard
  val Splitter      = au.com.cba.omnia.maestro.core.split.Splitter
  val Macros        = au.com.cba.omnia.maestro.macros.Macros
  val ModArgs       = au.com.cba.omnia.maestro.core.args.ModArgs
  val Execution     = com.twitter.scalding.Execution
}
