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

import au.com.cba.omnia.maestro.core.exec.LoadExecution
import au.com.cba.omnia.maestro.core.task._ // TODO should be removed soon
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
  val  HiveTable       = au.com.cba.omnia.maestro.core.hive.HiveTable
  val  Partition       = au.com.cba.omnia.maestro.core.partition.Partition
  val  HivePartition   = au.com.cba.omnia.maestro.core.partition.HivePartition

  type TimeSource      = au.com.cba.omnia.maestro.core.time.TimeSource
  val  TimeSource      = au.com.cba.omnia.maestro.core.time.TimeSource

  type Clean           = au.com.cba.omnia.maestro.core.clean.Clean
  val  Clean           = au.com.cba.omnia.maestro.core.clean.Clean

  type Validator[A]    = au.com.cba.omnia.maestro.core.validate.Validator[A]
  val  Validator       = au.com.cba.omnia.maestro.core.validate.Validator
  val  Check           = au.com.cba.omnia.maestro.core.validate.Check

  type RowFilter       = au.com.cba.omnia.maestro.core.filter.RowFilter
  val  RowFilter       = au.com.cba.omnia.maestro.core.filter.RowFilter

  type Splitter        = au.com.cba.omnia.maestro.core.split.Splitter
  val  Splitter        = au.com.cba.omnia.maestro.core.split.Splitter

  type Decode[A]       = au.com.cba.omnia.maestro.core.codec.Decode[A]
  type Transform[A, B] = au.com.cba.omnia.maestro.core.transform.Transform[A, B]
  type Tag[A]          = au.com.cba.omnia.maestro.core.codec.Tag[A]
  type Field[A, B]     = au.com.cba.omnia.maestro.core.data.Field[A, B]
  val  Macros          = au.com.cba.omnia.maestro.macros.Macros

  type GuardFilter     = au.com.cba.omnia.maestro.core.hdfs.GuardFilter
  val  Guard           = au.com.cba.omnia.maestro.core.hdfs.Guard

  type Config          = com.twitter.scalding.Config
  type Execution[A]    = com.twitter.scalding.Execution[A]
  val  Execution       = com.twitter.scalding.Execution

  type MaestroConfig   = au.com.cba.omnia.maestro.core.exec.MaestroConfig
  val  MaestroConfig   = au.com.cba.omnia.maestro.core.exec.MaestroConfig

  type LoadConfig[A]   = au.com.cba.omnia.maestro.core.exec.LoadConfig[A]
  type LoadInfo        = au.com.cba.omnia.maestro.core.exec.LoadInfo
  type LoadSuccess     = au.com.cba.omnia.maestro.core.exec.LoadSuccess
  type LoadFailure     = au.com.cba.omnia.maestro.core.exec.LoadFailure
  val  LoadSuccess     = au.com.cba.omnia.maestro.core.exec.LoadSuccess
  val  LoadFailure     = au.com.cba.omnia.maestro.core.exec.LoadFailure
  val  EmptyLoad       = au.com.cba.omnia.maestro.core.exec.EmptyLoad

  type UploadInfo      = au.com.cba.omnia.maestro.core.task.UploadInfo
  val  UploadInfo      = au.com.cba.omnia.maestro.core.task.UploadInfo
}
