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

import cascading.flow.FlowDef

import com.twitter.scalding._, TDsl._

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.hive.conf.HiveConf

import au.com.cba.omnia.ebenezer.scrooge.PartitionParquetScroogeSource

import au.com.cba.omnia.maestro.core.partition.Partition
import au.com.cba.omnia.maestro.core.hive._

trait View {
  /** Partitions a pipe using the given partition scheme and writes out the data.*/
  def view[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter]
    (partition: Partition[A, B], output: String)
    (pipe: TypedPipe[A])
    (implicit flowDef: FlowDef, mode: Mode): Unit = {
    pipe
      .map(v => partition.extract(v) -> v)
      .write(PartitionParquetScroogeSource[B, A](partition.pattern, output))
  }

  /**
    * Writes out the data to a hive table.
    * 
    * This will create the table if it doesn't already exist. If the existing schema doesn't match
    * the schema expected the job will fail.
    * 
    * @param append iff true add files to an already existing table.
    */
  def viewHive[A <: ThriftStruct : Manifest, ST]
    (table: HiveTable[A, ST], append: Boolean) (pipe: TypedPipe[A])
    (implicit flowDef: FlowDef, mode: Mode): Unit = 
    table.write(pipe, append)
}
