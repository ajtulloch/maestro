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

package au.com.cba.omnia.maestro.core.hive

import cascading.flow.FlowDef

import org.apache.hadoop.fs.Path

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE

import com.twitter.scalding.{TupleSetter, TypedSink, TypedPipe, Source, Execution, Mode, Mappable}

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.maestro.core.partition.Partition

import au.com.cba.omnia.ebenezer.scrooge.hive._

/** 
  * Base type for partitioned and unpartitioned Hive tables. 
  * ST - Type parameter of [[TypedSink]] and [[TypedPipe]]
  * returned in `sink` and `write` methods respectively. 
  */
sealed trait HiveTable[T <: ThriftStruct, ST] {
  /** Fully qualified SQL reference to table.*/
  val name: String = s"$database.$table"

  /** Path of the table. */
  lazy val path = new Path(externalPath.getOrElse(
    s"${(new HiveConf()).getVar(METASTOREWAREHOUSE)}/$database.db/$table"
  ))

  def database: String
  def table: String
  def externalPath: Option[String]

  /** Creates a scalding source to read from the hive table.*/
  def source: Mappable[T]
  /** Creates a scalding sink to write to the hive table.*/
  def sink(append: Boolean = true): TypedSink[ST] with Source
  /** Creates [[Execution]] that writes to `this.sink` using given [[TypedPipe]].*/
  def writeExecution(pipe: TypedPipe[T], append: Boolean = true): Execution[Unit]
  /** Writes to `this.sink` using given [[TypedPipe]].*/ 
  def write(pipe: TypedPipe[T], append: Boolean = true)
    (implicit flowDef: FlowDef, mode: Mode): TypedPipe[ST]
}

/** Information need to address/describe a specific partitioned hive table.*/
case class PartitionedHiveTable[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter](
  database: String, table: String, partition: Partition[A, B], externalPath: Option[String] = None
) extends HiveTable[A, (B, A)] {

  /** List of partition column names and type (string by default). */ 
  val partitionMetadata: List[(String, String)] = partition.fieldNames.map(n => (n, "string"))

  override def source(): PartitionHiveParquetScroogeSource[A] =
    PartitionHiveParquetScroogeSource[A](database, table, partitionMetadata)

  override def sink(append: Boolean = true) =
    PartitionHiveParquetScroogeSink[B, A](database, table, partitionMetadata, externalPath, append)

  override def writeExecution(pipe: TypedPipe[A], append: Boolean = true): Execution[Unit] =
    pipe.map(v => partition.extract(v) -> v).writeExecution(sink(append))

  override def write(pipe: TypedPipe[A], append: Boolean = true)
    (implicit flowDef: FlowDef, mode: Mode): TypedPipe[(B, A)] =
    pipe.map(v => partition.extract(v) -> v).write(sink(append))
}

/** Information need to address/describe a specific unpartitioned hive table.*/
case class UnpartitionedHiveTable[A <: ThriftStruct : Manifest](
  database: String, table: String, externalPath: Option[String] = None
) extends HiveTable[A, A] {

  override def source() =
    new HiveParquetScroogeSource[A](database, table, None)

  override def sink(append: Boolean = true) =
    new HiveParquetScroogeSource[A](database, table, None)

  override def writeExecution(pipe: TypedPipe[A], append: Boolean = true): Execution[Unit] =
    pipe.writeExecution(sink(append))

  override def write(pipe: TypedPipe[A], append: Boolean = true)
    (implicit flowDef: FlowDef, mode: Mode): TypedPipe[A] =
    pipe.write(sink(append))
}

/** Contructors for HiveTable. */
object HiveTable {
  /** Information need to address/describe a specific partitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter](
    database: String, table: String, partition: Partition[A, B], path: String
  ): HiveTable[A, (B,A)] =
    new PartitionedHiveTable(database, table, partition, Some(path))

  /** Information need to address/describe a specific partitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter](
    database: String, table: String, partition: Partition[A, B], pathOpt: Option[String]
  ): HiveTable[A, (B, A)] =
    new PartitionedHiveTable(database, table, partition, pathOpt)

  /** Information need to address/describe a specific unpartitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest](
    database: String, table: String, path: String
  ): HiveTable[A, A] =
    new UnpartitionedHiveTable(database, table, Some(path))

  /** Information need to address/describe a specific unpartitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest](
    database: String, table: String, pathOpt: Option[String]
  ): HiveTable[A, A] =
    new UnpartitionedHiveTable(database, table, pathOpt)
}
