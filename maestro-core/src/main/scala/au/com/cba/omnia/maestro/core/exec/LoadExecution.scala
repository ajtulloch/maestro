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

import java.security.{MessageDigest, SecureRandom}

import com.twitter.scalding.{Execution, ExecutionCounters, MultipleTextLineFiles, TypedPipe}
import com.twitter.scalding.typed.TypedPipeFactory
import com.twitter.scalding.Dsl._ // Pipe.eachTo

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.maestro.core.codec.{Decode, Tag}
import au.com.cba.omnia.maestro.core.clean.Clean
import au.com.cba.omnia.maestro.core.filter.RowFilter
import au.com.cba.omnia.maestro.core.scalding.StatKeys
import au.com.cba.omnia.maestro.core.split.Splitter
import au.com.cba.omnia.maestro.core.task.{ExtractTime, GenerateKey, Load, RawRow}
import au.com.cba.omnia.maestro.core.time.TimeSource
import au.com.cba.omnia.maestro.core.validate.Validator

/** Information about a Load */
sealed trait LoadInfo {
  def continue: Boolean = this match {
    case EmptyLoad            => false
    case LoadFailure(_, _, _) => false
    case LoadSuccess(_, _, _) => true
  }

  def continueWithStats: Execution[LoadSuccess] = for {
    ls @ LoadSuccess(_, _, _) <- Execution.from(this)
    if this.continue
  } yield ls
}

/** The Load had no new data to process */
case object EmptyLoad extends LoadInfo

/** The number of rows which failed to load exceeded our error threshold */
case class LoadFailure(read: Long, written: Long, failed: Long) extends LoadInfo

/** The number of rows which failed to load was within acceptable limits */
case class LoadSuccess(read: Long, written: Long, failed: Long) extends LoadInfo

/** Factory methods for `LoadInfo` */
object LoadInfo {
  def fromCounters(counters: ExecutionCounters, errorThreshold: Double): LoadInfo = {
    // If there is no data, nothing will be read in and this counter won't be set
    val read    = counters.get(StatKeys.tuplesRead).getOrElse(0l)
    // If all values fail nothing will be written out and this counter won't be set
    val written = counters.get(StatKeys.tuplesWritten).getOrElse(0l)
    // If all values pass nothing will be written out and this counter won't be set
    val failed  = counters.get(StatKeys.tuplesTrapped).getOrElse(read - written)

    if (read == 0) EmptyLoad
    else if (failed / read.toDouble >= errorThreshold) LoadFailure(read, written, failed)
    else LoadSuccess(read, written, failed)
  }
}

/**
  * Configuration options for load
  *
  * @param errors: The hdfs path used to store rows which failed processing.
  * @param splitter: The `Splitter` used to split rows into fields.
  *                  Defaults to splitting on the "|" character.
  * @param timeSource: The `TimeSource` used to find the effective date for each row.
  *                    Defaults to pulling the year, month, and day from the directory structure of the input file.
  * @param generateKey: If true, load will append a unique key to each row.
  *                     Defaults to false.
  * @param filter: The `RowFilter` which scans each row after splitting,
  *                potentially discarding bad rows. Defaults to now filtering.
  * @param clean: The `Clean` function to be applied to each field.
  *               Defaults to trimming fields and removing non-printable characters.
  * @param none: The field value denoting a `None` value for an optional field.
  *              Optional string fields are `None` only if they match this value.
  *              Other optional fields are `None` if they match this or the empty string.
  *              Defaults to the empty sring.
  * @param validator: The `Validator` to check the thrift struct to ensure it is correct.
  *                   Defaults to no validation.
  * @param errorThreshold: The fraction of rows that can fail during the load step
  *                        before the overall job should fail. Defaults to 0.05 or 5%.
  */
case class LoadConfig[A](
  errors: String,
  splitter: Splitter      = Splitter.delimited("|"),
  timeSource: TimeSource  = TimeSource.fromDirStructure,
  generateKey: Boolean    = false,
  filter: RowFilter       = RowFilter.keep,
  clean: Clean            = Clean.default,
  none: String            = "",
  validator: Validator[A] = Validator.all[A](),
  errorThreshold: Double  = 0.05
)

/** Executions for load tasks */
trait LoadExecution {
  /**
    * Loads the supplied text files and converts them to the specified thrift struct.
    *
    * The operations performed are:
    *  1. Append a time field to each line using the provided time source
    *  1. Split each line into columns/fields using the provided splitter
    *  1. Apply the provided filter to each list of fields (defaults to no filtering).
    *  1. Clean each field using the provided cleaner
    *  1. Convert the list of fields into the provided thrift struct.
    *  1. Validate each struct (defaults to assuming all structs are ok).
    *  1. The fraction of successfull rows is compared against the error threshold
    *     and the appropriate `LoadInfo` value returned.
    */
  def load[A <: ThriftStruct : Decode : Tag : Manifest]
    (config: LoadConfig[A])(sources: List[String]): Execution[(TypedPipe[A], LoadInfo)] =
    LoadEx.load[A](config)(sources)
}

/**
  * WARNING: not considered part of the load execution api.
  * We may change this without considering backwards compatibility.
  */
object LoadEx extends Load {
  def load[A <: ThriftStruct : Decode : Tag : Manifest](config: LoadConfig[A])(sources: List[String]): Execution[(TypedPipe[A], LoadInfo)] = {
    val input = config.generateKey match {
      case false => pipeWithDate(sources, config.timeSource)
      case true  => pipeWithDateAndKey(sources, config.timeSource)
    }

    val pipe = Load.loadProcess(
      input, config.splitter, config.errors, config.clean, config.validator,
      config.filter, config.none
    )

    pipe.forceToDiskExecution.getAndResetCounters.map { case (pipe, counters) =>
      (pipe, LoadInfo.fromCounters(counters, config.errorThreshold))
    }
  }

  def pipeWithDate(sources: List[String], timeSource: TimeSource): TypedPipe[RawRow] =
    TypedPipeFactory { (flowDef, mode) =>
      TypedPipe.fromSingleField[RawRow](
        MultipleTextLineFiles(sources: _*)
          .read(flowDef, mode)
          .eachTo(('offset, 'line), 'result)(_ => new ExtractTime(timeSource))
      )(flowDef, mode)
    }

  def pipeWithDateAndKey(sources: List[String], timeSource: TimeSource): TypedPipe[RawRow] = {
    val rnd    = new SecureRandom()
    val seed   = rnd.generateSeed(4)
    val md     = MessageDigest.getInstance("SHA-1")
    val hashes =
      sources
        .map(k => k -> md.digest(seed ++ k.getBytes("UTF-8")).drop(12).map("%02x".format(_)).mkString)
        .toMap

    TypedPipeFactory { (flowDef, mode) =>
      TypedPipe.fromSingleField[RawRow](
        MultipleTextLineFiles(sources: _*)
          .read(flowDef, mode)
          .eachTo(('offset, 'line), 'result)(_ => new GenerateKey(timeSource, hashes))
      )(flowDef, mode)
    }
  }
}
