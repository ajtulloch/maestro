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

package au.com.cba.omnia.maestro.core.time

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import scala.util.matching.Regex

/** Retrieve a load's effective date */
sealed trait TimeSource {
  def getTime(path: String): String = this match {
    case Predetermined(time) => time
    case FromPath(extract)  => extract(path)
  }
}

case class Predetermined(time: String) extends TimeSource
case class FromPath(extract: String => String) extends TimeSource

/** Factory functions for time sources */
object TimeSource {
  /** Use a predetermined date time string as the time source */
  def predetermined(time: String): TimeSource =
    Predetermined(time)

  /** Use a predetermined date as the time source */
  def predetermined(time: DateTime, format: String = "yyyy-MM-dd"): TimeSource = {
    val f = DateTimeFormat.forPattern(format).withZoneUTC
    predetermined(f.print(time))
  }

  /** Use the current time as the time source */
  def now(format: String = "yyyy-MM-dd"): TimeSource =
    predetermined(new DateTime, format)

  /** Derive the load time from the file path */
  def fromPath(extract: String => String): TimeSource =
    FromPath(extract)

  /**
    * Derive the load time from the file path using the provided regex.
    *
    * The regex needs to extract the year, month and day into separate capture groups.
    */
  def fromPath(regex: Regex): TimeSource = fromPath { (path: String) =>
    val m = regex.pattern.matcher(path)
    m.matches
    s"${m.group(1)}-${m.group(2)}-${m.group(3)}"
  }

  /**
    * Derive the load time from the file path using the provided regex.
    *
    * The regex needs to extract the year, month, day, and hour into separate capture groups.
    */
  def fromPathHour(regex: Regex): TimeSource = fromPath { (path: String) =>
    val m = regex.pattern.matcher(path)
    m.matches
    s"${m.group(1)}-${m.group(2)}-${m.group(3)}-${m.group(4)}"
  }

  /**
    * Derive the load time from the file path using the provided regex.
    *
    * The regex needs to extract the year, month, day, hour, minute, and second into separate capture groups.
    */
  def fromPathSecond(regex: Regex): TimeSource = fromPath { (path: String) =>
    val m = regex.pattern.matcher(path)
    m.matches
    s"${m.group(1)}-${m.group(2)}-${m.group(3)} ${m.group(4)}:${m.group(5)}:${m.group(6)}"
  }

  /** A time source which takes the year, month, and day from our standard directory structure */
  def fromDirStructure: TimeSource =
    fromPath(".*/([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/.*".r)

  /** A time source which takes the year, month, day, and hour from our standard directory structure */
  def fromDirStructureHour: TimeSource =
    fromPathHour(".*/([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/([0-9]{1,2})/.*".r)

  /** A time source which takes the year, month, day, hour, minute, and second from our standard directory structure */
  def fromDirStructureSecond: TimeSource =
    fromPathSecond(".*/([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/([0-9]{1,2})/([0-9]{1,2})/([0-9]{1,2})/.*".r)
}

/** Trait for time source factory functions that used to be provided via a trait, maintained for backwards compatibility */
trait OldTimeSourceFunctions {
  /** Use the current time yyyy-MM-dd as the load time for the data */
  def now(format: String = "yyyy-MM-dd") = TimeSource.now(format)

  /**
    * Derive the load time from the file path using the provided regex.
    * The regex needs to extract the year,month and day into separate capture groups.
    */
  def timeFromPath(regex: Regex): TimeSource = TimeSource.fromPath(regex)

  /** Extracts time from path up to hour level using given Regex */
  def timeFromPathHour(regex: Regex): TimeSource = TimeSource.fromPathHour(regex)

  /** Extracts time from path up to seconds level using given Regex */
  def timeFromPathSecond(regex: Regex): TimeSource = TimeSource.fromPathSecond(regex)
}
