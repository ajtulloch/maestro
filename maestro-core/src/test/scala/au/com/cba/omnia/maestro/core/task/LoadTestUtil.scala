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

package au.com.cba.omnia.maestro.core
package task

import java.text.SimpleDateFormat
import java.util.TimeZone

import au.com.cba.omnia.maestro.core.clean.Clean
import au.com.cba.omnia.maestro.core.codec.{Decode, Tag}
import au.com.cba.omnia.maestro.core.data.Field
import au.com.cba.omnia.maestro.core.filter.RowFilter
import au.com.cba.omnia.maestro.core.time.TimeSource
import au.com.cba.omnia.maestro.core.validate.Validator

import au.com.cba.omnia.maestro.core.thrift.scrooge.StringPair

trait LoadTestUtil {
  val clean      = Clean.all()
  val validator  = Validator.all[StringPair]()
  val filter     = RowFilter.keep
  val now        = TimeSource.now()

  implicit val StringPairDecode: Decode[StringPair] = for {
    first  <- Decode.of[String]
    second <- Decode.of[String]
  } yield StringPair(first, second)

  implicit val StringPairTag: Tag[StringPair] = {
    val fields =
      Field("FIRST", (p:StringPair) => p.first) +:
    Field("SECOND",(p:StringPair) => p.second) +:
    Stream.continually[Field[StringPair,String]](
      Field("UNKNOWN", _ => throw new Exception("invalid field"))
    )

    Tag(_ zip fields)
  }
}
