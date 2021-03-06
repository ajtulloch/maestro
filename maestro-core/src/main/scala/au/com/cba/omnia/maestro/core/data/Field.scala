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
package data

/**
  * Represents a field of `A` with type `B`. It has the name of the field and a getter given an
  * instance of `A`.
  */
case class Field[A : Manifest, B : Manifest](name: String, get: A => B) {
  val structType = manifest[A].getClass
  val columnType = manifest[B].getClass

  /**
   * Fields are consider equal if name and type of Thrift struct are equal and column type are equal
   * Notice that this function will be working correctly only with Fields generated by FieldsMacro.
   * Do not try to use it with custom created fields.
   *
   * @throws RuntimeException when encounters 2 fields with same name from the same Thrift struct with different column type,
   *                          the intention is to indicate serious error in logic of your program.
   */
  override def equals(other: Any): Boolean = other match {
    case f: Field[_, _] => equalityTest(f)
    case _              => false
  }

  private def equalityTest(f: Field[_, _]): Boolean = {
    val equalFields = structType == f.structType && name == f.name
    if (equalFields && columnType != f.columnType) {
      throw new RuntimeException("Can't have two columns with the same name from the same Thrift structure with different column type")
    }
    equalFields
  }

  override def hashCode: Int = name.hashCode * 41 + structType.hashCode
}
