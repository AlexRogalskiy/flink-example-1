package org.example.util.flink

import org.apache.flink.api.common.functions.MapFunction
import org.json4s._
import org.json4s.native.Serialization

class CaseClassToJsonConverter[T <: Product with Serializable] extends MapFunction[T, String] {
  lazy implicit val formats: Formats = DefaultFormats

  override def map(value: T): String = Serialization.write(value)
}
