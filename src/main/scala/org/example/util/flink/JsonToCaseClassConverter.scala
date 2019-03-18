package org.example.util.flink

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import org.json4s._
import org.json4s.native.JsonMethods

import scala.util.{Failure, Success, Try}

class JsonToCaseClassConverter[T <: Product with Serializable](strictMode: Boolean = true,
                                                               additionalFormats: Seq[CustomSerializer[_]] = Seq.empty)
                                                              (implicit m: Manifest[T])
  extends FlatMapFunction[String, T] {

  lazy implicit val formats: Formats = DefaultFormats ++ additionalFormats

  override def flatMap(value: String, out: Collector[T]): Unit = {
    Try(JsonMethods.parse(value).extract[T]) match {
      case Success(x) => out.collect(x)
      case Failure(exception) => if (strictMode) throw exception
    }
  }
}
