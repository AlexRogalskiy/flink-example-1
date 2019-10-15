package org.example.showcase

import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.example.showcase.clickstream._

object ClickStreamExampleJob extends App {

  /** Create environment */
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  /** Set parameters for connection to Kafka */
  val kafkaConsumerProperties = new Properties()
  kafkaConsumerProperties.setProperty("bootstrap.servers", "localhost:29092")

  /** Stream of page views */
  val pageviews: DataStream[PageView] = env
    .fromElements(
      PageView(10, "User1", "PageA"),
      PageView(11, "User1", "PageA"),
      PageView(12, "User1", "PageA"),
      PageView(20, "User2", "PageB"),
      PageView(40, "User3", "PageA"),
      PageView(Long.MaxValue, "User3", "PageA")
    )
    .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[PageView] {
      override def extractAscendingTimestamp(element: PageView): Long = element.viewtime
    })

  pageviews.print()

  /**
    * PATTERN 1: Windows and sessions
    *
    * Let's build user sessions!
    * Session stays active until there is no page views at least [sessionDelay] seconds.
    *
    */

  val sessionDelay = Time.seconds(2)

  /** Accumulator class - it will be used in the aggregation function below */
  case class SessionAccumulator(duration: Long, page_count: Long)


  val sessions = pageviews

    /** keyBy(keyExpression) acts like partitionBy in Spark but for streams instead of datasets */
    .keyBy(_.userid)

    /** We choose SessionWindow over Processing time.
      * Alternative: Time/count/custom count window over Event time. */
    .window(EventTimeSessionWindows.withGap(sessionDelay))

    /** Aggregation of elements grouped by window consists of two steps:
      * 1. Accumulate elements into one SessionAccumulator
      * 2. Merge such accumulators (yes, there can be more than one) and build session */
    .aggregate(

      /** 1 step */
      new AggregateFunction[PageView, SessionAccumulator, SessionAccumulator] {
        override def createAccumulator(): SessionAccumulator = SessionAccumulator(0, 0)

        override def add(value: PageView, accumulator: SessionAccumulator): SessionAccumulator =
          SessionAccumulator(accumulator.duration + value.viewtime, accumulator.page_count + 1)

        override def getResult(accumulator: SessionAccumulator): SessionAccumulator = accumulator

        override def merge(a: SessionAccumulator, b: SessionAccumulator): SessionAccumulator = SessionAccumulator(
          a.duration + b.duration,
          a.page_count + b.page_count
        )
      },

      /** 2 step */
      new WindowFunction[SessionAccumulator, Session, String, TimeWindow] {
        override def apply(key_user_id: String,
                           window: TimeWindow,
                           input: Iterable[SessionAccumulator],
                           out: Collector[Session]): Unit = {
          val total_duration = input.map(_.duration).sum
          val total_page_count = input.map(_.page_count).sum
          val session = Session(
            user_id = key_user_id,
            duration = total_duration,
            page_count = total_page_count,
            avg_page_time = total_duration.toDouble / total_page_count
          )
          out.collect(session)
        }
      }

    )

  sessions.print()

  /**
    * PATTERN 2: Enrichment
    *
    * Now we have user sessions.
    * Let's enrich them with information about users from another Kafka topic.
    * We will use Flink's State for saving actual user information between restarts.
    *
    */

  /** Definition of our result class */
  case class SessionWithRegion(session: Session, region: String)

  /** Stream of user updates */
  val userUpdates: DataStream[UserUpdate] = env
    .fromElements(
      UserUpdate(1, "User1", "Russia", "M"),
      UserUpdate(2, "User2", "China", "F"),
      UserUpdate(Long.MaxValue, "User2", "China", "F")
    )
    .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[UserUpdate] {
      override def extractAscendingTimestamp(el: UserUpdate): Long = el.registertime
    })

  /** Enrichment step.
    * We use RichCoFlatMapFunctions for this.
    * "Rich" = "Has access to execution context and can checkpoint it's state"
    * "Co" = "Works for connected streams" */
  val sessionsWithRegion = sessions
    .connect(userUpdates)
    /** keyBy for each of streams. Expressions must return identical values for the same user */
    .keyBy(_.user_id, _.userid)
    .flatMap(new RichCoFlatMapFunction[Session, UserUpdate, SessionWithRegion] {
      private var userRegion: ValueState[String] = _

      /** Init ValueState (our storage) at start */
      override def open(parameters: Configuration): Unit = {
        userRegion = getRuntimeContext.getState(
          new ValueStateDescriptor[String]("user-region", createTypeInformation[String])
        )
      }

      /** Process function for session stream: get region value for this user from Flink State and add to session */
      override def flatMap1(session: Session, out: Collector[SessionWithRegion]): Unit = {
        val region = userRegion.value
        val sessionWithRegion = SessionWithRegion(
          session,
          if (region != null) region else "unknown"
        )
        out.collect(sessionWithRegion)
      }

      /** Process function for user update stream: just update value for this user in the State */
      override def flatMap2(userUpdate: UserUpdate, out: Collector[SessionWithRegion]): Unit =
        userRegion.update(userUpdate.regionid)
    })

  sessionsWithRegion.print()

  env.execute()
}
