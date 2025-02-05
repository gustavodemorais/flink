/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.testutils.EachCallbackWrapper
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.legacy.api.Types
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.table.utils.LegacyRowExtension
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.TestTemplate
import org.junit.jupiter.api.extension.{ExtendWith, RegisterExtension}

import scala.collection.JavaConversions._

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class UnnestITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @RegisterExtension private val _: EachCallbackWrapper[LegacyRowExtension] =
    new EachCallbackWrapper[LegacyRowExtension](new LegacyRowExtension)

  @TestTemplate
  def testUnnestPrimitiveArrayFromTable(): Unit = {
    val data = List(
      (1, Array(12, 45), Array(Array(12, 45))),
      (2, Array(41, 5), Array(Array(18), Array(87))),
      (3, Array(18, 42), Array(Array(1), Array(45)))
    )
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, b, s FROM T, UNNEST(T.b) AS A (s)"
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "1,[12, 45],12",
      "1,[12, 45],45",
      "2,[41, 5],41",
      "2,[41, 5],5",
      "3,[18, 42],18",
      "3,[18, 42],42")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestArrayOfArrayFromTable(): Unit = {
    val data = List(
      (1, Array(12, 45), Array(Array(12, 45))),
      (2, Array(41, 5), Array(Array(18), Array(87))),
      (3, Array(18, 42), Array(Array(1), Array(45)))
    )
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, s FROM T, UNNEST(T.c) AS A (s)"
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,[12, 45]", "2,[18]", "2,[87]", "3,[1]", "3,[45]")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestObjectArrayFromTableWithFilter(): Unit = {
    val data = List(
      (1, Array((12, "45.6"), (12, "45.612"))),
      (2, Array((13, "41.6"), (14, "45.2136"))),
      (3, Array((18, "42.6")))
    )
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, b, s, t FROM T, UNNEST(T.b) AS A (s, t) WHERE s > 13"
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,[13,41.6, 14,45.2136],14,45.2136", "3,[18,42.6],18,42.6")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestMultiSetFromCollectResult(): Unit = {
    val data = List(
      (1, 1, (12, "45.6")),
      (2, 2, (12, "45.612")),
      (3, 2, (13, "41.6")),
      (4, 3, (14, "45.2136")),
      (5, 3, (18, "42.6")))
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sqlQuery =
      """
        |WITH T1 AS (SELECT b, COLLECT(c) as `set` FROM T GROUP BY b)
        |SELECT b, id, point FROM T1, UNNEST(T1.`set`) AS A(id, point) WHERE b < 3
      """.stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    val sink = new TestingRetractSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,12,45.6", "2,12,45.612", "2,13,41.6")
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testLeftUnnestMultiSetFromCollectResult(): Unit = {
    val data = List(
      (1, "1", "Hello"),
      (1, "2", "Hello2"),
      (2, "2", "Hello"),
      (3, null.asInstanceOf[String], "Hello"),
      (4, "4", "Hello"),
      (5, "5", "Hello"),
      (5, null.asInstanceOf[String], "Hello"),
      (6, "6", "Hello"),
      (7, "7", "Hello World"),
      (7, "8", "Hello World")
    )

    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sqlQuery =
      """
        |WITH T1 AS (SELECT a, COLLECT(b) as `set` FROM T GROUP BY a)
        |SELECT a, s FROM T1 LEFT JOIN UNNEST(T1.`set`) AS A(s) ON TRUE WHERE a < 5
      """.stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    val sink = new TestingRetractSink
    result.addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1",
      "1,2",
      "2,2",
      "3,null",
      "4,4"
    )
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testTumbleWindowAggregateWithCollectUnnest(): Unit = {
    val data = TestData.tupleData3.map { case (i, l, s) => (l, i, s) }
    val stream = failingDataSource(data)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Long, Int, String)](0L))
    val t = stream.toTable(tEnv, 'b, 'a, 'c, 'rowtime.rowtime)
    tEnv.createTemporaryView("T", t)

    val sqlQuery =
      """
        |WITH T1 AS (SELECT b, COLLECT(b) as `set`
        |    FROM T
        |    GROUP BY b, TUMBLE(rowtime, INTERVAL '3' SECOND)
        |)
        |SELECT b, s FROM T1, UNNEST(T1.`set`) AS A(s) where b < 3
      """.stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    val sink = new TestingRetractSink
    result.addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1",
      "2,2",
      "2,2"
    )
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testCrossWithUnnest(): Unit = {
    val data = List(
      (1, 1L, Array("Hi", "w")),
      (2, 2L, Array("Hello", "k")),
      (3, 2L, Array("Hello world", "x"))
    )

    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, s FROM T, UNNEST(T.c) as A (s)"
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,Hi", "1,w", "2,Hello", "2,k", "3,Hello world", "3,x")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testCrossWithUnnestForMap(): Unit = {
    val data = List(
      Row.of(
        Int.box(1),
        Long.box(11L), {
          val map = new java.util.HashMap[String, String]()
          map.put("a", "10")
          map.put("b", "11")
          map
        }),
      Row.of(
        Int.box(2),
        Long.box(22L), {
          val map = new java.util.HashMap[String, String]()
          map.put("c", "20")
          map
        }),
      Row.of(
        Int.box(3),
        Long.box(33L), {
          val map = new java.util.HashMap[String, String]()
          map.put("d", "30")
          map.put("e", "31")
          map
        })
    )

    implicit val typeInfo = Types.ROW(
      Array("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.MAP(Types.STRING, Types.STRING))
    )
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, b, v FROM T CROSS JOIN UNNEST(c) as f (k, v)"
    val result = tEnv.sqlQuery(sqlQuery)

    TestSinkUtil.addValuesSink(
      tEnv,
      "MySink",
      List("a", "b", "v"),
      List(DataTypes.INT, DataTypes.BIGINT, DataTypes.STRING),
      ChangelogMode.all()
    )
    result.executeInsert("MySink").await()

    val expected =
      List("1,11,10", "1,11,11", "2,22,20", "3,33,30", "3,33,31")
    assertThat(
      TestValuesTableFactory
        .getResultsAsStrings("MySink")
        .sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testJoinWithUnnestOfTuple(): Unit = {
    val data = List(
      (1, Array((12, "45.6"), (2, "45.612"))),
      (2, Array((13, "41.6"), (1, "45.2136"))),
      (3, Array((18, "42.6")))
    )

    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, b, x, y " +
      "FROM " +
      "  (SELECT a, b FROM T WHERE a < 3) as tf, " +
      "  UNNEST(tf.b) as A (x, y) " +
      "WHERE x > a"
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "1,[12,45.6, 2,45.612],12,45.6",
      "1,[12,45.6, 2,45.612],2,45.612",
      "2,[13,41.6, 1,45.2136],13,41.6")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestObjectArrayWithoutAlias(): Unit = {
    val data = List(
      (1, Array((12, "45.6"), (12, "45.612"))),
      (2, Array((13, "41.6"), (14, "45.2136"))),
      (3, Array((18, "42.6")))
    )
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, b, A._1, A._2 FROM T, UNNEST(T.b) AS A where A._1 > 13"
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,[13,41.6, 14,45.2136],14,45.2136", "3,[18,42.6],18,42.6")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestWithNestedFilter(): Unit = {
    val data = List(
      (1, Array((12, "45.6"), (12, "45.612"))),
      (2, Array((13, "41.6"), (14, "45.2136"))),
      (3, Array((18, "42.6")))
    )
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("MyTable", t)

    val sqlQuery =
      """
        |SELECT * FROM (
        |   SELECT a, b1, b2 FROM
        |       (SELECT a, b FROM MyTable) T
        |       CROSS JOIN
        |       UNNEST(T.b) as S(b1, b2)
        |       WHERE S.b1 >= 12
        |   ) tmp
        |WHERE b2 <> '42.6'
    """.stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,12,45.612", "1,12,45.6", "2,13,41.6", "2,14,45.2136")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestWithValuesStream(): Unit = {
    val sqlQuery = "SELECT * FROM UNNEST(ARRAY[1,2,3])"
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1", "2", "3")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestWithValuesStream2(): Unit = {
    val sqlQuery = "SELECT * FROM (VALUES('a')) CROSS JOIN UNNEST(ARRAY[1, 2, 3])"
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("a,1", "a,2", "a,3")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestWithOrdinalityWithValuesStream(): Unit = {
    val sqlQuery = "SELECT * FROM (VALUES('a')) CROSS JOIN UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY"
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("a,1,1", "a,2,2", "a,3,3")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestArrayWithOrdinality(): Unit = {
    val data = List(
      (1, Array(12, 45)),
      (2, Array(41, 5)),
      (3, Array(18, 42))
    )
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = """
                     |SELECT a, number, ordinality 
                     |FROM T CROSS JOIN UNNEST(b) WITH ORDINALITY AS t(number, ordinality)
                     |""".stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,12,1", "1,45,2", "2,41,1", "2,5,2", "3,18,1", "3,42,2")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestArrayOfArrayWithOrdinality(): Unit = {
    val data = List(
      (1, Array(Array(1, 2), Array(3))),
      (2, Array(Array(4, 5), Array(6, 7))),
      (3, Array(Array(8)))
    )
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'id, 'nested_array)
    tEnv.createTemporaryView("T", t)

    val sqlQuery =
      """
        |SELECT id, array_val, array_pos, `element`, element_pos
        |FROM T
        |CROSS JOIN UNNEST(nested_array) WITH ORDINALITY AS A(array_val, array_pos)
        |CROSS JOIN UNNEST(array_val) WITH ORDINALITY AS B(`element`, element_pos)
        |""".stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "1,[1, 2],1,1,1",
      "1,[1, 2],1,2,2",
      "1,[3],2,3,1",
      "2,[4, 5],1,4,1",
      "2,[4, 5],1,5,2",
      "2,[6, 7],2,6,1",
      "2,[6, 7],2,7,2",
      "3,[8],1,8,1")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestMultisetWithOrdinality(): Unit = {
    val data = List(
      (1, 1, "Hi"),
      (1, 2, "Hello"),
      (2, 2, "World"),
      (3, 3, "Hello world")
    )
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sqlQuery =
      """
        |WITH T1 AS (SELECT a, COLLECT(c) as words FROM T GROUP BY a)
        |SELECT a, word, pos
        |FROM T1 CROSS JOIN UNNEST(words) WITH ORDINALITY AS A(word, pos)
        |""".stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    val sink = new TestingRetractSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "1,Hi,1",
      "1,Hello,2",
      "2,World,1",
      "3,Hello world,1"
    )
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestMapWithOrdinality(): Unit = {
    val data = List(
      Row.of(
        Int.box(1), {
          val map = new java.util.HashMap[String, String]()
          map.put("a", "10")
          map.put("b", "11")
          map
        }),
      Row.of(
        Int.box(2), {
          val map = new java.util.HashMap[String, String]()
          map.put("c", "20")
          map.put("d", "21")
          map
        })
    )

    implicit val typeInfo = Types.ROW(
      Array("id", "map_data"),
      Array[TypeInformation[_]](Types.INT, Types.MAP(Types.STRING, Types.STRING))
    )
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'id, 'map_data)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = """
                     |SELECT id, k, v, pos
                     |FROM T CROSS JOIN UNNEST(map_data) WITH ORDINALITY AS f(k, v, pos)
                     |""".stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toDataStream

    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val resultsWithoutordinality = assertAndRemoveOrdinality(sink.getAppendResults, 2)
    val expected = List("1,a,10", "1,b,11", "2,c,20", "2,d,21")

    assertThat(resultsWithoutordinality.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestWithOrdinalityForChainOfArraysAndMaps(): Unit = {
    val data = List(
      Row.of(
        Int.box(1),
        Array("a", "b"), {
          val map = new java.util.HashMap[String, String]()
          map.put("x", "10")
          map.put("y", "20")
          map
        }),
      Row.of(
        Int.box(2),
        Array("c", "d"), {
          val map = new java.util.HashMap[String, String]()
          map.put("z", "30")
          map.put("w", "40")
          map
        })
    )

    implicit val typeInfo = Types.ROW(
      Array("id", "array_data", "map_data"),
      Array[TypeInformation[_]](Types.INT, Types.OBJECT_ARRAY(Types.STRING), Types.MAP(Types.STRING, Types.STRING))
    )
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'id, 'array_data, 'map_data)
    tEnv.createTemporaryView("T", t)

    val sqlQuery =
      """
        |SELECT id, array_val, array_pos, map_key, map_val, map_pos
        |FROM T
        |CROSS JOIN UNNEST(array_data) WITH ORDINALITY AS A(array_val, array_pos)
        |CROSS JOIN UNNEST(map_data) WITH ORDINALITY AS B(map_key, map_val, map_pos)
        |""".stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toDataStream

    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val resultsWithoutOrdinality = assertAndRemoveOrdinality(sink.getAppendResults, 2)
    val expected = List(
      "1,a,1,x,10", "1,a,1,y,20", "1,b,2,x,10", "1,b,2,y,20",
      "2,c,1,z,30", "2,c,1,w,40", "2,d,2,z,30", "2,d,2,w,40"
    )

    assertThat(resultsWithoutOrdinality.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestWithOrdinalityForEmptyArray(): Unit = {
  val data = List(
    (1, Array[Int]())
  )
  val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b)
  tEnv.createTemporaryView("T", t)

  val sqlQuery = """
    |SELECT a, number, ordinality
    |FROM T CROSS JOIN UNNEST(b) WITH ORDINALITY AS t(number, ordinality)
    |""".stripMargin
  val result = tEnv.sqlQuery(sqlQuery).toDataStream
  val sink = new TestingAppendSink
  result.addSink(sink)
  env.execute()

  val expected = List()
  assertThat(sink.getAppendResults.sorted).isEqualTo(expected)
}

  @TestTemplate
  def testUnnestWithOrdinalityForMapWithNullValues(): Unit = {
    val data = List(
      Row.of(
        Int.box(1), {
          val map = new java.util.HashMap[String, String]()
          map.put("a", "10")
          map.put("b", null)
          map
        }),
      Row.of(
        Int.box(2), {
          val map = new java.util.HashMap[String, String]()
          map.put("c", "20")
          map.put("d", null)
          map
        })
    )

    implicit val typeInfo = Types.ROW(
      Array("id", "map_data"),
      Array[TypeInformation[_]](Types.INT, Types.MAP(Types.STRING, Types.STRING))
    )
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'id, 'map_data)
    tEnv.createTemporaryView("T", t)

    val sqlQuery =
      """
        |SELECT id, k, v, pos
        |FROM T CROSS JOIN UNNEST(map_data) WITH ORDINALITY AS f(k, v, pos)
        |""".stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toDataStream

    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val resultsWithoutordinality = assertAndRemoveOrdinality(sink.getAppendResults, 2)
    val expected = List("1,a,10", "1,b,null", "2,c,20", "2,d,null")
    assertThat(resultsWithoutordinality.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUnnestArrayWithMixedTypesAndOrdinality(): Unit = {
    val data = List(
      (1, Array(10, "20")),
      (2, Array(30, "40"))
    )
    val t = StreamingEnvUtil.fromCollection(env, data).toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("T", t)

    val sqlQuery =
      """
        |SELECT a, number, ordinality
        |FROM T CROSS JOIN UNNEST(b) WITH ORDINALITY AS m(number, ordinality)
        |""".stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,10,1", "1,20,2", "2,30,1", "2,40,2")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  /* Utility for maps to assert that ordinality is within range and remove it from output.
  * Necessary since maps are not ordered */
  def assertAndRemoveOrdinality(results: List[String], maxOrdinality: Int): List[String] = {
    results.foreach {
      result =>
        val columns = result.split(",")
        val ordinality = columns.last.toInt
        assert(ordinality >= 1 && ordinality <= maxOrdinality, s"Ordinality $ordinality out of range")
    }

    results.map(_.split(",").dropRight(1).mkString(","))
  }
}
