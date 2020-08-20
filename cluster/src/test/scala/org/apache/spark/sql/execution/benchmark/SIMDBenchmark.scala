/*
 * Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.execution.benchmark

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{Executors, TimeUnit}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.sys.process._

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import io.snappydata.SnappyFunSuite
import io.snappydata.benchmark.TPCHTableSchema

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator, CodegenContext}
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal, UnsafeRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.benchmark.TAQTest.cores
import org.apache.spark.sql.execution.columnar.encoding.{BitSet, UncompressedDecoder}
import org.apache.spark.sql.internal.CodeGenerationException
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{SnappySession, SparkSession}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.util.Benchmark
import org.apache.spark.util.random.XORShiftRandom

class SIMDBenchmark extends SnappyFunSuite {

  // NativeCalls.getInstance().setEnvironment("TEST_CORES", "1")

  override protected def newSparkConf(addOn: SparkConf => SparkConf = null): SparkConf =
    TAQTest.newSparkConf(addOn).set("snappydata.store.memory-size", "4g")

  private val airlineData = "/export/shared/QA_DATA/airlineParquetData_2007-15"

  ignore("TPCH Q6") {
    val sc = this.sc
    val spark = new SparkSession(sc)
    val snappy = new SnappySession(sc)
    val parquetData = "/export/shared/QA_DATA/TPCH_1GB/parquet_lineitem_1"
    val lineItem = spark.read.parquet(parquetData)
    val lineItem2 = snappy.read.parquet(parquetData)
    lineItem.createOrReplaceTempView("lineitem")
    lineItem2.createOrReplaceTempView("lineitem")

    val q6 =
      """
        select sum(l_extendedprice * l_discount) as revenue
        from lineitem
        where
          l_shipdate >= to_date('1994-01-01') and
          l_shipdate < to_date('1995-01-01') and
          l_discount between 0.05 and 0.07 and
          l_quantity < 24
      """

    val size = lineItem.count().toInt
    spark.sql(q6).show()

    optimizeForLatency(snappy)

    // arrays for handwritten query
    val l_extendedprice = new Array[Double](size)
    val l_discount = new Array[Double](size)
    val l_shipdate = new Array[Int](size)
    val l_quantity = new Array[Double](size)
    val startDate = Cast(Literal("1994-01-01"), DateType).eval(null).asInstanceOf[Int]
    val endDate = Cast(Literal("1995-01-01"), DateType).eval(null).asInstanceOf[Int]
    val numThreads = cores
    val threadSize = size / numThreads
    val executorService = Executors.newFixedThreadPool(numThreads)
    val executor = ExecutionContext.fromExecutorService(executorService)

    // noinspection ScalaStyle
    println(s"Using $numThreads threads")

    val numIters = 20
    val benchmark = new BenchmarkWithCleanup("TPCH Q6", size)

    def time(action: String, f: => Unit): Unit = {
      val start = System.nanoTime().toDouble
      f
      val end = System.nanoTime().toDouble
      // noinspection ScalaStyle
      println(s"Time taken for $action = ${(end - start) / 1000000.0}ms")
    }

    def fillArrays(): Unit = {
      val rows = Utils.sqlInternal(snappy,
        "select l_extendedprice, l_discount, l_shipdate, l_quantity from lineitem")
          .collectInternal()
      var i = 0
      while (i < size) {
        val row = rows.next()
        l_extendedprice(i) = row.getDouble(0)
        l_discount(i) = row.getDouble(1)
        l_shipdate(i) = row.getInt(2)
        l_quantity(i) = row.getDouble(3)
        i += 1
      }
      assert(!rows.hasNext)
    }

    benchmark.addCase("Parquet", numIters) { _ =>
      spark.sql(q6).collect()
    }
    ColumnCacheBenchmark.addCaseWithCleanup(benchmark, "Cache", numIters,
      () => time("cache", lineItem.persist().count()),
      () => lineItem.unpersist(blocking = true)) { _ =>
      spark.sql(q6).collect()
    }
    benchmark.addCase("Snappy Parquet", numIters) { _ =>
      snappy.sql(q6).collect()
    }
    ColumnCacheBenchmark.addCaseWithCleanup(benchmark, "Hand written", numIters,
      () => time("arrays insert", fillArrays()), () => Unit) { _ =>
      def task(id: Int): Double = {
        var i = id * threadSize
        val end = if (id == numThreads - 1) size else i + threadSize
        var revenue = 0.0
        while (i < end) {
          val shipdate = l_shipdate(i)
          if (shipdate >= startDate && shipdate < endDate) {
            val discount = l_discount(i)
            if (discount >= 0.05 && discount <= 0.07 && l_quantity(i) < 24.0) {
              revenue += l_extendedprice(i) * discount
            }
          }
          i += 1
        }
        revenue
      }

      val tasks = (0 until numThreads).map(i => Future(task(i))(executor))
      val revenue = tasks.map(Await.result(_, Duration.Inf)).sum
      assert(revenue > 1.0e+8)
    }
    ColumnCacheBenchmark.addCaseWithCleanup(benchmark, "Snappy", numIters, () => {
      snappy.dropTable("lineitem", ifExists = true)
      val newSchema = TPCHTableSchema.newLineItemSchema(lineItem2.schema)
      snappy.createTable("lineitem", "column", newSchema, Map("buckets" -> "32"))
      time("column insert", lineItem2.write.insertInto("lineitem"))
    }, () => snappy.dropTable("lineitem", ifExists = true)) { i =>
      // scalastyle:off println
      if (i == 0) println(snappy.sql(s"explain codegen $q6").collect().mkString("\n"))
      snappy.sql(q6).collect()
    }

    benchmark.run()

    executorService.shutdown()
    executorService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
  }

  private def optimizeForLatency(snappy: SnappySession): Unit = {
    val tableName = "optimizeForLatency_tmp"
    snappy.sql(s"drop table if exists $tableName")
    snappy.sql(s"create table $tableName (id int) using column options (buckets '16')")
    for (_ <- 0 until 1000) {
      snappy.sql(s"select * from $tableName").collect()
    }
    snappy.sql(s"drop table if exists $tableName")
  }

  ignore("Average no group by") {
    val snappy = new SnappySession(sc)

    val size = 53000000
    val numIters = 20
    val numThreads = cores
    val threadSize = size / numThreads

    val rnd = new XORShiftRandom
    val dataCol = Array.fill(size)(rnd.nextInt(1000))
    val dataColU = Platform.allocateMemory(size << 2)
    Platform.copyMemory(dataCol, Platform.INT_ARRAY_OFFSET, null, dataColU, size << 2)

    val tableName = "tempAvg"
    snappy.sql(s"drop table if exists $tableName")
    snappy.sql(s"create table $tableName (arrDelay int) using column options (buckets '$cores')")
    snappy.read.parquet(airlineData).selectExpr("arrDelay").write.insertInto(tableName)

    optimizeForLatency(snappy)

    snappy.sql(s"select avg(arrDelay) from $tableName").show()
    snappy.sql(s"select count(*) as totalCount from $tableName").show()
    snappy.sql(s"select count(*) as nullCount from $tableName where arrDelay is null").show()

    val delayArr = Utils.sqlInternal(snappy, s"select arrDelay from $tableName").collectInternal()
        .map(r => if (r.isNullAt(0)) Int.MinValue else r.getInt(0)).toArray
    val nulls = new Array[Long](UnsafeRow.calculateBitSetWidthInBytes(delayArr.length) >>> 3)
    var delayArrIndex = -1
    val delayData = delayArr.filter { v =>
      delayArrIndex += 1
      if (v != Int.MinValue) true
      else {
        BitSet.set(nulls, Platform.LONG_ARRAY_OFFSET, delayArrIndex)
        delayArr(delayArrIndex) = 0
        false
      }
    }

    val delayU = Platform.allocateMemory(delayArr.length << 2)
    val delayU2 = Platform.allocateMemory(delayArr.length << 2)
    val nullsU = Platform.allocateMemory(nulls.length << 3)
    Platform.copyMemory(delayData, Platform.INT_ARRAY_OFFSET, null, delayU, delayData.length << 2)
    Platform.copyMemory(delayArr, Platform.INT_ARRAY_OFFSET, null, delayU2, delayArr.length << 2)
    Platform.copyMemory(nulls, Platform.LONG_ARRAY_OFFSET, null, nullsU, nulls.length << 3)

    val airThreadSize = (((delayArr.length / numThreads) + 7) >>> 3) << 3
    var numNulls = 0
    val delayDataStarts = (0 until numThreads).map { i =>
      val prevStart = if (i == 0) 0 else (i - 1) * airThreadSize
      val start = i * airThreadSize
      assert((start % 8) == 0)
      var index = prevStart
      while (index != start) {
        val b = Platform.getByte(null, nullsU + (index >>> 3))
        if (b != 0) numNulls += Integer.bitCount(b & 0xff)
        index += 8
      }
      start - numNulls
    }

    val executorService = Executors.newFixedThreadPool(numThreads)
    val executor = ExecutionContext.fromExecutorService(executorService)
    val benchmark = new Benchmark("SIMD vs no SIMD", size)

    // scalastyle:off println

    println(s"Running with $numThreads threads")

    benchmark.addCase("avg", numIters) { _ =>
      def task(id: Int): Double = {
        var sum = 0.0
        var i = id * threadSize
        val end = i + threadSize
        while (i < end) {
          sum += dataCol(i)
          i += 1
        }
        sum
      }

      val tasks = Array.tabulate(numThreads)(i => Future(task(i))(executor))
      val sums = tasks.map(Await.result(_, Duration.Inf))
      println(s"Avg = ${sums.sum / dataCol.length}")
    }
    benchmark.addCase("avg (nulls, compact data)", numIters) { _ =>
      def task(id: Int): (Double, Int) = {
        var sum = 0.0
        var count = 0
        var i = id * airThreadSize
        val end = if (id == numThreads - 1) delayArr.length else i + airThreadSize
        var addr = delayU + (delayDataStarts(id) << 2)
        while (i < end) {
          val b = Platform.getByte(null, nullsU + (i >>> 3))
          if (b == 0 || (b & (1 << (i & 0x7))) == 0) {
            sum += Platform.getInt(null, addr)
            addr += 4
            count += 1
          }
          i += 1
        }
        sum -> count
      }

      val tasks = Array.tabulate(numThreads)(i => Future(task(i))(executor))
      val avgs = tasks.map(Await.result(_, Duration.Inf))
      println(s"Avg = ${avgs.map(_._1).sum / avgs.map(_._2).sum}")
    }
    benchmark.addCase("avg (with nulls) SIMD off-heap", numIters) { _ =>
      val op = GenerateSIMDJNI.compile("avg_nulls_int_offheap", () => {
        val ctx = new CodegenContext
        val index = ctx.freshName("index")
        val addrVar = ctx.freshName("addr")
        val nullsAddrVar = ctx.freshName("nullsAddr")
        val arrVar = ctx.freshName("arr")
        val nullsVar = ctx.freshName("nulls")
        val offsetVar = ctx.freshName("offset")
        val sizeVar = ctx.freshName("size")
        val vectorSum = ctx.freshName("vecSum")
        val vectorMask = ctx.freshName("vecMask")
        val vectorCount = ctx.freshName("vecCount")
        val result = ctx.freshName("result")
        val jresult = ctx.freshName("jresult")
        val maskConvert = GenerateSIMDJNI.convert32IntsTo64Ints(vectorMask)
        val jniArgs = s"jlong $addrVar, jlong $nullsAddrVar, jint $offsetVar, jint $sizeVar"
        val jniCode =
          s"""
            __m128i* $arrVar = (__m128i*)$addrVar;
            int8_t* $nullsVar = (int8_t*)$nullsAddrVar;
            $arrVar += $offsetVar >> 2;
            __m256d $vectorSum = _mm256_setzero_pd();
            __m256i $vectorCount = _mm256_setzero_si256();
            int $index = $offsetVar;
            int ${index}_end = $index + $sizeVar;
            for (; $index < ${index}_end; $index += 4) {
              // load next 4-bits as mask
              __m128i $vectorMask = _mm_set1_epi32($nullsVar[$index >> 3] >> ($index & 0x4));
              $vectorMask = _mm_and_si128($vectorMask, _mm_set_epi32(0x8, 0x4, 0x2, 0x1));
              $vectorMask = _mm_cmpeq_epi32($vectorMask, _mm_setzero_si128());
              $vectorSum += _mm256_cvtepi32_pd(_mm_and_si128(*$arrVar++, $vectorMask));
              ${maskConvert._1}$vectorCount += ${
            GenerateSIMDJNI.and64(
              "_mm256_set1_epi64x(1LL)", maskConvert._2)
          };
            }
            jdouble $result[2];
            $result[0] = $vectorSum[0] + $vectorSum[1] + $vectorSum[2] + $vectorSum[3];
            $result[1] = $vectorCount[0] + $vectorCount[1] + $vectorCount[2] + $vectorCount[3];
            jdoubleArray $jresult;
            $jresult = (*env)->NewDoubleArray(env, 2);
            (*env)->SetDoubleArrayRegion(env, $jresult, 0, 2, $result);
            return $jresult;
          """
        val source =
          s"""
            public Object generate(Object[] references) {
              return new GeneratedSIMDOperation(references);
            }

            public static native double[] evalNative(long dataAddress, long nullsAddress,
                int offset, int size);

            static final class GeneratedSIMDOperation implements ${classOf[SIMDOperation].getName} {

              private Object[] references;
              ${ctx.declareMutableStates()}

              public GeneratedSIMDOperation(Object[] references) {
                this.references = references;
                System.load((String)references[0]);
                ${ctx.initMutableStates()}
                ${ctx.initPartition()}
              }

              ${ctx.declareAddedFunctions()}

              public long[] eval(long[] addresses, int offset, int size) {
                double[] result = evalNative(addresses[0], addresses[1], offset, size);
                return new long[] { Double.doubleToLongBits(result[0]), (long)result[1] };
              }
           }
          """
        // try to compile, helpful for debug
        val cleanedSource = CodeFormatter.stripOverlappingComments(
          new CodeAndComment(CodeFormatter.stripExtraNewLines(source),
            ctx.getPlaceHolderToComments()))

        CodeGeneration.logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
        (cleanedSource, jniArgs, "jdoubleArray", jniCode)
      })

      def task(id: Int): (Double, Long) = {
        val start = id * airThreadSize
        val size = if (id == numThreads - 1) {
          // floor to 4 for now
          ((delayArr.length - start) >> 2) << 2
        } else airThreadSize
        val result = op.eval(Array(delayU2, nullsU), start, size)
        java.lang.Double.longBitsToDouble(result(0)) -> result(1)
      }

      val tasks = Array.tabulate(numThreads)(i => Future(task(i))(executor))
      val avgs = tasks.map(Await.result(_, Duration.Inf))
      println(s"Avg = ${avgs.map(_._1).sum / avgs.map(_._2).sum}")
    }
    benchmark.addCase("avg (nulls, compact data) SIMD off-heap", numIters) { _ =>
      val op = GenerateSIMDJNI.compile("avg_nulls_compact_int_offheap", () => {
        val ctx = new CodegenContext
        val index = ctx.freshName("index")
        val addrVar = ctx.freshName("addr")
        val nullsAddrVar = ctx.freshName("nullsAddr")
        val arrVar = ctx.freshName("arr")
        val nullsVar = ctx.freshName("nulls")
        val offsetVar = ctx.freshName("offset")
        val endVar = ctx.freshName("end")
        val vectorSum = ctx.freshName("vecSum")
        val nullMask = ctx.freshName("nullMask")
        val nonNullsMask = ctx.freshName("nonNullsMask")
        val validMask = ctx.freshName("validMask")
        val vectorLoad = ctx.freshName("vecLoad")
        val vectorPositions = ctx.freshName("positions")
        val vectorCount = ctx.freshName("vecCount")
        val result = ctx.freshName("result")
        val jresult = ctx.freshName("jresult")
        val maskConvert = GenerateSIMDJNI.convert32IntsTo64Ints(nonNullsMask)
        val jniArgs = s"jlong $addrVar, jlong $nullsAddrVar, jint $offsetVar, jint $endVar"
        val jniCode =
          s"""
            int32_t* $arrVar = (int32_t*)$addrVar;
            uint8_t* $nullsVar = (uint8_t*)$nullsAddrVar;
            __m256d $vectorSum = _mm256_setzero_pd();
            __m256i $vectorCount = _mm256_setzero_si256();
            int $index;
            for ($index = $offsetVar; $index < $endVar; $index += 4) {
              __m128i $vectorLoad;
              __m128i $nonNullsMask;
              __m128i $validMask;

              int ${validMask}_b = $index <= $endVar - 4;
              if (!${validMask}_b) {
                $validMask = _mm_cmplt_epi32(_mm_set_epi32(3, 2, 1, 0),
                    _mm_set1_epi32($endVar - $index));
              }
              // load next 4-bits as mask
              unsigned int $nullMask = $nullsVar[$index >> 3];
              if (${validMask}_b & ($nullMask == 0 || ($nullMask >>= ($index & 0x4)) == 0)) {
                $vectorLoad = _mm_lddqu_si128((__m128i*)$arrVar);
                $nonNullsMask = _mm_set1_epi32(-1);
                $arrVar += 4;
              } else {
                // create bitmask and calculate the positions to be loaded as running totals
                $nonNullsMask = _mm_set1_epi32($nullMask);
                // AND with 8,4,2,1 to get those bit positions respectively in registers
                $nonNullsMask = _mm_and_si128($nonNullsMask, _mm_set_epi32(0x8, 0x4, 0x2, 0x1));
                // compare against zeros to get 0xffffffff for non-null positions and 0 for null
                $nonNullsMask = _mm_cmpeq_epi32($nonNullsMask, _mm_setzero_si128());
                // mask to 1 bit to get ones for non-null positions and zeros for null
                __m128i $vectorPositions = _mm_and_si128($nonNullsMask, _mm_set1_epi32(1));
                // convert to running totals
                // a|b|c|d + 0|a|b|c => a|b+a|c+b|d+c
                $vectorPositions += _mm_slli_si128($vectorPositions, 4);
                // a|b+a|c+b|d+c + 0|0|a|b+a => a|b+a|c+b+a|d+c+b+a
                $vectorPositions += _mm_slli_si128($vectorPositions, 8);
                if (!${validMask}_b) $nonNullsMask = _mm_and_si128($nonNullsMask, $validMask);
                // Now load using above positions and mask out null positions.
                // For example given null bits 0010, the vectorPositions will be 1|2|2|3
                // and vectorMask will be 0|0|-1|0 with the gather+mask operation
                // skipping the third position
                $vectorLoad = _mm_mask_i32gather_epi32(_mm_setzero_si128(), $arrVar - 1,
                    $vectorPositions, $nonNullsMask, 4);
                // update the data cursor with last position added
                $arrVar += _mm_extract_epi32($vectorPositions, 3);
              }
              $nonNullsMask = _mm_and_si128($nonNullsMask,
                  _mm_cmpgt_epi32($vectorLoad, _mm_set1_epi32(2)));
              $vectorSum += _mm256_cvtepi32_pd($vectorLoad);
              ${maskConvert._1}$vectorCount += ${
            GenerateSIMDJNI.and64(
              "_mm256_set1_epi64x(1LL)", maskConvert._2)
          };
            }
            jdouble $result[2];
            $result[0] = $vectorSum[0] + $vectorSum[1] + $vectorSum[2] + $vectorSum[3];
            $result[1] = _mm256_extract_epi64($vectorCount, 0) +
                _mm256_extract_epi64($vectorCount, 1) +
                _mm256_extract_epi64($vectorCount, 2) +
                _mm256_extract_epi64($vectorCount, 3);
            jdoubleArray $jresult;
            $jresult = (*env)->NewDoubleArray(env, 2);
            (*env)->SetDoubleArrayRegion(env, $jresult, 0, 2, $result);
            return $jresult;
          """
        val source =
          s"""
            public Object generate(Object[] references) {
              return new GeneratedSIMDOperation(references);
            }

            public static native double[] evalNative(long dataAddress, long nullsAddress,
                int offset, int end);

            static final class GeneratedSIMDOperation implements ${classOf[SIMDOperation].getName} {

              private Object[] references;
              ${ctx.declareMutableStates()}

              public GeneratedSIMDOperation(Object[] references) {
                this.references = references;
                System.load((String)references[0]);
                ${ctx.initMutableStates()}
                ${ctx.initPartition()}
              }

              ${ctx.declareAddedFunctions()}

              public long[] eval(long[] addresses, int offset, int end) {
                double[] result = evalNative(addresses[0], addresses[1], offset, end);
                return new long[] { Double.doubleToLongBits(result[0]), (long)result[1] };
              }
           }
          """
        // try to compile, helpful for debug
        val cleanedSource = CodeFormatter.stripOverlappingComments(
          new CodeAndComment(CodeFormatter.stripExtraNewLines(source),
            ctx.getPlaceHolderToComments()))

        CodeGeneration.logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
        (cleanedSource, jniArgs, "jdoubleArray", jniCode)
      })

      def task(id: Int): (Double, Long) = {
        val start = id * airThreadSize
        val end = if (id == numThreads - 1) delayArr.length else start + airThreadSize
        val result = op.eval(Array(delayU + (delayDataStarts(id) << 2), nullsU), start, end)
        java.lang.Double.longBitsToDouble(result(0)) -> result(1)
      }

      val tasks = Array.tabulate(numThreads)(i => Future(task(i))(executor))
      val avgs = tasks.map(Await.result(_, Duration.Inf))
      println(s"Count = ${avgs.map(_._2).sum} Avg = ${avgs.map(_._1).sum / avgs.map(_._2).sum}")
    }

    benchmark.addCase("avg Snappy", numIters) { _ =>
      val avg = snappy.sql(s"select avg(arrDelay) from $tableName").collect()(0).getDouble(0)
      println(s"Avg = $avg")
    }
    benchmark.addCase("avg Snappy filter", numIters) { _ =>
      val avg = snappy.sql(s"select avg(arrDelay) from $tableName where arrDelay > 0")
          .collect()(0).getDouble(0)
      println(s"Avg = $avg")
    }
    benchmark.addCase("avg auto SIMD off-heap", numIters) { _ =>
      def task(id: Int): Double = {
        val simdGroupSize = 4
        val sums = new Array[Long](simdGroupSize)
        val decoder = new UncompressedDecoder(null, dataColU, null, null)
        var i = id * threadSize
        val end = i + threadSize
        while (i < end) {
          var gi = 0
          while (gi < simdGroupSize) {
            sums(gi) += decoder.readInt(null, i + gi)
            gi += 1
          }
          i += simdGroupSize
        }
        sums.sum.toDouble
      }

      // val sums = SIMDTest.run(dataColU, size, groupSize)
      val tasks = Array.tabulate(numThreads)(i => Future(task(i))(executor))
      val sums = tasks.map(Await.result(_, Duration.Inf))
      println(s"Avg = ${sums.sum / dataCol.length}")
    }

    benchmark.addCase("avg JNI SIMD off-heap", numIters) { _ =>
      val op = GenerateSIMDJNI.compile("avg_int_offheap", () => {
        val ctx = new CodegenContext
        val addrVar = ctx.freshName("addr")
        val arrVar = ctx.freshName("arr")
        val offsetVar = ctx.freshName("offset")
        val sizeVar = ctx.freshName("size")
        val endArrVar = ctx.freshName("endArr")
        val vectorResult = ctx.freshName("vecResult")
        // val vecLoad = ctx.freshName("vecLoad")
        val jniArgs = s"jlong $addrVar, jint $offsetVar, jint $sizeVar"
        val jniCode =
          s"""
            __m128i* $arrVar = (__m128i*)$addrVar;
            $arrVar += $offsetVar >> 2;
            __m128i* $endArrVar = $arrVar + ($sizeVar >> 2);
            __m256d $vectorResult = _mm256_setzero_pd();
            while ($arrVar < $endArrVar) {
              $vectorResult += _mm256_cvtepi32_pd(_mm_lddqu_si128($arrVar++));
            }
            return $vectorResult[0] + $vectorResult[1] + $vectorResult[2] + $vectorResult[3];
          """
        val source =
          s"""
            public Object generate(Object[] references) {
              return new GeneratedSIMDOperation(references);
            }

            public static native double evalNative(long address, int offset, int size);

            static final class GeneratedSIMDOperation implements ${classOf[SIMDOperation].getName} {

              private Object[] references;
              ${ctx.declareMutableStates()}

              public GeneratedSIMDOperation(Object[] references) {
                this.references = references;
                System.load((String)references[0]);
                ${ctx.initMutableStates()}
                ${ctx.initPartition()}
              }

              ${ctx.declareAddedFunctions()}

              public long[] eval(long[] addresses, int offset, int size) {
                return new long[] { Double.doubleToLongBits(
                    evalNative(addresses[0], offset, size)) };
              }
           }
          """
        // try to compile, helpful for debug
        val cleanedSource = CodeFormatter.stripOverlappingComments(
          new CodeAndComment(CodeFormatter.stripExtraNewLines(source),
            ctx.getPlaceHolderToComments()))

        CodeGeneration.logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
        (cleanedSource, jniArgs, "double", jniCode)
      })

      def task(id: Int): Double =
        java.lang.Double.longBitsToDouble(op.eval(Array(dataColU), id * threadSize, threadSize)(0))

      val tasks = Array.tabulate(numThreads)(i => Future(task(i))(executor))
      val sums = tasks.map(Await.result(_, Duration.Inf))
      println(s"Avg = ${sums.sum / dataCol.length}")
    }
    // scalastyle:on println

    benchmark.run()

    executorService.shutdown()
    executorService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
  }

  ignore("Average Group By") {
    val snappy = new SnappySession(sc)

    val size = 53000000
    val groupSize = 32
    val numIters = 20
    val numThreads = cores
    val threadSize = size / numThreads

    val rnd = new XORShiftRandom
    val groupCol = Array.tabulate(size)(i => (i.toLong * groupSize.toLong / size.toLong).toShort)
    val groupColU = Platform.allocateMemory(size << 1)
    Platform.copyMemory(groupCol, Platform.SHORT_ARRAY_OFFSET, null, groupColU, size << 1)

    val dataCol = Array.fill(size)(rnd.nextInt(1000))
    val dataColU = Platform.allocateMemory(size << 2)
    Platform.copyMemory(dataCol, Platform.DOUBLE_ARRAY_OFFSET, null, dataColU, size << 2)

    val tableName = "tempAvg"
    snappy.sql(s"drop table if exists $tableName")
    snappy.sql(s"create table $tableName (arrDelay int, depDelay int, " +
        s"uniqueCarrier string) using column options (buckets '$cores')")
    snappy.read.parquet(airlineData).selectExpr("arrDelay", "depDelay", "uniqueCarrier")
        .write.insertInto(tableName)

    optimizeForLatency(snappy)

    snappy.sql(s"select avg(arrDelay) from $tableName").show()
    snappy.sql(s"select count(*) from $tableName where arrDelay is null").show()

    val delayArr = Utils.sqlInternal(snappy, s"select arrDelay from $tableName").collectInternal()
        .map(r => if (r.isNullAt(0)) Int.MinValue else r.getInt(0)).toArray
    val nulls = new Array[Long](UnsafeRow.calculateBitSetWidthInBytes(delayArr.length) >>> 3)
    var delayArrIndex = -1
    val delayData = delayArr.filter { v =>
      delayArrIndex += 1
      if (v != Int.MinValue) true
      else {
        BitSet.set(nulls, Platform.LONG_ARRAY_OFFSET, delayArrIndex)
        delayArr(delayArrIndex) = 0
        false
      }
    }

    val delayU = Platform.allocateMemory(delayArr.length << 2)
    val delayU2 = Platform.allocateMemory(delayArr.length << 2)
    val nullsU = Platform.allocateMemory(nulls.length << 3)
    Platform.copyMemory(delayData, Platform.INT_ARRAY_OFFSET, null, delayU, delayData.length << 2)
    Platform.copyMemory(delayArr, Platform.INT_ARRAY_OFFSET, null, delayU2, delayArr.length << 2)
    Platform.copyMemory(nulls, Platform.LONG_ARRAY_OFFSET, null, nullsU, nulls.length << 3)

    val airThreadSize = (((delayArr.length / numThreads) + 7) >>> 3) << 3
    var numNulls = 0
    val delayDataStarts = (0 until numThreads).map { i =>
      val prevStart = if (i == 0) 0 else (i - 1) * airThreadSize
      val start = i * airThreadSize
      assert((start % 8) == 0)
      var index = prevStart
      while (index != start) {
        val b = Platform.getByte(null, nullsU + (index >>> 3))
        if (b != 0) numNulls += Integer.bitCount(b & 0xff)
        index += 8
      }
      start - numNulls
    }

    val executorService = Executors.newFixedThreadPool(numThreads)
    val executor = ExecutionContext.fromExecutorService(executorService)
    val benchmark = new Benchmark("SIMD vs no SIMD", size)

    // scalastyle:off println

    println(s"Running with $numThreads threads")

    benchmark.addCase("avg group by", numIters) { _ =>
      def task(id: Int): (Double, Long) = {
        val sums = new Array[Double](groupSize)
        val counts = new Array[Long](groupSize)
        var i = id * threadSize
        val end = i + threadSize
        while (i < end) {
          val g = groupCol(i)
          sums(g) += dataCol(i)
          counts(g) += 1
          i += 1
        }
        sums.sum -> counts.sum
      }

      val tasks = Array.tabulate(numThreads)(i => Future(task(i))(executor))
      val avgs = tasks.map(Await.result(_, Duration.Inf))
      println(s"Avg = ${avgs.map(_._1).sum / avgs.map(_._2).sum}")
    }

    benchmark.addCase("avg group by 2", numIters) { _ =>
      def initDictionaryArray(): Array[MapType] = {
        val results = new Array[MapType](groupSize)
        var i = 0
        while (i < groupSize) {
          results(i) = new MapType(s"key$i".getBytes(StandardCharsets.UTF_8), 0.0, 0L)
          i += 1
        }
        results
      }

      def task(id: Int): (Double, Long) = {
        val dictionaryArray = initDictionaryArray()
        var i = id * threadSize
        val end = i + threadSize
        while (i < end) {
          var res: MapType = null
          var g = -1
          if (dictionaryArray ne null) {
            g = Platform.getShort(null, groupColU + (i << 1))
            res = dictionaryArray(g)
            if (res eq null) {
              res = new MapType(s"key$i".getBytes(StandardCharsets.UTF_8), 0.0, 0L)
              dictionaryArray(g) = res
            }
          } else {
            g = Platform.getShort(null, groupColU)
            res = dictionaryArray(g)
          }
          var field1 = res.field1
          var field2 = res.field2
          field1 += Platform.getInt(null, dataColU + (i << 2))
          field2 += 1
          res.field1 = field1
          res.field2 = field2
          i += 1
        }
        dictionaryArray.map(t => if (t ne null) t.field1 else 0.0).sum ->
            dictionaryArray.map(t => if (t ne null) t.field2 else 0L).sum
      }

      val tasks = Array.tabulate(numThreads)(i => Future(task(i))(executor))
      val avgs = tasks.map(Await.result(_, Duration.Inf))
      println(s"Avg = ${avgs.map(_._1).sum / avgs.map(_._2).sum}")
    }

    benchmark.addCase("avg grouped group by", numIters) { _ =>
      def task(id: Int): (Double, Long) = {
        val sums = new Array[Double](groupSize)
        val counts = new Array[Long](groupSize)

        var prevGroup = 0
        var prevSum = 0.0
        var prevCount = 0
        var i = id * threadSize
        val end = i + threadSize
        while (i < end) {
          val g = groupCol(i)
          if (g != prevGroup) {
            sums(prevGroup) += prevSum
            counts(prevGroup) += prevCount
            prevGroup = g
            prevSum = 0.0
            prevCount = 0
          }
          prevSum += dataCol(i)
          prevCount += 1
          i += 1
        }
        sums(prevGroup) += prevSum
        counts(prevGroup) += prevCount
        sums.sum -> counts.sum
      }

      val tasks = Array.tabulate(numThreads)(i => Future(task(i))(executor))
      val avgs = tasks.map(Await.result(_, Duration.Inf))
      println(s"Avg = ${avgs.map(_._1).sum / avgs.map(_._2).sum}")
    }

    benchmark.addCase("avg grouped group by off-heap", numIters) { _ =>
      def task(id: Int): (Double, Long) = {
        val sums = new Array[Double](groupSize)
        val counts = new Array[Long](groupSize)
        val groupDecoder = new UncompressedDecoder(null, groupColU, null, null)
        val dataDecoder = new UncompressedDecoder(null, dataColU, null, null)

        var prevGroup = 0
        var prevSum = 0.0
        var prevCount = 0
        var i = id * threadSize
        val end = i + threadSize
        while (i < end) {
          val g = groupDecoder.readShort(null, i)
          if (g != prevGroup) {
            sums(prevGroup) += prevSum
            counts(prevGroup) += prevCount
            prevGroup = g
            prevSum = 0.0
            prevCount = 0
          }
          prevSum += dataDecoder.readInt(null, i)
          prevCount += 1
          i += 1
        }
        sums(prevGroup) += prevSum
        counts(prevGroup) += prevCount
        sums.sum -> counts.sum
      }

      val tasks = Array.tabulate(numThreads)(i => Future(task(i))(executor))
      val avgs = tasks.map(Await.result(_, Duration.Inf))
      println(s"Avg = ${avgs.map(_._1).sum / avgs.map(_._2).sum}")
    }
    benchmark.addCase("Snappy group by", numIters) { _ =>
      val avgs = snappy.sql(s"select avg(arrDelay) from $tableName group by uniqueCarrier")
          .collect().map(_.getDouble(0))
      println(s"Avgs = ${avgs.mkString(", ")}")
    }

    benchmark.addCase("avg group by (nulls, compact data) SIMD off-heap", numIters) { _ =>
      val op = GenerateSIMDJNI.compile("avg_groupBy_nulls_compact_int_offheap", () => {
        val ctx = new CodegenContext
        val index = ctx.freshName("index")
        val addrVar = ctx.freshName("addr")
        val nullsAddrVar = ctx.freshName("nullsAddr")
        val arrVar = ctx.freshName("arr")
        val nullsVar = ctx.freshName("nulls")
        val offsetVar = ctx.freshName("offset")
        val endVar = ctx.freshName("end")
        val vectorSum = ctx.freshName("vecSum")
        val nullMask = ctx.freshName("nullMask")
        val nullsMask = ctx.freshName("nullsMask")
        val validMask = ctx.freshName("validMask")
        val vectorLoad = ctx.freshName("vecLoad")
        val vectorPositions = ctx.freshName("positions")
        val vectorCount = ctx.freshName("vecCount")
        val result = ctx.freshName("result")
        val jresult = ctx.freshName("jresult")
        val maskConvert = GenerateSIMDJNI.convert32IntsTo64Ints(nullsMask)
        val jniArgs = s"jlong $addrVar, jlong $nullsAddrVar, jint $offsetVar, jint $endVar"
        val jniCode =
          s"""
            int32_t* $arrVar = (int32_t*)$addrVar;
            uint8_t* $nullsVar = (uint8_t*)$nullsAddrVar;
            __m256d $vectorSum = _mm256_setzero_pd();
            __m256i $vectorCount = _mm256_setzero_si256();
            int $index;
            for ($index = $offsetVar; $index < $endVar; $index += 4) {
              __m128i $vectorLoad;
              __m128i $nullsMask;
              __m128i $validMask;

              int ${validMask}_b = $index <= $endVar - 4;
              if (!${validMask}_b) {
                $validMask = _mm_cmplt_epi32(_mm_set_epi32(3, 2, 1, 0),
                    _mm_set1_epi32($endVar - $index));
              }
              // load next 4-bits as mask
              unsigned int $nullMask = $nullsVar[$index >> 3];
              if (${validMask}_b & ($nullMask == 0 || ($nullMask >>= ($index & 0x4)) == 0)) {
                $vectorLoad = _mm_lddqu_si128((__m128i*)$arrVar);
                $nullsMask = _mm_set1_epi32(-1);
                $arrVar += 4;
              } else {
                const __m128i ZERO_128 = _mm_setzero_si128();
                // create bitmask and calculate the positions to be loaded as running totals
                $nullsMask = _mm_set1_epi32($nullMask);
                // AND with 8,4,2,1 to get those bit positions respectively in registers
                $nullsMask = _mm_and_si128($nullsMask, _mm_set_epi32(0x8, 0x4, 0x2, 0x1));
                // compare against zeros to get 0xffffffff for non-null positions and 0 for null
                $nullsMask = _mm_cmpeq_epi32($nullsMask, ZERO_128);
                // mask to 1 bit to get ones for non-null positions and zeros for null
                __m128i $vectorPositions = _mm_and_si128($nullsMask, _mm_set1_epi32(1));
                // convert to running totals
                // a|b|c|d + 0|a|b|c => a|b+a|c+b|d+c
                $vectorPositions += _mm_slli_si128($vectorPositions, 4);
                // a|b+a|c+b|d+c + 0|0|a|b+a => a|b+a|c+b+a|d+c+b+a
                $vectorPositions += _mm_slli_si128($vectorPositions, 8);
                if (!${validMask}_b) $nullsMask = _mm_and_si128($nullsMask, $validMask);
                // Now load using above positions and mask out null positions.
                // For example given null bits 0010, the vectorPositions will be 1|2|2|3
                // and vectorMask will be 0|0|-1|0 with the gather+mask operation
                // skipping the second position=2
                $vectorLoad = _mm_mask_i32gather_epi32(ZERO_128, $arrVar - 1,
                    $vectorPositions, $nullsMask, 4);
                // update the data cursor with last position added
                $arrVar += _mm_extract_epi32($vectorPositions, 3);
              }
              $vectorSum += _mm256_cvtepi32_pd($vectorLoad);
              ${maskConvert._1}$vectorCount += ${GenerateSIMDJNI.and64(
            "_mm256_set1_epi64x(1LL)", maskConvert._2)};
            }
            jdouble $result[2];
            $result[0] = $vectorSum[0] + $vectorSum[1] + $vectorSum[2] + $vectorSum[3];
            $result[1] = _mm256_extract_epi64($vectorCount, 0) +
                _mm256_extract_epi64($vectorCount, 1) +
                _mm256_extract_epi64($vectorCount, 2) +
                _mm256_extract_epi64($vectorCount, 3);
            jdoubleArray $jresult;
            $jresult = (*env)->NewDoubleArray(env, 2);
            (*env)->SetDoubleArrayRegion(env, $jresult, 0, 2, $result);
            return $jresult;
          """
        val source =
          s"""
            public Object generate(Object[] references) {
              return new GeneratedSIMDOperation(references);
            }

            public static native double[] evalNative(long dataAddress, long nullsAddress,
                int offset, int end);

            static final class GeneratedSIMDOperation implements ${classOf[SIMDOperation].getName} {

              private Object[] references;
              ${ctx.declareMutableStates()}

              public GeneratedSIMDOperation(Object[] references) {
                this.references = references;
                System.load((String)references[0]);
                ${ctx.initMutableStates()}
                ${ctx.initPartition()}
              }

              ${ctx.declareAddedFunctions()}

              public long[] eval(long[] addresses, int offset, int end) {
                double[] result = evalNative(addresses[0], addresses[1], offset, end);
                return new long[] { Double.doubleToLongBits(result[0]), (long)result[1] };
              }
           }
          """
        // try to compile, helpful for debug
        val cleanedSource = CodeFormatter.stripOverlappingComments(
          new CodeAndComment(CodeFormatter.stripExtraNewLines(source),
            ctx.getPlaceHolderToComments()))

        CodeGeneration.logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
        (cleanedSource, jniArgs, "jdoubleArray", jniCode)
      })

      def task(id: Int): (Double, Long) = {
        val start = id * airThreadSize
        val end = if (id == numThreads - 1) delayArr.length else start + airThreadSize
        val result = op.eval(Array(delayU + (delayDataStarts(id) << 2), nullsU), start, end)
        java.lang.Double.longBitsToDouble(result(0)) -> result(1)
      }

      val tasks = Array.tabulate(numThreads)(i => Future(task(i))(executor))
      val avgs = tasks.map(Await.result(_, Duration.Inf))
      println(s"Count = ${avgs.map(_._2).sum} Avg = ${avgs.map(_._1).sum / avgs.map(_._2).sum}")
    }

    /*
    benchmark.addCase("avg group by JNI SIMD off-heap", numIters) { _ =>
      val op = GenerateSIMDJNI.compile("avg_group_double_offheap", () => {
        val ctx = new CodegenContext
        val addrsVar = ctx.freshName("addrs")
        val groupArrVar = ctx.freshName("groupArr")
        val sizeVar = ctx.freshName("size")
        val groupEndArrVar = ctx.freshName("groupEndArr")
        val arrVar = ctx.freshName("arr")
        val sumsVar = ctx.freshName("sums")
        val countsVar = ctx.freshName("counts")
        val vectorSums = ctx.freshName("vecSums")
        val vectorCounts = ctx.freshName("vecCounts")
        val vector1Const = ctx.freshName("vec1Const")
        val vectorLoad = ctx.freshName("vecLoad")
        val vectorGroupIndexes = ctx.freshName("vecGroupIndexes")
        val vectorGroupIndexes0 = vectorGroupIndexes.concat("_0")
        val jniArgs = s"jlongArray ${addrsVar}J, jint $sizeVar"
        val jniCode =
          s"""
            int64_t* $addrsVar = (int64_t*)${addrsVar}J;
            int32_t* $groupArrVar = (int32_t*)$addrsVar[0];
            double* $arrVar = (double*)$addrsVar[1];
            double* $sumsVar = (double*)$addrsVar[2];
            int32_t* $countsVar = (int32_t*)$addrsVar[3];
            int32_t* $groupEndArrVar = $groupArrVar + (int)$sizeVar;
            const __m256i $vector1Const = _mm256_set1_epi32(1);
            for (; $groupArrVar < $groupEndArrVar; $groupArrVar += 8,
                   $arrVar += 4, $sumsVar += 4, $countsVar += 8) {
              __m256i $vectorGroupIndexes = _mm256_load_si256((__m256i*)$groupArrVar);
              // gather counts, add 1 and scatter
              __m256i $vectorCounts = _mm256_i32gather_epi32($countsVar, $vectorGroupIndexes, 1);
              $vectorCounts = _mm256_add_epi32($vectorCounts, $vector1Const);
              _mm256_i32scatter_epi32($countsVar, $vectorGroupIndexes, $vectorCounts, 1);

              // load data, gather sums, add, and scatter
              __m256d $vectorLoad = _mm256_load_pd($arrVar);
              __m128i $vectorGroupIndexes0 = _mm256_extracti128_si256($vectorGroupIndexes, 0);
              __m256d $vectorSums = _mm256_i32gather_pd($sumsVar, $vectorGroupIndexes0, 1);
              $vectorSums = _mm256_add_pd($vectorSums, $vectorLoad);
              _mm256_i32scatter_pd($sumsVar, $vectorGroupIndexes0, $vectorSums, 1);

              $arrVar += 4;
              $sumsVar += 4;
              $vectorLoad = _mm256_load_pd($arrVar);
              $vectorGroupIndexes0 = _mm256_extracti128_si256($vectorGroupIndexes, 1);
              $vectorSums = _mm256_i32gather_pd($sumsVar, $vectorGroupIndexes0, 1);
              $vectorSums = _mm256_add_pd($vectorSums, $vectorLoad);
              _mm256_i32scatter_pd($sumsVar, $vectorGroupIndexes0, $vectorSums, 1);
            }
          """
        val source =
          s"""
            public Object generate(Object[] references) {
              return new GeneratedSIMDOperation(references);
            }

            public static native void evalDoubleNative(long[] addresses, int size);

            static final class GeneratedSIMDOperation implements ${classOf[SIMDOperation].getName} {

              private Object[] references;
              ${ctx.declareMutableStates()}

              public GeneratedSIMDOperation(Object[] references) {
                this.references = references;
                System.load((String)references[0]);
                ${ctx.initMutableStates()}
                ${ctx.initPartition()}
              }

              ${ctx.declareAddedFunctions()}

              public double evalDouble(long[] addresses, int size) {
                evalDoubleNative(addresses, size);
                return 0.0; // dummy
              }
           }
          """
        // try to compile, helpful for debug
        val cleanedSource = CodeFormatter.stripOverlappingComments(
          new CodeAndComment(CodeFormatter.stripExtraNewLines(source),
            ctx.getPlaceHolderToComments()))

        CodeGeneration.logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
        (cleanedSource, jniArgs, "void", jniCode)
      })
      val sumsDirect = ByteBuffer.allocateDirect(groupSize << 3)
      val sumsU = UnsafeHolder.getDirectBufferAddress(sumsDirect)
      val countsDirect = ByteBuffer.allocateDirect(groupSize << 2)
      val countsU = UnsafeHolder.getDirectBufferAddress(countsDirect)
      op.evalDouble(Array(groupColU, dataColU, sumsU, countsU), groupCol.length)
      val sums = new Array[Double](groupSize)
      val counts = new Array[Int](groupSize)
      Platform.copyMemory(null, sumsU, sums, Platform.DOUBLE_ARRAY_OFFSET, sumsDirect.capacity())
      Platform.copyMemory(null, countsU, counts, Platform.DOUBLE_ARRAY_OFFSET,
        countsDirect.capacity())
      println(s"Avg = ${sums.sum / counts.sum}")
    }
    */
    // scalastyle:on println

    benchmark.run()

    executorService.shutdown()
    executorService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
  }
}

object GenerateSIMDJNI {

  def findExecutablePath(execName: String): Option[Path] = {
    for (path <- System.getenv("PATH").split(java.io.File.pathSeparator)) {
      val execPath = Paths.get(path, execName)
      if (Files.isRegularFile(execPath) && Files.isExecutable(execPath)) {
        return Some(execPath.toAbsolutePath)
      }
    }
    None
  }

  val compiler: String = {
    // try clang first and then gcc
    findExecutablePath("clang").orElse(findExecutablePath("gcc")).map(_.toString).getOrElse(
      throw new IllegalStateException("SIMD support requires either CLANG or GCC in PATH"))
  }

  def and64(var1Name: String, var2Name: String): String = {
    if (GenerateSIMDJNI.hasAVX2) {
      s"_mm256_and_si256($var1Name, $var2Name)"
    } else {
      s"_mm256_castpd_si256(_mm256_and_pd(_mm256_castsi256_pd($var1Name), " +
          s"_mm256_castsi256_pd($var2Name)))"
    }
  }

  def convert32IntsTo64Ints(varName: String): (String, String) = {
    if (GenerateSIMDJNI.hasAVX2) {
      "" -> s"_mm256_cvtepi32_epi64($varName)"
    } else {
      s"""
        __m128i ${varName}_0 = _mm_cvtepi32_epi64($varName);
        __m128i ${varName}_1 = _mm_cvtepi32_epi64(_mm_castps_si128(_mm_permute_ps(
            _mm_castsi128_ps($varName), 0x0E)));
      """ -> s"_mm256_insertf128_si256(_mm256_castsi128_si256(${varName}_0), ${varName}_1, 1)"
    }
  }

  lazy val hasAVX2: Boolean = {
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    val getFlags = Seq("sh", "-c", s"$compiler -dM -E -march=native - < /dev/null")
    val status = getFlags ! ProcessLogger(stdout append _, stderr append _)
    if (status != 0) {
      throw new IllegalStateException("Failed to determine CPU AVX extensions.\n" +
          s"Output: $stdout.\nError: $stderr")
    }
    val flags = stdout.toString()
    if (flags.contains("AVX2")) true
    else if (flags.contains("AVX")) false
    else {
      throw new IllegalStateException(s"SIMD support requires AVX extensions.\n" +
          s"Output: $stdout.\nError: $stderr")
    }
  }

  private val jniCache: LoadingCache[JNIKey, SIMDOperation] =
    CacheBuilder.newBuilder().maximumSize(1000).build(
      new CacheLoader[JNIKey, SIMDOperation]() {
        // scalastyle:off println
        override def load(key: JNIKey): SIMDOperation = {
          val startTime = System.nanoTime()
          val (code, jniArgs, jniReturn, jniCode) = key.code()
          // first generate the java class having the native JNI method
          val result = CodeGenerator.compile(code)._1
          // use the generated class name to generate the JNI method
          val filePath = Files.createTempFile(key.key, ".c")
          val fileName = filePath.toString
          val libName = fileName.replace(".c", ".so")
          val jniMethod = result.getClass.getName.replace('.', '_').concat("_evalNative")
          val jniFileCode =
            s"""
               |#include <stdint.h>
               |#include <jni.h>
               |#include <stdio.h>
               |#include <immintrin.h>
               |
             |inline int bitCount4(int i) {
               |  i = i - ((i >> 1) & 0x5);
               |  return (i & 0x3) + ((i >> 2) & 0x3);
               |}
               |
             |JNIEXPORT $jniReturn JNICALL Java_$jniMethod(JNIEnv *env, jobject obj, $jniArgs) {
               |  $jniCode
               |}
          """.stripMargin
          // compile and link the JNI library into the JVM
          new java.io.PrintWriter(fileName) {
            write(jniFileCode)
            close()
          }
          val stdout = new StringBuilder
          val stderr = new StringBuilder
          val javaHome = System.getenv("JAVA_HOME")
          val compile = s"$compiler -shared -O3 -fPIC -DPIC -march=native " +
              s"-I$javaHome/include -I$javaHome/include/linux $fileName -o $libName"
          val status = compile ! ProcessLogger(stdout append _, stderr append _)
          println(stdout)
          if (stderr.nonEmpty) {
            println(s"GCC stderr: $stderr")
          }
          if (status != 0) {
            throw new CodeGenerationException(s"GCC failed compile: $stderr")
          }
          // Files.delete(filePath)
          new java.io.File(libName).deleteOnExit()

          val endTime = System.nanoTime()
          val timeMs: Double = (endTime - startTime).toDouble / 1000000
          println(s"Code with JNI generated in $timeMs ms")
          result.generate(Array(libName)).asInstanceOf[SIMDOperation]
        }
      })
  // scalastyle:on println

  def compile(key: String, code: () => (CodeAndComment, String, String, String)): SIMDOperation = {
    jniCache.get(new JNIKey(key, code))
  }
}

final class JNIKey(val key: String, val code: () => (CodeAndComment, String, String, String)) {

  override def hashCode(): Int = key.hashCode

  override def equals(obj: Any): Boolean = obj match {
    case o: JNIKey => (this eq o) || key == o.key
    case _ => false
  }
}

trait SIMDOperation {
  def eval(offsets: Array[Long], offset: Int, size: Int): Array[Long]
}

final class MapType(val keyBytes: Array[Byte], var field1: Double, var field2: Long) {

  override val hashCode: Int =
    Murmur3_x86_32.hashUnsafeBytes(keyBytes, Platform.BYTE_ARRAY_OFFSET, keyBytes.length, 42)

  override def equals(obj: Any): Boolean = obj match {
    case o: MapType =>
      keyBytes.length == o.keyBytes.length && ByteArrayMethods.arrayEquals(keyBytes,
        Platform.BYTE_ARRAY_OFFSET, o.keyBytes, Platform.BYTE_ARRAY_OFFSET, o.keyBytes.length)
    case _ => false
  }
}
