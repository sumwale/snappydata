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

package org.apache.spark.sql.execution.oplog.impl

import java.math.{BigDecimal => JDecimal}
import java.nio.ByteBuffer
import java.sql.{Timestamp, Types}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.internal.cache.DiskEntry.Helper
import com.gemstone.gemfire.internal.cache._
import com.gemstone.gemfire.internal.offheap.ByteSource
import com.gemstone.gemfire.internal.shared.FetchRequest
import com.pivotal.gemfirexd.internal.catalog.UUID
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store._
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.{ColumnDescriptor, ColumnDescriptorList}
import com.pivotal.gemfirexd.internal.iapi.types._
import io.snappydata.recovery.RecoveryService

import org.apache.spark.serializer.StructTypeSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, MapData, SerializedArray, SerializedMap, SerializedRow, TypeSpecializedGetters}
import org.apache.spark.sql.execution.RDDKryo
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDecoder, ColumnDeleteDecoder, ColumnDeltaDecoder, ColumnEncoding, ColumnStatsSchema, UpdatedColumnDecoder}
import org.apache.spark.sql.execution.columnar.impl.{ColumnDelta, ColumnFormatEntry, ColumnFormatKey, ColumnFormatValue}
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.{DataType, _}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.{Partition, SparkEnv, TaskContext}

class OpLogRdd(
    @transient private val session: SparkSession,
    private var fullTableName: String,
    private var dbName: String,
    private var tableName: String,
    private var internalFQTN: String,
    private var schema: StructType,
    private var isColumnTable: Boolean,
    private var tableSchemaMap: mutable.Map[String, StructType],
    private var bucketHostMap: mutable.Map[Int, String])
    extends RDDKryo[InternalRow](session.sparkContext, Nil) with KryoSerializable {

  private lazy val tableSchemas: Array[StructType] = {
    val schemas = new Array[StructType](tableSchemaMap.size)
    for ((key, schema) <- tableSchemaMap) {
      val version = key.substring(0, key.indexOf('#')).toInt
      schemas(version) = schema
    }
    schemas
  }

  private var rowFormatter: RowFormatter = _
  private var allRowFormatters: Array[RowFormatter] = Array.empty

  /**
   * Method gets DataValueDescritor type from given StructField
   *
   * @param field StructField of a column
   * @return DataTypeDescriptor
   */
  def getDVDType(field: StructField): DataTypeDescriptor = {
    val dataType = field.dataType
    val isNullable = field.nullable
    val metadata = field.metadata

    val dataTypeDescriptor = dataType match {
      case LongType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT, isNullable)
      case IntegerType =>
        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER, isNullable)
      case BooleanType =>
        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN, isNullable)
      case ByteType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT, isNullable)
      case FloatType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.REAL, isNullable)
      case BinaryType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BLOB, isNullable)
      case DoubleType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE, isNullable)
      case ShortType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT, isNullable)
      case TimestampType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP,
        isNullable)
      case DateType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DATE, isNullable)
      case d: DecimalType =>
        val precision = d.precision
        val scale = d.scale
        new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.DECIMAL),
          precision, scale, isNullable, precision)
      case StringType =>
        if (metadata.contains("base")) {
          metadata.getString("base") match {
            case "STRING" | "CLOB" =>
              DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CLOB, isNullable)
            case "VARCHAR" =>
              DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, isNullable,
                metadata.getLong("size").toInt)
            case "CHAR" =>
              DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, isNullable,
                metadata.getLong("size").toInt)
          }
        } else {
          // when - create table using column as select ..- is used,
          // it create string column with no base information in metadata
          DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CLOB, isNullable)
        }
      case _: ArrayType | _: MapType | _: StructType =>
        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BLOB, isNullable)
      case _ => new DataTypeDescriptor(TypeId.CHAR_ID, isNullable)
    }
    logDebug(s"Field: $field ===> DataTypeDescriptor $dataTypeDescriptor")
    dataTypeDescriptor
  }

  /**
   * Method creates and returns a RowFormatter from schema
   *
   * @return RowFormatter
   */
  def getRowFormatter(versionNum: Int, schemaStruct: StructType): RowFormatter = {
    if (versionNum == 0) {
      if (rowFormatter ne null) return rowFormatter
    } else if (versionNum < allRowFormatters.length) {
      val rf = allRowFormatters(versionNum)
      if (rf ne null) return rf
    }
    val cdl = new ColumnDescriptorList()
    schemaStruct.toList.foreach(field => {
      val cd = new ColumnDescriptor(
        field.name,
        schemaStruct.fieldIndex(field.name) + 1,
        getDVDType(field),
        // getDVDType(field.dataType),
        null,
        null,
        null.asInstanceOf[UUID],
        null.asInstanceOf[UUID],
        0L,
        0L,
        0L,
        false
      )
      cdl.add(null, cd)
    })
    if (isColumnTable) {
      cdl.add(null, new ColumnDescriptor(StoreUtils.ROWID_COLUMN_NAME, schemaStruct.size + 1,
        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT, false),
        null,
        null,
        null.asInstanceOf[UUID],
        null.asInstanceOf[UUID],
        0L,
        0L,
        0L,
        false))
    }
    val newRowFormatter = new RowFormatter(cdl, dbName, tableName, versionNum, null, false)
    if (versionNum == 0) rowFormatter = newRowFormatter
    if (versionNum >= allRowFormatters.length) {
      allRowFormatters = java.util.Arrays.copyOf(allRowFormatters, versionNum + 1)
    }
    allRowFormatters(versionNum) = newRowFormatter
    newRowFormatter
  }

  private def getRow(bytes: Array[Byte], byteArrays: Array[Array[Byte]]): InternalRow = {
    val versionNum = RowFormatter.readVersion(bytes)
    val schemaOfVersion = tableSchemas(versionNum)
    val rowFormatter = getRowFormatter(versionNum, schemaOfVersion)
    val numColumns = schema.size
    val dvdArr = new Array[DataValueDescriptor](numColumns)

    if (byteArrays ne null) {
      var i = 0
      while (i < numColumns) {
        dvdArr(i) = rowFormatter.getColumn(i + 1, byteArrays)
        i += 1
      }
    } else {
      var i = 0
      while (i < numColumns) {
        dvdArr(i) = rowFormatter.getColumn(i + 1, bytes)
        i += 1
      }
    }
    new DVDToInternalRow(dvdArr)
  }

  /**
   * Reads data from row buffer regions and appends result to provided ArrayBuffer
   *
   * @param phdrRow PlaceHolderDiskRegion of row
   */
  def iterateRowData(phdrRow: PlaceHolderDiskRegion): Iterator[InternalRow] = {
    val regionMap = phdrRow.getRegionMap
    if (regionMap == null || regionMap.isEmpty) return Iterator.empty
    logDebug(s"RegionMap keys for $phdrRow are: ${regionMap.keySet()}")
    val regMapItr = regionMap.regionEntries().iterator().asScala
    regMapItr.map { regEntry =>
      getValueInVMOrDiskWithoutFaultIn(phdrRow, regEntry) match {
        case bytes: Array[Byte] => getRow(bytes, byteArrays = null)
        case byteArrays: Array[Array[Byte]] => getRow(byteArrays(0), byteArrays)
        case Token.TOMBSTONE => null
      }
    }.filter(_ ne null)
  }

  def getValueInVMOrDiskWithoutFaultIn(phdr: PlaceHolderDiskRegion, entry: RegionEntry): Any = {
    val regEntry = phdr.getDiskEntry(entry.getKey)
    val rawValue = false
    val faultin = false
    var v = DiskEntry.Helper.getValueRetain(regEntry, phdr, rawValue)
    val isRemovedFromDisk = Token.isRemovedFromDisk(v)
    if (v == null || isRemovedFromDisk) {
      regEntry.synchronized {
        v = DiskEntry.Helper.getValueRetain(regEntry, phdr, rawValue)
        if (v == null) {
          v = Helper.getOffHeapValueOnDiskOrBuffer(
            regEntry, phdr.getDiskRegionView, phdr, faultin, rawValue)
        }
      }
    }
    if (isRemovedFromDisk) v = null
    else v match {
      case bs: ByteSource =>
        val deserVal = bs.getDeserializedForReading
        if (deserVal ne v) {
          bs.release()
          v = deserVal
        }
      case _ =>
    }
    v
  }

  /**
   * Reads data from col buffer regions and appends result to provided ArrayBuffer
   *
   * @param phdrCol PlaceHolderDiskRegion of column batch
   */
  def iterateColData(phdrCol: PlaceHolderDiskRegion): Iterator[InternalRow] = {
    if (phdrCol.getRegionMap == null || phdrCol.getRegionMap.isEmpty) return Iterator.empty
    val directBuffers = mutable.ListBuffer.empty[ByteBuffer]
    val regMap = phdrCol.getRegionMap
    // assert(regMap != null, "region map for column batch is null")
    logDebug(s"RegionMap size for $phdrCol are: ${regMap.keySet().size()}")
    regMap.keySet().iterator().asScala.flatMap {
      // for every stats key there will be a key corresponding to every column in schema
      // we can get keys and therefore values of a column batch from the stats key by
      // changing the index to corresponding column index
      case k: ColumnFormatKey if k.getColumnIndex == ColumnFormatEntry.STATROW_COL_INDEX =>
        // get required info about deletes
        val delKey = k.withColumnIndex(ColumnFormatEntry.DELETE_MASK_COL_INDEX)
        val delEntry = regMap.getEntry(delKey)
        val (deleteBuffer, deleteDecoder) = if (delEntry ne null) {
          val regValue = DiskEntry.Helper.readValueFromDisk(delEntry.asInstanceOf[DiskEntry],
            phdrCol).asInstanceOf[ColumnFormatValue]
          val valueBuffer = regValue.getValueRetain(FetchRequest.DECOMPRESS).getBuffer
          valueBuffer -> new ColumnDeleteDecoder(valueBuffer)
        } else (null, null)
        // get required info about columns
        var columnIndex = 1
        var hasTombstone = false
        val decodersAndValues = schema.map { field =>
          val columnKey = k.withColumnIndex(columnIndex)
          columnIndex += 1
          val entry = regMap.getEntry(columnKey)
          if (!hasTombstone && entry.isTombstone) {
            hasTombstone = true
          }
          if (hasTombstone) null else {
            val value = getValueInVMOrDiskWithoutFaultIn(phdrCol, entry)
                .asInstanceOf[ColumnFormatValue]
            val valueBuffer = value.getValueRetain(FetchRequest.DECOMPRESS).getBuffer
            val decoder = ColumnEncoding.getColumnDecoder(valueBuffer, field)
            val valueArray = if (valueBuffer == null || valueBuffer.isDirect) {
              directBuffers += valueBuffer
              null
            } else {
              valueBuffer.array()
            }
            decoder -> valueArray
          }
        }

        if (hasTombstone) Iterator.empty
        else {
          val statsEntry = regMap.getEntry(k)
          val statsValue = getValueInVMOrDiskWithoutFaultIn(phdrCol, statsEntry)
              .asInstanceOf[ColumnFormatValue].getValueRetain(FetchRequest.DECOMPRESS)
          val numStatsColumns = schema.size * ColumnStatsSchema.NUM_STATS_PER_COLUMN + 1
          val stats = org.apache.spark.sql.collection.SharedUtils
              .toUnsafeRow(statsValue.getBuffer, numStatsColumns)
          val numOfRows = stats.getInt(0)
          val deletedCount = if ((deleteDecoder ne null) && (deleteBuffer ne null)) {
            val allocator = ColumnEncoding.getAllocator(deleteBuffer)
            ColumnEncoding.readInt(allocator.baseObject(deleteBuffer),
              allocator.baseOffset(deleteBuffer) + deleteBuffer.position() + 8)
          } else 0
          var currentDeleted = 0
          val colNullCounts = Array.fill[Int](schema.size)(0)

          val updatedDecoders = schema.indices.map { colIndx =>
            val deltaColIndex = ColumnDelta.deltaColumnIndex(colIndx, 0)
            val deltaEntry1 = regMap.getEntry(k.withColumnIndex(deltaColIndex))
            val delta1 = if (deltaEntry1 ne null) {
              val buffer = DiskEntry.Helper.readValueFromDisk(deltaEntry1.asInstanceOf[DiskEntry],
                phdrCol).asInstanceOf[ColumnFormatValue]
                  .getValueRetain(FetchRequest.DECOMPRESS).getBuffer
              if (buffer.isDirect) directBuffers += buffer
              buffer
            } else null

            val deltaEntry2 = regMap.getEntry(k.withColumnIndex(deltaColIndex - 1))
            val delta2 = if (deltaEntry2 ne null) {
              val buffer = DiskEntry.Helper.readValueFromDisk(deltaEntry2.asInstanceOf[DiskEntry],
                phdrCol).asInstanceOf[ColumnFormatValue]
                  .getValueRetain(FetchRequest.DECOMPRESS).getBuffer
              if (buffer.isDirect) directBuffers += buffer
              buffer
            } else null

            val updateDecoder = if ((delta1 ne null) || (delta2 ne null)) {
              UpdatedColumnDecoder(decodersAndValues(colIndx)._1, schema(colIndx), delta1, delta2)
            } else null
            updateDecoder
          }

          (0 until (numOfRows - deletedCount)).map { rowNum =>
            while ((deleteDecoder ne null) && deleteDecoder.deleted(rowNum + currentDeleted)) {
              // null counts should be added as we go even for deleted records
              // because it is required to build indexes in colbatch
              schema.indices.map { colIndx =>
                val decoderAndValue = decodersAndValues(colIndx)
                val colDecoder = decoderAndValue._1
                val colNextNullPosition = colDecoder.getNextNullPosition
                if (rowNum + currentDeleted == colNextNullPosition) {
                  colNullCounts(colIndx) += 1
                  colDecoder.findNextNullPosition(
                    decoderAndValue._2, colNextNullPosition, colNullCounts(colIndx))
                }
              }
              // calculate how many consecutive rows to skip so that
              // i+numDeletd points to next un deleted row
              currentDeleted += 1
            }
            InternalRow.fromSeq(schema.indices.map { colIndx =>
              val decoderAndValue = decodersAndValues(colIndx)
              val colDecoder = decoderAndValue._1
              val colArray = decoderAndValue._2
              val colNextNullPosition = colDecoder.getNextNullPosition
              val fieldIsNull = rowNum + currentDeleted == colNextNullPosition
              if (fieldIsNull) {
                colNullCounts(colIndx) += 1
                colDecoder.findNextNullPosition(
                  colArray, colNextNullPosition, colNullCounts(colIndx))
              }

              val updatedDecoder = updatedDecoders(colIndx)
              if ((updatedDecoder ne null) && !updatedDecoder.unchanged(rowNum + currentDeleted) &&
                  updatedDecoder.readNotNull) {
                getUpdatedValue(updatedDecoder.getCurrentDeltaBuffer, schema(colIndx))
              } else {
                if (fieldIsNull) null else {
                  getDecodedValue(colDecoder, colArray,
                    schema(colIndx).dataType, rowNum + currentDeleted - colNullCounts(colIndx))
                }
              }
            })
          }.toIterator
        }
      case _ => Iterator.empty
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    logDebug(s"starting compute for partition ${split.index} of table $fullTableName")
    try {
      val currentHost = SparkEnv.get.executorId
      val expectedHost = bucketHostMap.getOrElse(split.index, "")
      require(expectedHost.nonEmpty, s"Preferred host cannot be empty for partition ${
        split
            .index
      } of table $fullTableName. Verify corresponding entry in combinedViewsMapSortedSet " +
          s"from debug logs of the leader.")

      if (expectedHost != currentHost) {
        throw new IllegalStateException(s"Expected compute to launch at $expectedHost," +
            s" but was launched at $currentHost. Try increasing value of " +
            s"spark.locality.wait.process higher than current value(default 1800s) in recovery " +
            s"mode. Refer troubleshooting section under Data Extractor Tool for more explanation.")
      }

      val diskStores = Misc.getGemFireCache.listDiskStores()
      var diskStrCol: DiskStoreImpl = null
      var diskStrRow: DiskStoreImpl = null

      val regionPath = Misc.getRegionPath(internalFQTN)
      val colRegPath = if (regionPath eq null) {
        throw new IllegalStateException(s"regionPath for $internalFQTN not found")
      } else {
        s"/_PR//B_${regionPath.substring(1, regionPath.length - 1)}/_${split.index}"
      }
      val rowRegPath = s"/_PR//B_${fullTableName.replace('.', '/')}/${split.index}"

      var phdrRow: PlaceHolderDiskRegion = null
      var phdrCol: PlaceHolderDiskRegion = null

      for (diskStore <- diskStores.asScala) {
        val diskRegMap = diskStore.getAllDiskRegions
        logDebug(s"Number of Disk Regions : ${diskRegMap.size()} in ${diskStore.toString}")
        for ((_, adr) <- diskRegMap.asScala) {
          val adrPath = adr.getFullPath
          var adrUnescapePath = PartitionedRegionHelper.unescapePRPath(adrPath)
          // var adrUnescapePath = adrPath
          // unescapePRPath replaces _ in db or table name with /
          val wrongTablePattern = tableName.replace('_', '/')
          if (tableName.contains('_') && adrUnescapePath.contains(wrongTablePattern)) {
            adrUnescapePath = adrUnescapePath
                .replace(wrongTablePattern, tableName.replace('/', '_'))
          }
          // todo: add more debug lines here
          if (adrUnescapePath.equals(colRegPath) && adr.isBucket) {
            diskStrCol = diskStore
            phdrCol = adr.asInstanceOf[PlaceHolderDiskRegion]
          } else if (adrUnescapePath.equals(rowRegPath)) {
            diskStrRow = diskStore
            phdrRow = adr.asInstanceOf[PlaceHolderDiskRegion]
          } else if (!adr.isBucket && adrUnescapePath
              .equals("/" + fullTableName.replace('.', '/'))) {
            diskStrRow = diskStore
            phdrRow = adr.asInstanceOf[PlaceHolderDiskRegion]
          }
        }
      }
      assert(diskStrRow != null, s"Row disk store is null. row region path not found:$rowRegPath")
      assert(phdrRow != null, s"PlaceHolderDiskRegion not found for regionPath=$rowRegPath")
      val rowIter: Iterator[InternalRow] = iterateRowData(phdrRow)

      if (isColumnTable) {
        assert(diskStrCol != null, s"col disk store is null")
        val colIter = iterateColData(phdrCol)
        rowIter ++ colIter
      } else {
        rowIter
      }
    } catch {
      // in case of error log and return empty iterator. cluster shouldn't go down
      case x@(_: Exception | _: AssertionError) =>
        logError(s"Unable to read $fullTableName.", x)
        Seq.empty.iterator
    }
  }

  def getUpdatedValue(currentDeltaBuffer: ColumnDeltaDecoder, field: StructField): Any = {
    field.dataType match {
      case LongType => currentDeltaBuffer.readLong
      case IntegerType => currentDeltaBuffer.readInt
      case BooleanType => currentDeltaBuffer.readBoolean
      case ByteType => currentDeltaBuffer.readByte
      case FloatType => currentDeltaBuffer.readFloat
      case DoubleType => currentDeltaBuffer.readDouble
      case BinaryType => currentDeltaBuffer.readBinary
      case ShortType => currentDeltaBuffer.readShort
      case TimestampType => new Timestamp(currentDeltaBuffer.readTimestamp / 1000)
      case StringType => currentDeltaBuffer.readUTF8String
      case DateType =>
        val daysSinceEpoch = currentDeltaBuffer.readDate
        DateTimeUtils.toJavaDate(daysSinceEpoch)
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        currentDeltaBuffer.readLongDecimal(d.precision, d.scale)
      case d: DecimalType => currentDeltaBuffer.readDecimal(d.precision, d.scale)
      case _ => null
    }
  }

  /**
   * For a given StructField datatype this method reads appropriate
   * value from provided Byte[]
   *
   * @param decoder  decoder for the given Byte[]
   * @param value    Byte[] read from the region
   * @param dataType datatype of the column which is to be decoded
   * @param rowNum   next non null position in Byte[]
   * @return decoded value of the given datatype
   */
  def getDecodedValue(
      decoder: ColumnDecoder,
      value: Array[Byte],
      dataType: DataType,
      rowNum: Int): Any = {
    dataType match {
      case LongType => decoder.readLong(value, rowNum)
      case IntegerType => decoder.readInt(value, rowNum)
      case BooleanType => decoder.readBoolean(value, rowNum)
      case ByteType => decoder.readByte(value, rowNum)
      case FloatType => decoder.readFloat(value, rowNum)
      case DoubleType => decoder.readDouble(value, rowNum)
      case BinaryType => decoder.readBinary(value, rowNum)
      case ShortType => decoder.readShort(value, rowNum)
      case TimestampType =>
        val lv = decoder.readTimestamp(value, rowNum) / 1000
        new Timestamp(lv)
      case StringType => decoder.readUTF8String(value, rowNum)
      case DateType =>
        val daysSinceEpoch = decoder.readDate(value, rowNum)
        // adjust for timezone of machine
        DateTimeUtils.toJavaDate(daysSinceEpoch)
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        decoder.readLongDecimal(value, d.precision, d.scale, rowNum)
      case d: DecimalType => decoder.readDecimal(value, d.precision, d.scale, rowNum)
      case a: ArrayType =>
        if (a.elementType == DateType) {
          decoder.readArray(value, rowNum).toArray[Integer](IntegerType)
              .map(daysSinceEpoch => {
                if (daysSinceEpoch == null) null else DateTimeUtils.toJavaDate(daysSinceEpoch)
              })
        } else if (a.elementType == TimestampType) {
          decoder.readArray(value, rowNum).toArray[Object](LongType).map(nsSinceEpoch => {
            if (nsSinceEpoch == null) null else new Timestamp(nsSinceEpoch.asInstanceOf[Long] /
                1000)
          })
        } else {
          decoder.readArray(value, rowNum).toArray(a.elementType)
        }
      case _: MapType => decoder.readMap(value, rowNum)
      case s: StructType => decoder.readStruct(value, s.length, rowNum)
      case _ => null
    }
  }

  def getPartitionEvaluator: () => Array[Partition] = () => getPartitions

  /**
   * Returns number of buckets for a given schema and table name
   *
   * @return number of buckets
   */
  override protected def getPartitions: Array[Partition] = {
    val (numBuckets, _) = RecoveryService.getNumBuckets(dbName, tableName)
    if (numBuckets == 0) logWarning(s"Number of buckets for $dbName.$tableName is 0.")
    val partition = (0 until numBuckets).map { p =>
      new Partition {
        override def index: Int = p
      }
    }.toArray[Partition]
    partition
  }

  /**
   * Returns seq of hostnames where the corresponding
   * split/bucket is present.
   *
   * @param split partition corresponding to bucket
   * @return sequence of hostnames
   */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val preferredHosts = RecoveryService.getExecutorHost(dbName, tableName, split.index)
    logDebug(s"Preferred hosts for partition ${split.index} of $fullTableName are $preferredHosts")
    preferredHosts
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    super.write(kryo, output)
    output.writeString(internalFQTN)
    output.writeString(fullTableName)
    output.writeBoolean(isColumnTable)
    output.writeInt(tableSchemaMap.size)
    tableSchemaMap.iterator.foreach(ele => {
      output.writeString(ele._1)
      StructTypeSerializer.write(kryo, output, ele._2)
    })
    output.writeInt(bucketHostMap.size)
    bucketHostMap.foreach(ele => {
      output.writeInt(ele._1)
      output.writeString(ele._2)
    })
    StructTypeSerializer.write(kryo, output, schema)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)
    internalFQTN = input.readString()
    fullTableName = input.readString()
    isColumnTable = input.readBoolean()
    val schemaMapSize = input.readInt()
    tableSchemaMap = mutable.Map.empty
    for (_ <- 0 until schemaMapSize) {
      val k = input.readString()
      val v = StructTypeSerializer.read(kryo, input, c = null)
      tableSchemaMap.put(k, v)
    }
    val bucketHostMapSize = input.readInt()
    bucketHostMap = mutable.Map.empty
    (0 until bucketHostMapSize).foreach(_ => {
      val k = input.readInt()
      val v = input.readString()
      bucketHostMap.put(k, v)
    })
    schema = StructTypeSerializer.read(kryo, input, c = null)
  }
}

private final class DVDToInternalRow(dvds: Array[DataValueDescriptor])
    extends InternalRow with TypeSpecializedGetters {

  override def numFields: Int = dvds.length

  override def setNullAt(i: Int): Unit = throw new UnsupportedOperationException()

  override def update(i: Int, value: Any): Unit = throw new UnsupportedOperationException()

  override def copy(): InternalRow = new DVDToInternalRow(dvds)

  override def isNullAt(ordinal: Int): Boolean = dvds(ordinal).isNull

  override def getBoolean(ordinal: Int): Boolean = dvds(ordinal).getBoolean

  override def getByte(ordinal: Int): Byte = dvds(ordinal).getByte

  override def getShort(ordinal: Int): Short = dvds(ordinal).getShort

  override def getInt(ordinal: Int): Int = dvds(ordinal).getInt

  override def getLong(ordinal: Int): Long = dvds(ordinal).getLong

  override def getFloat(ordinal: Int): Float = dvds(ordinal).getFloat

  override def getDouble(ordinal: Int): Double = dvds(ordinal).getDouble

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    val bd = dvds(ordinal).getObject match {
      case bd: JDecimal => bd
      case f: java.lang.Float => new JDecimal(java.lang.Float.toString(f.floatValue()))
      case d: java.lang.Double => JDecimal.valueOf(d.doubleValue())
      case n: java.lang.Number => JDecimal.valueOf(n.longValue())
      case o => new JDecimal(o.toString)
    }
    Decimal(bd)
  }

  override def getUTF8String(ordinal: Int): UTF8String =
    UTF8String.fromString(dvds(ordinal).getString)

  override def getBinary(ordinal: Int): Array[Byte] = dvds(ordinal).getBytes

  override def getInterval(ordinal: Int): CalendarInterval =
    new CalendarInterval(0, dvds(ordinal).getLong)

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
    val data = dvds(ordinal).getBytes
    if (data ne null) {
      val row = new SerializedRow(4, numFields) // includes size
      row.pointTo(data, Platform.BYTE_ARRAY_OFFSET, data.length)
      row
    } else null
  }

  override def getArray(ordinal: Int): ArrayData = {
    val data = dvds(ordinal).getBytes
    if (data ne null) {
      val array = new SerializedArray(8) // includes size
      array.pointTo(data, Platform.BYTE_ARRAY_OFFSET, data.length)
      array
    } else null
  }

  override def getMap(ordinal: Int): MapData = {
    val data = dvds(ordinal).getBytes
    if (data ne null) {
      val map = new SerializedMap()
      map.pointTo(data, Platform.BYTE_ARRAY_OFFSET)
      map
    } else null
  }

  override def get(ordinal: Int, dataType: DataType): AnyRef = typedGet(ordinal, dataType)
}

private final class ColumnDecoderRow(decoders: Array[ColumnDecoder], data: Array[AnyRef],
    deletedDecoder: ColumnDeleteDecoder, isLongDecimal: Array[Boolean], numRows: Int)
    extends InternalRow with TypeSpecializedGetters {

  private[this] var rowIndex: Int = -1

  private def setRowIndex(index: Int): ColumnDecoderRow = {
    rowIndex = index
    this
  }

  def nextRow(): Boolean = {
    rowIndex += 1
    while (rowIndex < numRows) {
      if ((deletedDecoder ne null) && deletedDecoder.deleted(rowIndex)) {
        rowIndex += 1
      } else {
        var i = 0
        while (i < decoders.length) {
          val decoder = decoders(i)
          val nextNullPosition = decoder.getNextNullPosition
          if (rowIndex <= nextNullPosition) {
            decoder.nonNullPosition += 1
          } else {
            decoder.findNextNullPosition(data(i), nextNullPosition, 0)
          }
          i += 1
        }
        return true
      }
    }
    false
  }

  override def numFields: Int = decoders.length

  override def setNullAt(i: Int): Unit = throw new UnsupportedOperationException()

  override def update(i: Int, value: Any): Unit = throw new UnsupportedOperationException()

  override def copy(): InternalRow = {
    new ColumnDecoderRow(decoders, data, deletedDecoder, isLongDecimal, numRows)
        .setRowIndex(rowIndex)
  }

  override def isNullAt(ordinal: Int): Boolean = rowIndex == decoders(ordinal).getNextNullPosition

  override def getBoolean(ordinal: Int): Boolean = {
    val decoder = decoders(ordinal)
    decoder.readBoolean(data(ordinal), decoder.nonNullPosition)
  }

  override def getByte(ordinal: Int): Byte = {
    val decoder = decoders(ordinal)
    decoder.readByte(data(ordinal), decoder.nonNullPosition)
  }

  override def getShort(ordinal: Int): Short = {
    val decoder = decoders(ordinal)
    decoder.readShort(data(ordinal), decoder.nonNullPosition)
  }

  override def getInt(ordinal: Int): Int = {
    val decoder = decoders(ordinal)
    decoder.readInt(data(ordinal), decoder.nonNullPosition)
  }

  override def getLong(ordinal: Int): Long = {
    val decoder = decoders(ordinal)
    decoder.readLong(data(ordinal), decoder.nonNullPosition)
  }

  override def getFloat(ordinal: Int): Float = {
    val decoder = decoders(ordinal)
    decoder.readFloat(data(ordinal), decoder.nonNullPosition)
  }

  override def getDouble(ordinal: Int): Double = {
    val decoder = decoders(ordinal)
    decoder.readDouble(data(ordinal), decoder.nonNullPosition)
  }

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    val decoder = decoders(ordinal)
    if (isLongDecimal(ordinal)) {
      decoder.readLongDecimal(data(ordinal), precision, scale, decoder.nonNullPosition)
    } else {
      decoder.readDecimal(data(ordinal), precision, scale, decoder.nonNullPosition)
    }
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    val decoder = decoders(ordinal)
    decoder.readUTF8String(data(ordinal), decoder.nonNullPosition)
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    val decoder = decoders(ordinal)
    decoder.readBinary(data(ordinal), decoder.nonNullPosition)
  }

  override def getInterval(ordinal: Int): CalendarInterval = {
    val decoder = decoders(ordinal)
    decoder.readInterval(data(ordinal), decoder.nonNullPosition)
  }

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
    val decoder = decoders(ordinal)
    decoder.readStruct(data(ordinal), numFields, decoder.nonNullPosition)
  }

  override def getArray(ordinal: Int): ArrayData = {
    val decoder = decoders(ordinal)
    decoder.readArray(data(ordinal), decoder.nonNullPosition)
  }

  override def getMap(ordinal: Int): MapData = {
    val decoder = decoders(ordinal)
    decoder.readMap(data(ordinal), decoder.nonNullPosition)
  }

  override def get(ordinal: Int, dataType: DataType): AnyRef = typedGet(ordinal, dataType)
}
