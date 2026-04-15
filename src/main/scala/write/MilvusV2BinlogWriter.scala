package com.zilliz.spark.connector.write

import java.net.URI
import java.util.{HashMap => JHashMap}
import scala.collection.mutable

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/** Describes one parquet file produced by a backfill write into a StorageV2
  * (non-manifest packed parquet) segment.
  *
  * @param fieldId
  *   Milvus field ID of the new column.
  * @param logId
  *   The `logID` we allocated for this file (also the last path segment).
  * @param path
  *   Bucket-relative path under `insert_log/...` ready to be stored back in the
  *   snapshot AVRO.
  * @param rowsWritten
  *   Row count actually written (for validation / result JSON).
  */
case class V2BinlogFile(
    fieldId: Long,
    logId: Long,
    path: String,
    rowsWritten: Long
)

/** Writes StorageV2 per-field binlog parquet files for a single segment.
  *
  * Backfill always emits **single-field column groups**, so the writer creates
  * one parquet file per new field at
  * {{{
  *   {rootPath}/insert_log/{coll}/{part}/{seg}/{fieldID}/{logID}
  * }}}
  * with the kv-metadata milvus expects for StorageV2:
  *   - `storage_version = "1.0.0"` (parquet-level)
  *   - `group_field_id_list = "{fieldID}"` (single-field group)
  * and the Arrow-compatible field metadata `PARQUET:field_id = "{fieldID}"`.
  *
  * The writer is NOT thread-safe; create one per segment on the executor.
  *
  * Rows must arrive with fields in the same positional order as `targetSchema`.
  */
class MilvusV2BinlogWriter(
    collectionId: Long,
    partitionId: Long,
    segmentId: Long,
    newFieldNames: Seq[String], // positional in targetSchema
    newFieldIds: Seq[Long], // same size as newFieldNames
    targetSchema: StructType, // one StructField per new field, no metadata cols
    rootPath: String, // bucket-relative root (usually "files")
    bucket: String, // s3a bucket for full-path construction
    hadoopConf: Configuration,
    allocateLogId: () => Long
) extends Logging {

  require(
    newFieldNames.size == newFieldIds.size && newFieldIds.size == targetSchema.fields.length,
    s"newFieldNames/newFieldIds/targetSchema size mismatch: " +
      s"${newFieldNames.size} / ${newFieldIds.size} / ${targetSchema.fields.length}"
  )

  private case class PerFieldState(
      fieldId: Long,
      logId: Long,
      bucketRelativePath: String,
      schema: Schema,
      writer: org.apache.parquet.hadoop.ParquetWriter[GenericRecord],
      var rowCount: Long
  )

  private val fields: Array[PerFieldState] = newFieldNames.zipWithIndex.map {
    case (name, idx) =>
      val fid = newFieldIds(idx)
      val logId = allocateLogId()
      val bucketRelative =
        s"${rootPath.stripSuffix("/")}/insert_log/$collectionId/$partitionId/$segmentId/$fid/$logId"
      val fullPath = s"s3a://${bucket.stripPrefix("/")}/$bucketRelative"
      val sparkField = targetSchema.fields(idx)
      val avroSchema = buildSingleFieldSchema(name, sparkField.dataType, fid)
      val kv = new JHashMap[String, String]()
      // StorageV2's parquet-level kv-metadata as observed on real milvus files.
      kv.put("storage_version", "1.0.0")
      // Single-field group: the kv value is just the field id as a plain
      // string (verified against the v2 sample with group_field_id_list
      // == "100,0,1;101;102" — a single-field file would omit the separator).
      kv.put("group_field_id_list", fid.toString)
      val writer = AvroParquetWriter
        .builder[GenericRecord](new Path(new URI(fullPath)))
        .withSchema(avroSchema)
        .withDataModel(GenericData.get())
        .withCompressionCodec(CompressionCodecName.ZSTD)
        .withConf(hadoopConf)
        .withExtraMetaData(kv)
        .build()
      PerFieldState(fid, logId, bucketRelative, avroSchema, writer, 0L)
  }.toArray

  /** Consume one Spark row. Each field in `row` must be at the same positional
    * index as its entry in `targetSchema`.
    */
  def write(row: InternalRow): Unit = {
    var i = 0
    while (i < fields.length) {
      val state = fields(i)
      val sparkField = targetSchema.fields(i)
      val value = extractValue(row, i, sparkField.dataType)
      val rec = new GenericData.Record(state.schema)
      rec.put(sparkField.name, value)
      state.writer.write(rec)
      state.rowCount += 1
      i += 1
    }
  }

  /** Close all per-field writers and return the list of produced binlog files.
    */
  def close(): Seq[V2BinlogFile] = {
    val out = mutable.ArrayBuffer.empty[V2BinlogFile]
    var firstErr: Throwable = null
    fields.foreach { state =>
      try state.writer.close()
      catch {
        case e: Throwable if firstErr == null => firstErr = e
        case e: Throwable =>
          logWarning(s"secondary close error for fieldId=${state.fieldId}", e)
      }
      out += V2BinlogFile(
        fieldId = state.fieldId,
        logId = state.logId,
        path = state.bucketRelativePath,
        rowsWritten = state.rowCount
      )
    }
    if (firstErr != null) throw firstErr
    out.toSeq
  }

  /** Abort: best-effort close of any open writers. Errors are logged, not
    * thrown — we want the surrounding `try`/`catch` to surface the *original*
    * exception, not a secondary abort failure.
    */
  def abort(): Unit = {
    fields.foreach { state =>
      try state.writer.close()
      catch {
        case e: Throwable =>
          logWarning(
            s"abort: error closing writer for fieldId=${state.fieldId}",
            e
          )
      }
    }
  }

  // -------------------------------------------------------------------------
  // Private: Avro schema + value extraction
  // -------------------------------------------------------------------------

  /** Build an Avro record schema for one field. The field is always nullable
    * (union[null, T]) to match Milvus's nullability-by-default convention and
    * to tolerate backfill data that contains NULLs for un-joined rows.
    */
  private def buildSingleFieldSchema(
      fieldName: String,
      dataType: DataType,
      fieldId: Long
  ): Schema = {
    val inner = sparkTypeToAvro(dataType)
    val nullable = Schema.createUnion(Schema.create(Schema.Type.NULL), inner)
    val recordName = s"MilvusBinlogField_$fieldId"
    val rec = Schema.createRecord(recordName, null, "com.zilliz.milvus", false)
    val field =
      new Schema.Field(fieldName, nullable, null, null.asInstanceOf[AnyRef])
    // Arrow parquet readers (including milvus-storage) match columns by
    // PARQUET:field_id metadata. Set it so the read side can find our column
    // by its milvus field id regardless of the logical name.
    field.addProp("PARQUET:field_id", fieldId.toString)
    rec.setFields(java.util.Arrays.asList(field))
    rec
  }

  private def sparkTypeToAvro(dt: DataType): Schema = dt match {
    case BooleanType => Schema.create(Schema.Type.BOOLEAN)
    case IntegerType | ShortType | ByteType => Schema.create(Schema.Type.INT)
    case LongType                           => Schema.create(Schema.Type.LONG)
    case FloatType                          => Schema.create(Schema.Type.FLOAT)
    case DoubleType                         => Schema.create(Schema.Type.DOUBLE)
    case StringType                         => Schema.create(Schema.Type.STRING)
    case BinaryType                         => Schema.create(Schema.Type.BYTES)
    case _ =>
      throw new IllegalArgumentException(
        s"unsupported new-field data type for StorageV2 backfill: $dt. " +
          "Scalar types (bool/int/long/float/double/string/binary) are supported " +
          "in this phase; vector types require additional work."
      )
  }

  /** Pull a single field's value out of an InternalRow and normalize it to an
    * Avro-compatible Java object.
    */
  private def extractValue(
      row: InternalRow,
      idx: Int,
      dataType: DataType
  ): AnyRef = {
    if (row.isNullAt(idx)) return null
    dataType match {
      case BooleanType => java.lang.Boolean.valueOf(row.getBoolean(idx))
      case ByteType    => java.lang.Integer.valueOf(row.getByte(idx).toInt)
      case ShortType   => java.lang.Integer.valueOf(row.getShort(idx).toInt)
      case IntegerType => java.lang.Integer.valueOf(row.getInt(idx))
      case LongType    => java.lang.Long.valueOf(row.getLong(idx))
      case FloatType   => java.lang.Float.valueOf(row.getFloat(idx))
      case DoubleType  => java.lang.Double.valueOf(row.getDouble(idx))
      case StringType  => row.getUTF8String(idx).toString
      case BinaryType  => java.nio.ByteBuffer.wrap(row.getBinary(idx))
      case _ =>
        throw new IllegalArgumentException(
          s"unsupported value extraction for data type $dataType at idx=$idx"
        )
    }
  }
}
