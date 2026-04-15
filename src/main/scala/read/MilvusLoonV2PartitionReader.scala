package com.zilliz.spark.connector.read

import org.apache.arrow.c.{ArrowArrayStream, ArrowSchema, Data}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

import com.zilliz.spark.connector.loon.Properties
import com.zilliz.spark.connector.serde.ArrowConverter
import com.zilliz.spark.connector.MilvusOption
import io.milvus.grpc.schema.CollectionSchema
import io.milvus.storage.{
  ArrowUtils,
  MilvusStorageColumnGroups,
  MilvusStorageReader,
  NativeLibraryLoader
}

/** Spark partition reader for milvus-segment-info `storage_version = 2`
  * (non-manifest packed parquet) segments.
  *
  * Unlike [[MilvusLoonPartitionReader]], which resolves a milvus-storage
  * `.milvus_manifest` file via `MilvusStorageManifest.getColumnGroupsScala`,
  * this reader is handed a pre-materialized set of [[V2ColumnGroup]]s recovered
  * from the snapshot AVRO (slot -> file paths) + one parquet footer's
  * `group_field_id_list` kv-metadata (slot -> real field IDs). The packed
  * reader then opens only the files for the requested columns.
  *
  * The reader iterates the Arrow stream sequentially (no vector search, no
  * filter pushdown). Backfill only needs the PK column plus positional row
  * metadata added by [[MilvusPartitionReaderFactory]], so the minimal shape
  * fits well.
  */
class MilvusLoonV2PartitionReader(
    schema: StructType,
    columnGroups: Seq[V2ColumnGroup],
    milvusSchema: CollectionSchema,
    milvusOption: MilvusOption,
    neededColumnFieldIds: Seq[Long]
) extends PartitionReader[InternalRow]
    with Logging {

  NativeLibraryLoader.loadLibrary()

  private val allocator = ArrowUtils.getAllocator
  private val sourceSchema = schema

  // Field ID -> logical column name. StorageV2 packed-parquet files written
  // by milvus segcore use the field's logical name in the parquet schema
  // (with PARQUET:field_id metadata for cross-reference). The packed reader
  // matches columns by name, so we must pass logical names — NOT
  // field-id-as-string like the V3 reader does.
  private val fieldIdToName: Map[Long, String] = {
    val systemFields = Map(0L -> "RowID", 1L -> "Timestamp")
    val userFields =
      milvusSchema.fields.map(f => f.fieldID -> f.name).toMap
    systemFields ++ userFields
  }
  private val fieldNameToId: Map[String, Long] =
    fieldIdToName.map { case (id, name) => name -> id }

  // Arrow schema for the read: columns named by their logical names so the
  // packed reader can find them by name in the on-disk parquet.
  private val (arrowSchemaObj, arrowSchemaPtr) = {
    val arrowSchema =
      com.zilliz.spark.connector.MilvusSchemaUtil.convertToArrowSchema(
        milvusSchema
      )
    val arrowSchemaC = ArrowSchema.allocateNew(allocator)
    Data.exportSchema(allocator, arrowSchema, null, arrowSchemaC)
    (arrowSchemaC, arrowSchemaC.memoryAddress())
  }

  // ArrowConverter looks up Spark column names against this map to find the
  // backing field id; both V2 and V3 readers share that contract.
  private val fieldNameToIdString: Map[String, String] =
    fieldNameToId.map { case (name, id) => name -> id.toString }

  private val readerProperties = Properties.fromMilvusOption(milvusOption)

  // Which column names to ask the packed reader for. Prefer explicit
  // projection from neededColumnFieldIds; fall back to sourceSchema's Spark
  // field names. In both cases we coerce to logical names that the parquet
  // actually carries.
  private val neededColumns: Array[String] = {
    val explicitNames: Seq[String] =
      if (neededColumnFieldIds.nonEmpty)
        neededColumnFieldIds.flatMap(fid => fieldIdToName.get(fid))
      else sourceSchema.fieldNames.toSeq.filter(fieldNameToId.contains)
    // Drop names that aren't declared by any of the v2 column groups —
    // `loon_reader_new` would reject unknown columns.
    val declared: Set[String] =
      columnGroups.flatMap(_.fieldIds.flatMap(fieldIdToName.get)).toSet
    explicitNames.filter(name => declared.contains(name)).toArray
  }

  // Build LoonColumnGroups directly from the materialized v2 layout. Each
  // group's `columns` is the LOGICAL names of its fields (matching what
  // milvus segcore wrote into the parquet). Row counts (per file) come from
  // the AVRO's AvroBinlog.entries_num — required because the packed reader
  // rejects negative end indices.
  private val columnGroupsPtr: Long = {
    val cols = columnGroups.map { cg =>
      cg.fieldIds.flatMap(fieldIdToName.get).toArray
    }.toArray
    val files = columnGroups.map(_.filePaths.toArray).toArray
    val rowCounts = columnGroups.map { cg =>
      require(
        cg.fileRowCounts.size == cg.filePaths.size,
        s"V2ColumnGroup with fields=${cg.fieldIds} has ${cg.filePaths.size} files " +
          s"but ${cg.fileRowCounts.size} row counts; both must match"
      )
      cg.fileRowCounts.toArray
    }.toArray
    MilvusStorageColumnGroups.createFromGroups(cols, files, rowCounts)
  }

  private val reader = new MilvusStorageReader()
  reader.create(
    columnGroupsPtr,
    arrowSchemaPtr,
    neededColumns,
    readerProperties
  )

  if (!reader.isValid) {
    throw new IllegalStateException(
      "Failed to create MilvusStorageReader for V2 packed segment"
    )
  }

  private val recordBatchReaderPtr = reader.getRecordBatchReaderScala()
  private val arrowArrayStream = ArrowArrayStream.wrap(recordBatchReaderPtr)
  private val arrowReader = Data.importArrayStream(allocator, arrowArrayStream)

  private var _currentBatch: VectorSchemaRoot = {
    if (arrowReader.loadNextBatch()) arrowReader.getVectorSchemaRoot else null
  }
  private var _currentRowIndex: Int = 0

  override def next(): Boolean = {
    // Advance past exhausted batches.
    while (
      _currentBatch != null && _currentRowIndex >= _currentBatch.getRowCount
    ) {
      if (arrowReader.loadNextBatch()) {
        _currentBatch = arrowReader.getVectorSchemaRoot
        _currentRowIndex = 0
      } else {
        _currentBatch = null
      }
    }
    _currentBatch != null && _currentRowIndex < _currentBatch.getRowCount
  }

  override def get(): InternalRow = {
    if (_currentBatch == null) {
      throw new IllegalStateException("No batch loaded")
    }
    val row = ArrowConverter.arrowToInternalRow(
      _currentBatch,
      _currentRowIndex,
      sourceSchema,
      fieldNameToIdString
    )
    _currentRowIndex += 1
    row
  }

  override def close(): Unit = {
    try {
      if (arrowReader != null) arrowReader.close()
      if (arrowArrayStream != null) arrowArrayStream.close()
      if (reader != null) reader.destroy()
      if (columnGroupsPtr != 0L) {
        MilvusStorageColumnGroups.destroy(columnGroupsPtr)
      }
      if (arrowSchemaObj != null) arrowSchemaObj.close()
      readerProperties.free()
    } catch {
      case e: Exception =>
        logWarning("Error closing V2 packed-parquet reader", e)
    }
  }
}
