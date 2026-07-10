#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/database.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {

PostgresMetadataManager::PostgresMetadataManager(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

bool PostgresMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	switch (type.id()) {
	// Unnamed composite types are not supported.
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	// Postgres timestamp/date ranges are narrower than DuckDB's
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	// Postgres bytea input format differs from DuckDB's blob text format
	case LogicalTypeId::BLOB:
	// Postgres cannot store null bytes in VARCHAR/TEXT columns
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::VARIANT:
	// If we knew that the Postgres installation has PostGIS installed, we could support GEOMETRY in the future.
	case LogicalTypeId::GEOMETRY:
		return false;
	default:
		return true;
	}
}

bool PostgresMetadataManager::SupportsInlining(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARIANT) {
		return false;
	}
	return DuckLakeMetadataManager::SupportsInlining(type);
}

string PostgresMetadataManager::GetColumnTypeInternal(const LogicalType &column_type) {
	switch (column_type.id()) {
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::TINYINT:
		return "SMALLINT";
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		return "INTEGER";
	case LogicalTypeId::UINTEGER:
		return "BIGINT";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return "BYTEA";
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return "VARCHAR";
	default:
		return column_type.ToString();
	}
}

unique_ptr<QueryResult> PostgresMetadataManager::ExecuteQuery(DuckLakeSnapshot snapshot, string &query,
                                                              string command) {
	auto &commit_info = transaction.GetCommitInfo();

	query = StringUtil::Replace(query, "{SNAPSHOT_ID}", to_string(snapshot.snapshot_id));
	query = StringUtil::Replace(query, "{SCHEMA_VERSION}", to_string(snapshot.schema_version));
	query = StringUtil::Replace(query, "{NEXT_CATALOG_ID}", to_string(snapshot.next_catalog_id));
	query = StringUtil::Replace(query, "{NEXT_FILE_ID}", to_string(snapshot.next_file_id));
	query = StringUtil::Replace(query, "{AUTHOR}", commit_info.author.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_MESSAGE}", commit_info.commit_message.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_EXTRA_INFO}", commit_info.commit_extra_info.ToSQLString());

	auto &connection = transaction.GetConnection();
	auto &ducklake_catalog = transaction.GetCatalog();
	auto catalog_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataDatabaseName());
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
	auto schema_identifier_escaped = StringUtil::Replace(schema_identifier, "'", "''");
	auto schema_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataSchemaName());
	auto metadata_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataPath());
	auto data_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.DataPath());

	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_LITERAL}", catalog_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_IDENTIFIER}", catalog_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_NAME_LITERAL}", schema_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", schema_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_ESCAPED}", schema_identifier_escaped);
	query = StringUtil::Replace(query, "{METADATA_PATH}", metadata_path);
	query = StringUtil::Replace(query, "{DATA_PATH}", data_path);

	return connection.Query(StringUtil::Format("CALL %s(%s, %s)", command, catalog_literal, SQLString(query)));
}
unique_ptr<QueryResult> PostgresMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	return ExecuteQuery(snapshot, query, "postgres_execute");
}

unique_ptr<QueryResult> PostgresMetadataManager::Query(DuckLakeSnapshot snapshot, string &query) {
	return DuckLakeMetadataManager::Query(snapshot, query);
}

string PostgresMetadataManager::GetLatestSnapshotQuery() const {
	return R"(
	SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},
		'SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
		 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot WHERE snapshot_id = (
		     SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
		 );')
	)";
}

string PostgresMetadataManager::WrapPostgresQuery(const string &pg_sql) const {
	auto escaped = StringUtil::Replace(pg_sql, "'", "''");
	return "SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL}, '" + escaped + "')";
}

vector<DuckLakeGlobalStatsInfo> PostgresMetadataManager::GetGlobalTableStats(DuckLakeSnapshot snapshot,
                                                                             TableIndex table_id) {
	// Run the stats lookup server-side so Postgres applies the table_id predicate instead of
	// DuckDB scanning the attached stats tables with ctid-range COPYs.
	auto query = WrapPostgresQuery(StringUtil::Format(R"(
SELECT table_id, column_id, record_count, next_row_id, file_size_bytes, contains_null, contains_nan, min_value, max_value, extra_stats
FROM {METADATA_SCHEMA_ESCAPED}.ducklake_table_stats
LEFT JOIN {METADATA_SCHEMA_ESCAPED}.ducklake_table_column_stats USING (table_id)
WHERE table_id = %llu
  AND record_count IS NOT NULL
  AND file_size_bytes IS NOT NULL
ORDER BY table_id)",
	                                                  table_id.index));
	auto result = Query(snapshot, query);
	return ParseGlobalTableStats(*result);
}

unique_ptr<QueryResult> PostgresMetadataManager::ReadFileColumnStatsForTable(DuckLakeSnapshot snapshot,
                                                                             TableIndex table_id) {
	auto query = WrapPostgresQuery(StringUtil::Format(R"(
SELECT data.data_file_id, data.record_count, data.file_size_bytes,
       stats.column_id, stats.value_count, stats.null_count, stats.min_value, stats.max_value,
       stats.contains_nan, stats.extra_stats
FROM {METADATA_SCHEMA_ESCAPED}.ducklake_data_file data
LEFT JOIN {METADATA_SCHEMA_ESCAPED}.ducklake_file_column_stats stats ON stats.data_file_id = data.data_file_id
WHERE data.table_id = %d
  AND {SNAPSHOT_ID} >= data.begin_snapshot
  AND ({SNAPSHOT_ID} < data.end_snapshot OR data.end_snapshot IS NULL)
ORDER BY data.data_file_id)",
	                                                  table_id.index));
	return Query(snapshot, query);
}

string PostgresMetadataManager::GetDataFileSource(TableIndex table_id) {
	// Match the table_id / snapshot predicates of the enclosing query (which still applies them)
	// so Postgres only returns the table's live files.
	return "(" +
	       WrapPostgresQuery(StringUtil::Format(
	           "SELECT * FROM {METADATA_SCHEMA_ESCAPED}.ducklake_data_file WHERE table_id=%d AND "
	           "{SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)",
	           table_id.index)) +
	       ")";
}

string PostgresMetadataManager::GetDeleteFileSource(TableIndex table_id) {
	return "(" +
	       WrapPostgresQuery(StringUtil::Format(
	           "SELECT * FROM {METADATA_SCHEMA_ESCAPED}.ducklake_delete_file WHERE table_id=%d AND "
	           "{SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)",
	           table_id.index)) +
	       ")";
}

string PostgresMetadataManager::GetFileColumnStatsJoinSource(TableIndex table_id, idx_t column_field_index) {
	return "(" +
	       WrapPostgresQuery(StringUtil::Format(
	           "SELECT data_file_id, table_id, column_id, min_value, max_value FROM "
	           "{METADATA_SCHEMA_ESCAPED}.ducklake_file_column_stats WHERE table_id=%d AND column_id=%d",
	           table_id.index, NumericCast<int64_t>(column_field_index))) +
	       ")";
}

string PostgresMetadataManager::GetFilePartitionValueSource(TableIndex table_id) {
	return "(" +
	       WrapPostgresQuery(StringUtil::Format(
	           "SELECT * FROM {METADATA_SCHEMA_ESCAPED}.ducklake_file_partition_value WHERE table_id=%d",
	           table_id.index)) +
	       ")";
}

string PostgresMetadataManager::GenerateFileColumnStatsCTEBody(const CTERequirement &req, TableIndex table_id) {
	string select_list = "data_file_id";
	for (const auto &stat : req.referenced_stats) {
		select_list += ", " + stat;
	}
	return StringUtil::Format("  SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},\n"
	                          "    'SELECT %s\n"
	                          "     FROM {METADATA_SCHEMA_ESCAPED}.ducklake_file_column_stats\n"
	                          "     WHERE column_id = %d AND table_id = %d')\n",
	                          select_list, req.column_field_index, table_id.index);
}

// We need a specialized function here to do a reinterpret for postgres from BLOB to VARCHAR
shared_ptr<DuckLakeInlinedData>
PostgresMetadataManager::TransformInlinedData(QueryResult &result, const vector<LogicalType> &expected_types) {
	bool needs_reinterpret = false;
	if (!expected_types.empty()) {
		D_ASSERT(expected_types.size() == result.types.size());
		for (idx_t i = 0; i < expected_types.size(); i++) {
			if (result.types[i] != expected_types[i]) {
				D_ASSERT(result.types[i].id() == LogicalTypeId::BLOB &&
				         expected_types[i].id() == LogicalTypeId::VARCHAR);
				needs_reinterpret = true;
			}
		}
	}
	if (!needs_reinterpret) {
		return DuckLakeMetadataManager::TransformInlinedData(result, expected_types);
	}

	if (result.HasError()) {
		result.GetErrorObject().Throw("Failed to read inlined data from DuckLake: ");
	}
	auto context = transaction.context.lock();
	auto data = make_uniq<ColumnDataCollection>(*context, expected_types);
	DataChunk reinterpret_chunk;
	reinterpret_chunk.Initialize(*context, expected_types);
	while (true) {
		auto chunk = result.Fetch();
		if (!chunk) {
			break;
		}
		for (idx_t i = 0; i < expected_types.size(); i++) {
			reinterpret_chunk.data[i].Reinterpret(chunk->data[i]);
		}
		reinterpret_chunk.SetCardinality(chunk->size());
		data->Append(reinterpret_chunk);
	}
	auto inlined_data = make_shared_ptr<DuckLakeInlinedData>();
	inlined_data->data = std::move(data);
	return inlined_data;
}

} // namespace duckdb
