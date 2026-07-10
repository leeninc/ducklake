//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/postgres_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

class PostgresMetadataManager : public DuckLakeMetadataManager {
public:
	explicit PostgresMetadataManager(DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<PostgresMetadataManager>(transaction);
	}

	bool TypeIsNativelySupported(const LogicalType &type) override;
	bool SupportsInlining(const LogicalType &type) override;
	bool SupportsAppender() const override {
		return false;
	}
	idx_t MaxIdentifierLength() const override {
		return 63;
	}

	string GetColumnTypeInternal(const LogicalType &type) override;
	shared_ptr<DuckLakeInlinedData> TransformInlinedData(QueryResult &result,
	                                                     const vector<LogicalType> &expected_types) override;

	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;

	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query) override;

	vector<DuckLakeGlobalStatsInfo> GetGlobalTableStats(DuckLakeSnapshot snapshot, TableIndex table_id) override;
	unique_ptr<QueryResult> ReadFileColumnStatsForTable(DuckLakeSnapshot snapshot, TableIndex table_id) override;

protected:
	string GetLatestSnapshotQuery() const override;
	string GenerateFileColumnStatsCTEBody(const CTERequirement &req, TableIndex table_id) override;
	string GetDataFileSource(TableIndex table_id) override;
	string GetDeleteFileSource(TableIndex table_id) override;
	string GetFileColumnStatsJoinSource(TableIndex table_id, idx_t column_field_index) override;
	string GetFilePartitionValueSource(TableIndex table_id) override;

private:
	unique_ptr<QueryResult> ExecuteQuery(DuckLakeSnapshot snapshot, string &query, string command);
	//! Wrap pure Postgres SQL so it runs server-side via postgres_query (which disables the
	//! ctid-range parallel scan of attached tables). The SQL is single-quote escaped for embedding;
	//! placeholders such as {METADATA_SCHEMA_ESCAPED} / {SNAPSHOT_ID} may remain - they are
	//! substituted when the wrapped query is executed through Query().
	string WrapPostgresQuery(const string &pg_sql) const;
};

} // namespace duckdb
