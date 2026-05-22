# lance-flink Flink SQL feature matrix

Feature universe compiled from Apache Paimon, Iceberg, Hudi, and Fluss Flink docs.
Implementation status reflects the current state of this repository.

| Feature | In lance-flink | Limitation |
|---|---|---|
| `CREATE CATALOG` | Yes | `LanceNamespaceCatalog` only. |
| `CREATE / DROP DATABASE` | Yes | Depends on namespace impl. |
| `USE CATALOG` / `USE DATABASE` | Yes | — |
| `SHOW DATABASES` / `SHOW TABLES` | Yes | `sys` not listed. |
| `DESCRIBE TABLE` | Yes | — |
| `CREATE TABLE` | Yes | `path` / namespace options required. |
| `CREATE TABLE LIKE` | Yes | Must use `EXCLUDING OPTIONS`. |
| `CREATE TABLE AS SELECT` | Yes | — |
| `DROP TABLE` | Yes | — |
| `TRUNCATE TABLE` | Yes | Batch only. |
| `PRIMARY KEY NOT ENFORCED` | Yes | No PK evolution. |
| `ALTER TABLE ADD COLUMN` | Yes | Nullable only, no `FIRST/AFTER`, no default. |
| `ALTER TABLE DROP COLUMN` | Yes | Non-PK only, metadata-only. |
| `ALTER TABLE RENAME COLUMN` | Yes | Non-PK only. |
| `INSERT INTO` (append) | Yes | — |
| `INSERT INTO` (upsert via PK) | Yes | Via `mergeInsert`. |
| `INSERT OVERWRITE` | Yes | Batch only. |
| `UPDATE` (row-level) | Yes | PK table only. |
| `DELETE` (row-level) | Yes | PK table only. |
| Batch `SELECT` | Yes | — |
| Streaming / continuous `SELECT` | Yes | Append-only; PK rejected; fragment removal fails job. |
| Projection pushdown | Yes | — |
| Filter pushdown `=`, `!=`, `<`, `<=`, `>`, `>=` | Yes | — |
| Filter pushdown `AND` | Yes | — |
| Filter pushdown `IS [NOT] NULL` | Yes | — |
| Limit pushdown | Yes | Per-reader budget. |
| `FOR SYSTEM_TIME AS OF <ts>` | Yes | Data tables only. |
| Dynamic time-travel options | Yes | `scan.version` / `scan.snapshot-id` / `scan.tag-name` / `scan.timestamp[-millis]`. |
| Sync lookup join | Yes | Needs scalar index or `allow-full-scan`; snapshot frozen at open. |
| `lookup.cache = NONE` | Yes | — |
| `lookup.cache = PARTIAL` | Yes | — |
| Per-query `OPTIONS()` hint | Yes | — |
| `$snapshots` metadata table | Yes | Read-only, HEAD. |
| `$tags` metadata table | Yes | Read-only. |
| `$branches` metadata table | Yes | Read-only. |
| `$fragments` metadata table | Yes | Read-only. |
| `$options` metadata table | Yes | Read-only. |
| `PARTITIONED BY` | No | Lance uses fragments. |
| Hidden / transform partitioning | No | — |
| `WATERMARK FOR ... AS ...` | No | — |
| Computed columns | No | — |
| Generated / metadata columns | No | — |
| `ALTER TABLE MODIFY COLUMN` type | No | — |
| `ALTER TABLE MODIFY COLUMN` nullability | No | — |
| `ALTER TABLE MODIFY COLUMN` position | No | — |
| `ALTER TABLE MODIFY COLUMN` comment | No | — |
| `ALTER TABLE RENAME TO` | No | Namespace impl rejects. |
| `ALTER TABLE SET / RESET TBLPROPERTIES` | No | — |
| `ALTER TABLE ADD / DROP PARTITION` | No | No partitioning. |
| `ALTER TABLE ADD / DROP CONSTRAINT` | No | — |
| Mixed-kind changes in one `ALTER` | No | Single kind per statement. |
| Dynamic partition overwrite | No | No partitioning. |
| `INSERT INTO PARTITION` | No | No partitioning. |
| `MERGE INTO` statement | No | Not in Flink 1.19 SQL. |
| Bucketing | No | — |
| Clustering / z-order on write | No | — |
| Snapshot tagging on commit | No | — |
| Filter pushdown `OR` | No | — |
| Filter pushdown `IN` / `NOT IN` | No | — |
| Filter pushdown `LIKE` | No | — |
| Filter pushdown `BETWEEN` | No | — |
| Filter pushdown function calls | No | — |
| Aggregate pushdown | No | — |
| `FOR VERSION AS OF` | No | — |
| Branch reads (`scan.branch`) | No | — |
| Incremental reads (between commits) | No | Blocked on lance-core JNI. |
| CDC / changelog reads | No | No `getDeletedRows()`. |
| Audit-log table | No | — |
| Watermark-based reads | No | — |
| Async lookup join | No | — |
| `lookup.cache = FULL` | No | — |
| `lookup.max-retries` | No | — |
| `$manifests` metadata table | No | — |
| `$partitions` metadata table | No | No partitioning. |
| `$audit_log` / `$changelog` | No | — |
| `CALL sys.compact` | No | — |
| `CALL sys.expire_snapshots` | No | — |
| `CALL sys.rollback_to_version` | No | — |
| `CALL sys.rollback_to_tag` | No | — |
| `CALL sys.create_tag` / `sys.delete_tag` | No | — |
| `CALL sys.create_branch` / `sys.delete_branch` | No | — |
| `CALL sys.create_index` / `sys.drop_index` | No | — |
| `CALL sys.list_indices` / `sys.optimize_indices` | No | — |
| `CALL sys.rewrite_manifests` | No | — |
| `CALL sys.remove_orphan_files` | No | — |
| `CALL sys.register_table` / `sys.migrate` | No | — |
| `CALL sys.merge_into` procedure | No | — |
| `CALL sys.create_savepoint` / `sys.rollback_to_savepoint` | No | — |
| `CALL sys.replace_tag` / `sys.expire_tags` | No | — |
| `CALL sys.rename_branch` / `sys.merge_branch` / `sys.fast_forward` | No | — |
| `CALL sys.expire_partitions` | No | No partitioning. |
| `CALL sys.rescale` / bucket rescale | No | No bucketing. |
| `CALL sys.repair` | No | — |
| Show / describe-history procedures | No | — |
| `CREATE INDEX` (scalar) via SQL | No | — |
| `CREATE INDEX` (vector) via SQL | No | — |
| Vector / KNN search via SQL | No | — |
| Materialized views | No | — |
| Snapshot retention at DDL | No | — |
| ACID multi-statement transactions | No | Per-commit only. |
| Encryption | No | — |
| Migrate from Hive / Iceberg | No | — |
