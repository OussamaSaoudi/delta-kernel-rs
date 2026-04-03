# OSS Delta Spark Read-Path Test Analysis for improved_dat Spec Inspiration

## 1. Test File Inventory

### actions/ directory
| File | Test Count | Relevance |
|------|-----------|-----------|
| `AddFileSuite.scala` | N/A | Write-path, not relevant |
| `DeletionVectorDescriptorSuite.scala` | N/A | Already covered by DV specs |

### Domain Metadata
| File | Tests |
|------|-------|
| `DomainMetadataSuite.scala` | 13 tests |
| `DomainMetadataRemovalSuite.scala` | 4 tests |

**DomainMetadataSuite tests:**
- `DomainMetadata actions tracking in CRC should stop once threshold is crossed`
- `Validate crc can be read when domainMetadata is missing`
- `DomainMetadata action survives state reconstruction [w/o checkpoint, w/o checksum]`
- `DomainMetadata action survives state reconstruction [w/ checkpoint, w/ checksum]`
- `DomainMetadata action survives state reconstruction [w/ checkpoint, w/o checksum]`
- `DomainMetadata action survives state reconstruction [w/o checkpoint, w/ checksum]`
- `DomainMetadata deletion [w/o checkpoint, w/o checksum]`
- `DomainMetadata deletion [w/ checkpoint, w/o checksum]`
- `DomainMetadata deletion [w/o checkpoint, w/ checksum]`
- `DomainMetadata deletion [w/ checkpoint, w/ checksum]`
- `Multiple DomainMetadatas with the same domain should fail in single transaction`
- `Validate the failure when table feature is not enabled`
- `Validate the lifespan of metadata domains for the REPLACE TABLE operation`

**DomainMetadataRemovalSuite tests:**
- `DomainMetadata can be dropped`
- `Drop DomainMetadata feature when leaked domain metadata exist in the table`
- `Cannot drop DomainMetadata if there is a dependent feature on the table`
- `Drop domainMetadata after dropping a dependent feature`

### Protocol & Table Features
| File | Tests |
|------|-------|
| `DeltaProtocolVersionSuite.scala` | ~100 tests |
| `DeltaProtocolTransitionsSuite.scala` | 11 tests |
| `DeltaTableFeatureSuite.scala` | 18 tests |
| `ProtocolMetadataAdapterSuite.scala` | 8 parameterized tests |

**Key read-path DeltaProtocolVersionSuite tests:**
- `protocol for empty folder` - verifies protocol on fresh tables
- `access with protocol too high` - reads table with INT_MAX protocol, expects error
- `InvalidProtocolVersionException - error message with protocol too high - table path` - read fails with (4,7) then (3,8)
- `InvalidProtocolVersionException - error message with table name - warm/cold` - error via table name
- `DeltaUnsupportedTableFeatureException - error message - table path` - unknown reader/writer features
- `DeltaUnsupportedTableFeatureException - error message with table name - warm/cold`
- `Vacuum checks the write protocol` - unsupported writer feature blocks vacuum
- `Writes using X error out when unsupported writer features are present` (INSERT/UPDATE/DELETE/MERGE/CREATE OR REPLACE)

**Key DeltaProtocolTransitionsSuite tests:**
- `CREATE TABLE default protocol versions`
- `CREATE TABLE normalization`
- `ADD FEATURE normalization`
- `DROP FEATURE normalization`

**ProtocolMetadataAdapterSuite tests:**
- `assertTableReadable with readable table` / `with unsupported type widening`
- `isIcebergCompatAnyEnabled when ...`
- `isIcebergCompatGeqEnabled when ...`

### InCommitTimestamp
| File | Tests |
|------|-------|
| `InCommitTimestampSuite.scala` | ~20 tests |

**Key read-path tests:**
- `Enable ICT on commit 0` - verifies snapshot.timestamp == ICT
- `InCommitTimestamp is equal to snapshot.timestamp`
- `Missing CommitInfo should result in a DELTA_MISSING_COMMIT_INFO exception`
- `Missing CommitInfo.commitTimestamp should result in a DELTA_MISSING_COMMIT_TIMESTAMP exception`
- `CREATE OR REPLACE should not disable ICT`
- `Enablement tracking properties should not be added if ICT is enabled on commit 0`
- `snapshot.timestamp should be read from the CRC`
- `postCommitSnapshot.timestamp should be populated by protocolMetadataAndICTReconstruction when the table has no checkpoints`
- `snapshot.timestamp should be populated by protocolMetadataAndICTReconstruction during cold reads of checkpoints + deltas`
- `snapshot.timestamp cannot be populated by protocolMetadataAndICTReconstruction during cold reads of checkpoints`
- `snapshot.timestamp is read from file when CRC doesn't have ICT and the latest version has a checkpoint`

### Identity Columns
| File | Tests |
|------|-------|
| `IdentityColumnSuite.scala` | 16 tests |
| `IdentityColumnDMLSuite.scala` | 11 tests |
| `GenerateIdentityValuesSuite.scala` | 7 tests |

**Key read-path tests:**
- `reading table should not see identity column properties` - verifies schema has no identity metadata
- `compatibility` - reads table created with writer version 5 (old protocol + identity cols)
- `various configuration` - reads identity columns with various start/step configs
- `default configuration` - reads default identity column values
- `ctas/rtas does not produce an identity column` - CTAS output schema check
- `clone` - reads cloned identity table
- `restore - positive step / negative step / on partitioned table`

### Generated Columns
| File | Tests |
|------|-------|
| `GeneratedColumnSuite.scala` | ~50 tests |
| `GeneratedColumnCompatibilitySuite.scala` | 3 tests |

**Key read-path tests:**
- `reading from a Delta table should not see generation expressions` - schema verification across all read APIs
- `generated column metadata is not exposed in schema` - schema via batch, time travel, CDF, streaming
- `the generation expression constraint should support null values` - read after null insert
- `nullability mismatch fails at runtime` - read after legal write
- `test partition transform expressions end to end` - read partitioned gen col table
- `complex type extractors` - read gen cols on nested types
- `dbr 8_0` (compat suite) - reads old tables with gen cols

### Check Constraints
| File | Tests |
|------|-------|
| `CheckConstraintsSuite.scala` | ~20 tests |

**Key read-path tests:**
- `see constraints in table properties` - reads metadata after constraint operations
- `delta history for constraints` - reads history after ADD/DROP constraint
- `constraints with nulls` - reads data after complex null-aware constraint operations
- `complex constraints` - reads data with array/map constraints
- `constraint induced by varchar` - read with varchar type
- `drop table feature` - reads after dropping constraint feature

### Variant
| File | Tests |
|------|-------|
| `DeltaVariantSuite.scala` | ~25 tests |
| `DeltaVariantShreddingSuite.scala` | 10 tests |

**Key read-path tests:**
- `variant stable and preview features can be supported simultaneously and read` - read with both features
- `time travel with variant column works` - read version 0 with variant
- `Table with variant type can use CDF` - CDF read with variant
- `Existing table with variant type can enable CDF` - CDF read after ALTER TABLE
- `Variant can be used as a source for generated columns` - read gen col derived from variant
- `Variant respects Delta table IS NOT NULL constraints` - read with NOT NULL on variant
- `Variant respects Delta table CHECK constraints` - read with CHECK constraint on variant
- `column mapping works - name/id - pushVariantIntoScan` - read with column mapping + variant
- `Spark can read shredded table containing the shredding table feature` - read shredded variant data
- `column mapping with pushVariantIntoScan` - read shredded variant with column mapping

### TimestampNTZ
| File | Tests |
|------|-------|
| `DeltaTimestampNTZSuite.scala` | 5 tests |

**Key read-path tests:**
- `use TIMESTAMP_NTZ in a partition column` - read partitioned by NTZ
- `min/max stats collection should apply on TIMESTAMP_NTZ` - stats on NTZ column

### Iceberg Compat / UniForm
| File | Tests |
|------|-------|
| `IcebergCompatUtilsBase.scala` | 0 (trait only) |
| `uniform/UniFormE2EIcebergSuiteBase.scala` | 9 tests |
| `uniform/IcebergCompatV2EnableUniformByAlterTableSuiteBase.scala` | 8 tests |

**Key read-path tests:**
- `Basic Insert - compatVN` - read after insert with iceberg compat
- `CIUD - compatVN` - read after CRUD operations
- `Table with partition - compatVN` - read partitioned table
- `Nested struct schema test - compatVN` - read nested schema
- `Insert Partitioned Table - Multiple Partitions` - read multi-partition

---

## 2. Read-Path Tests with Parameters

### Domain Metadata - Read Path

| Test | Feature | Read Operation | Parameters | Existing Spec? |
|------|---------|---------------|------------|----------------|
| DM state reconstruction [w/o ckpt, w/o crc] | domain_metadata | snapshot.domainMetadata | checkpoint=false, checksum=false | dm_state_no_ckpt_no_crc |
| DM state reconstruction [w/ ckpt, w/ crc] | domain_metadata | snapshot.domainMetadata | checkpoint=true, checksum=true | (partial: dm_state_ckpt_no_crc) |
| DM deletion [all 4 combos] | domain_metadata | snapshot.domainMetadata after delete | checkpoint/checksum combos | dm_deletion_* (covered) |
| Validate crc without domainMetadata | domain_metadata | CRC read | crc has no domainMetadata field | dm_crc_without_domain_metadata |
| CRC threshold crossed | domain_metadata | snapshot.domainMetadata | >2 domains, CRC stops tracking | NO |
| Feature not enabled error | domain_metadata | commit with DM | no table feature | NO (error case) |
| Duplicate domain in txn | domain_metadata | commit | two same-domain DMs | NO (error case) |
| REPLACE TABLE lifespan | domain_metadata | DomainMetadataUtils | existing+new domains | dm_replace_table_lifespan |

### Protocol Version - Read Path

| Test | Feature | Read Operation | Parameters | Existing Spec? |
|------|---------|---------------|------------|----------------|
| Access with protocol too high | protocol | spark.read.format("delta").load | protocol=(INT_MAX, INT_MAX) | pv_err_001_protocol_too_high |
| Protocol (4,7) too high - table path | protocol | spark.read.format("delta").load | readerVersion=4, writerVersion=7 | pv_err_protocol_too_high |
| Protocol (3,8) too high - write path | protocol | spark.range(1).write | readerVersion=3, writerVersion=8 | partial |
| Unsupported reader features | table_features | spark.read.format("delta").load | NonExistingReaderFeature1,2 | pv_err_002_unsupported_feature |
| Unsupported writer features - write | table_features | spark.write | NonExistingWriterFeature1,2 | partial (write-only) |
| Unsupported writer features - INSERT | table_features | sql INSERT | unknown writer feature | NO |
| Unsupported writer features - UPDATE | table_features | sql UPDATE | unknown writer feature | NO |
| Unsupported writer features - DELETE | table_features | sql DELETE | unknown writer feature | NO |

### InCommitTimestamp - Read Path

| Test | Feature | Read Operation | Parameters | Existing Spec? |
|------|---------|---------------|------------|----------------|
| Enable ICT on commit 0 | ict | snapshot.timestamp | ICT from commit 0 | ict_basic |
| ICT == snapshot.timestamp | ict | snapshot.timestamp | verify equality | ict_timestamp_equals_snapshot |
| Missing CommitInfo | ict | snapshot.timestamp | commitInfo removed from log | NO |
| Missing commitTimestamp | ict | snapshot.timestamp | inCommitTimestamp=None | NO |
| ICT from CRC | ict | snapshot.timestamp | read from checksum file | NO |
| ICT from checkpoint+delta | ict | snapshot.timestamp | cold read, no CRC | NO |
| ICT from checkpoint only | ict | snapshot.timestamp | cold read, checkpoint only | NO |
| CREATE OR REPLACE keeps ICT | ict | snapshot.timestamp | CRAS after enabling ICT | NO |

### Identity Columns - Read Path

| Test | Feature | Read Operation | Parameters | Existing Spec? |
|------|---------|---------------|------------|----------------|
| Reading hides identity props | identity | spark.read / sql / stream | schema should not show identity metadata | ic_005_identity_not_exposed |
| Compatibility (writer v5) | identity | spark.read from old table | old protocol + identity cols | NO |
| Various config read | identity | sql SELECT * | start=1..10, step=1..5 | ic_001_various_config |
| CTAS no identity | identity | schema check | CTAS strips identity | ic_006_ctas_no_inherit |
| Restore + read | identity | time travel | restore to version with identity | ic_008/009/010 |

### Generated Columns - Read Path

| Test | Feature | Read Operation | Parameters | Existing Spec? |
|------|---------|---------------|------------|----------------|
| Read hides gen expressions | generated_columns | spark.read / sql / stream | schema should not show gen expr | NO (schema-level) |
| Gen col metadata not exposed | generated_columns | spark.read with time travel, CDF | multiple read APIs | NO |
| Null support in gen cols | generated_columns | sql SELECT * | gen col with null input | gc_null_expression_result |
| Partition transform e2e | generated_columns | sql SELECT * | year/month/day/hour transforms | gc_datetime (partial) |
| Nullability mismatch read | generated_columns | spark.read.table | NOT NULL gen col | NO |
| Gen col from variant source | generated_columns | sql SELECT * | variant -> int gen col | NO |

### Check Constraints - Read Path

| Test | Feature | Read Operation | Parameters | Existing Spec? |
|------|---------|---------------|------------|----------------|
| See constraints in properties | check_constraints | DESCRIBE DETAIL | multiple constraints | cc_002_show_tblproperties |
| Constraints with nulls | check_constraints | spark.read.table | null-aware constraints on nested data | NO |
| Complex constraints | check_constraints | table read | array/map constraints | cc_009_array_constraint (partial) |
| Constraint with builtin methods | check_constraints | INSERT then read | LENGTH(text) < 10 | cc_010_length_constraint |
| Constraint with nested parens | check_constraints | INSERT then read | compound with AND | cc_011_compound_constraint |
| Variant + CHECK constraint | check_constraints | INSERT then read | variant_get >= 0 | NO |

### Variant - Read Path

| Test | Feature | Read Operation | Parameters | Existing Spec? |
|------|---------|---------------|------------|----------------|
| Stable + preview features read | variant | spark.table.selectExpr | both features enabled | var_027_stable_preview_features |
| Time travel with variant | variant | spark.read.option("versionAsOf","0") | version=0, 100 rows | var_020_time_travel |
| Variant + CDF read | variant | table_changes() | insert, delete, update | NO |
| Variant + NOT NULL constraint | variant | read after constrained inserts | NOT NULL on variant | NO |
| Variant + CHECK constraint | variant | read after constrained inserts | variant_get >= 0 | NO |
| Variant + column mapping | variant | sql select | columnMapping=name/id | var_018_column_mapping |
| Variant + gen col | variant | sql select * | v::int as gen col | NO |
| Shredded variant read | variant_shredding | spark.read.format("delta").load | shredded typed_value fields | var_028_shredded_variant_read |
| Shredded + column mapping | variant_shredding | sql select | colMap=name + shredding | NO |

### TimestampNTZ - Read Path

| Test | Feature | Read Operation | Parameters | Existing Spec? |
|------|---------|---------------|------------|----------------|
| NTZ partition column | timestamp_ntz | read partitioned table | partitioned by NTZ | ntz_partition |
| NTZ stats collection | timestamp_ntz | stats verification | min/max on NTZ | ntz_stats |

---

## 3. Proposed New Specs

### Domain Metadata

| Proposed Spec ID | Source Test | Description | Priority |
|-----------------|------------|-------------|----------|
| `dm_crc_threshold` | DM CRC threshold crossed | Table with >N domains where CRC stops tracking them; verify read still works via log replay | Medium |
| `dm_feature_not_enabled_err` | Feature not enabled error | Table without domainMetadata feature has DM in log; verify error on read | Low (write-path error) |
| `dm_duplicate_domain_err` | Duplicate domain in txn | Log entry with two DomainMetadata of same domain; verify error | Low |

### Protocol Version / Table Features

| Proposed Spec ID | Source Test | Description | Priority |
|-----------------|------------|-------------|----------|
| `pv_err_003_reader_v4` | Protocol (4,7) too high | Table with readerVersion=4, writerVersion=7; verify read error | High |
| `pv_err_004_writer_v8` | Protocol (3,8) too high | Table with readerVersion=3, writerVersion=8; verify read error | High |
| `pv_err_005_multiple_unsupported_reader` | Unsupported reader features x2 | Table with two unknown reader features; verify error lists both | High |
| `pv_err_006_multiple_unsupported_writer` | Unsupported writer features x2 | Table with two unknown writer features; read should succeed (writer-only), write should fail | Medium |
| `pv_err_007_reader_maxint` | Protocol INT_MAX | Table with reader=MAX_INT, writer=MAX_INT; verify read error | Medium |

### InCommitTimestamp

| Proposed Spec ID | Source Test | Description | Priority |
|-----------------|------------|-------------|----------|
| `ict_missing_commit_info` | Missing CommitInfo exception | ICT-enabled table where a commit log has no CommitInfo action; verify error on snapshot.timestamp | High |
| `ict_missing_commit_timestamp` | Missing commitTimestamp | ICT-enabled table where CommitInfo has inCommitTimestamp=None; verify error | High |
| `ict_from_crc` | ICT from CRC | ICT-enabled table with CRC containing ICT; verify timestamp read without parsing log | Medium |
| `ict_checkpoint_plus_delta` | ICT from checkpoint+delta | ICT table with checkpoint at v0, delta at v1; cold read gets timestamp from delta | Medium |
| `ict_checkpoint_only` | ICT from checkpoint only | ICT table at checkpoint; must read ICT from commit file | Medium |
| `ict_create_or_replace_keeps` | CREATE OR REPLACE keeps ICT | CRAS on ICT table should keep ICT enabled; verify read | Low |

### Identity Columns

| Proposed Spec ID | Source Test | Description | Priority |
|-----------------|------------|-------------|----------|
| `ic_020_compat_old_writer` | Compatibility (writer v5) | Table created with old writer version 5 + identity cols; verify read succeeds and data correct | High |
| `ic_024_unsupported_col_type` | Unsupported column type | Identity column with unsupported type; verify error | Low |

### Generated Columns

| Proposed Spec ID | Source Test | Description | Priority |
|-----------------|------------|-------------|----------|
| `gc_metadata_not_exposed` | Gen col metadata not exposed in schema | Table with gen cols; read schema should not contain generation expressions | High |
| `gc_not_null_gen_col` | Nullability mismatch read | Gen col with NOT NULL + legal data; verify read returns correct values | Medium |
| `gc_partition_transform` | Partition transform e2e | Gen cols using year/month/day/hour transforms on timestamp; read partitioned data | Medium |
| `gc_variant_source` | Gen col from variant | Generated column `v::int` from variant column; read and verify | Medium |

### Check Constraints

| Proposed Spec ID | Source Test | Description | Priority |
|-----------------|------------|-------------|----------|
| `cc_021_null_aware` | Constraints with nulls | Table with IS NULL constraint on nested field; insert nulls at various nesting levels; read | High |
| `cc_022_map_constraint` | Complex constraints with map | Table with `nested.m[id] = id` constraint; verify read | Medium |
| `cc_023_variant_check` | Variant CHECK constraint | Table with `variant_get(v, '$', 'INT') >= 0` constraint; verify read after insert | High |
| `cc_024_nested_parens` | Constraint with nested parens | Compound constraint with nested parentheses; verify data readable | Low |

### Variant

| Proposed Spec ID | Source Test | Description | Priority |
|-----------------|------------|-------------|----------|
| `var_033_variant_cdf` | Variant + CDF | Table with variant + CDF enabled; read table_changes showing insert/delete/update_preimage/update_postimage | High |
| `var_034_variant_not_null` | Variant NOT NULL | Table with `v VARIANT NOT NULL`; read data after inserts | Medium |
| `var_035_variant_check_constraint` | Variant CHECK constraint | Table with CHECK constraint on variant_get; read with predicate | High |
| `var_036_variant_gen_col` | Variant as gen col source | Table with gen col `vInt = v::int`; read and verify both columns | High |
| `var_037_shredded_col_mapping` | Shredded + column mapping | Shredded variant with column mapping name; rename column; read | Medium |
| `var_038_variant_cdf_enable_later` | Existing variant enables CDF | ALTER TABLE to enable CDF on existing variant table; read changes | Medium |
| `var_039_variant_default_value` | Variant with DEFAULT | Variant column with DEFAULT expression; read rows with default | Low |

### Iceberg Compat (new coverage area)

| Proposed Spec ID | Source Test | Description | Priority |
|-----------------|------------|-------------|----------|
| `ibc_001_basic_insert_v1` | Basic Insert - compatV1 | Table with icebergCompatV1 enabled; insert and read | High |
| `ibc_002_basic_insert_v2` | Basic Insert - compatV2 | Table with icebergCompatV2 enabled; insert and read | High |
| `ibc_003_partitioned` | Table with partition | IcebergCompat + partitioned table; read | Medium |
| `ibc_004_nested_struct` | Nested struct schema | IcebergCompat with nested struct schema; read | Medium |

### Cross-Feature Interactions

| Proposed Spec ID | Source Test | Description | Priority |
|-----------------|------------|-------------|----------|
| `xf_001_variant_identity_gen` | Multiple features | Table with variant + identity + generated columns; read and verify all columns | Medium |
| `xf_002_ict_domain_metadata` | ICT + domain metadata | Table with both ICT and domain metadata; verify timestamp and domain reads | Medium |

---

## 4. Summary

### Coverage by Feature Area

| Feature | Existing Specs | Proposed New | Key Gaps |
|---------|---------------|--------------|----------|
| Domain Metadata | 14 | 3 | CRC threshold, error cases |
| Protocol/Features | 22 | 5 | Multiple unsupported features, reader v4/writer v8 |
| InCommitTimestamp | 6 | 6 | Missing commit info errors, CRC/checkpoint reads |
| Identity Columns | 16 | 2 | Old writer version compat |
| Generated Columns | 14 | 4 | Schema metadata exposure, variant source |
| Check Constraints | 20 | 4 | Null-aware, map, variant constraints |
| Variant | 18 | 7 | CDF, NOT NULL, CHECK, gen col source, shredded+colmap |
| TimestampNTZ | 6 | 0 | Well covered |
| Void | 5 | 0 | Well covered |
| Interval | 5 | 0 | Well covered |
| Iceberg Compat | 0 | 4 | Entirely uncovered |
| Cross-feature | 0 | 2 | New category |

### Top Priority New Specs (ordered)

1. **ict_missing_commit_info** - ICT error when CommitInfo absent
2. **ict_missing_commit_timestamp** - ICT error when timestamp field absent
3. **pv_err_003_reader_v4** - Protocol reader v4 error
4. **pv_err_005_multiple_unsupported_reader** - Multiple unknown reader features
5. **var_033_variant_cdf** - Variant + CDF read
6. **var_035_variant_check_constraint** - Variant with CHECK constraint
7. **var_036_variant_gen_col** - Variant as generated column source
8. **cc_021_null_aware** - Null-aware check constraints
9. **cc_023_variant_check** - Variant CHECK constraint (alt perspective)
10. **gc_metadata_not_exposed** - Generated column metadata stripped from read schema
11. **ibc_001_basic_insert_v1** - Iceberg compat v1 basic read
12. **ibc_002_basic_insert_v2** - Iceberg compat v2 basic read
13. **ic_020_compat_old_writer** - Identity column with old writer version
14. **ict_from_crc** - ICT read from CRC file
