/**
 * Core read workloads (from CoreReadsSuiteCapture).
 * Run: ./bin/generate-workload.sh examples/core_reads.scala
 */
import io.delta.workload.WorkloadGenerator._

workload("read_basic", "Basic read") { w =>
  w.sql("CREATE TABLE tbl (value INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("read_partitioned", "Partitioned read with filter") { w =>
  w.sql("CREATE TABLE tbl (id BIGINT, part INT) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "part = 0")
  w.read(t, predicate = "part = 3")
  w.snapshot(t)
}

workload("read_empty_path", "Error: no delta table", "error") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val logDir = dir.resolve("_delta_log")
    if (java.nio.file.Files.exists(logDir)) {
      java.nio.file.Files.walk(logDir).sorted(java.util.Comparator.reverseOrder())
        .forEach(p => java.nio.file.Files.deleteIfExists(p))
    }
  }
  w.read(t)
}

workload("read_append", "Read after append") { w =>
  w.sql("CREATE TABLE tbl (value INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
  w.sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("read_overwrite", "Read after overwrite") { w =>
  w.sql("CREATE TABLE tbl (value INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
  w.sql("INSERT OVERWRITE tbl SELECT id FROM range(100, 106)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("read_multiple_types", "Multiple data types") { w =>
  w.sql("""CREATE TABLE tbl (
    id INT, name STRING, score DOUBLE, active BOOLEAN,
    created DATE, updated TIMESTAMP
  ) USING delta""")
  w.sql("INSERT INTO tbl VALUES (1,'alice',95.5,true,DATE'2024-01-01',TIMESTAMP'2024-01-01 10:00:00')")
  w.sql("INSERT INTO tbl VALUES (2,'bob',82.3,false,DATE'2024-02-15',TIMESTAMP'2024-02-15 14:30:00')")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("read_predicate", "Predicate pushdown") { w =>
  w.sql("CREATE TABLE tbl (value INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "value > 5")
  w.snapshot(t)
}

workload("read_bad_version", "Error: non-existent version", "error") { w =>
  w.sql("CREATE TABLE tbl (value INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
  val t = w.table("tbl")
  w.read(t, version = 99)
  w.snapshot(t)
}

workload("read_version_zero", "Time travel to v0") { w =>
  w.sql("CREATE TABLE tbl (value INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
  w.sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
  w.sql("INSERT INTO tbl SELECT id FROM range(11, 16)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.snapshot(t)
}

workload("read_after_delete", "Read after DELETE") { w =>
  w.sql("CREATE TABLE tbl (value INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
  w.sql("DELETE FROM tbl WHERE value <= 3")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("read_after_update", "Read after UPDATE") { w =>
  w.sql("CREATE TABLE tbl (value INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
  w.sql("UPDATE tbl SET value = value + 100 WHERE value <= 5")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "value > 100")
  w.snapshot(t)
}

workload("read_after_merge", "Read after MERGE") { w =>
  w.sql("CREATE TABLE target (id INT, val STRING) USING delta")
  w.sql("INSERT INTO target VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("CREATE TABLE src (id INT, val STRING) USING delta")
  w.sql("INSERT INTO src VALUES (2,'updated'),(4,'new')")
  w.sql("""MERGE INTO target t USING src s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET val = s.val
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("target")
  w.read(t)
  w.snapshot(t)
}

workload("read_nulls", "Null values across types") { w =>
  w.sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE, active BOOLEAN) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,'alice',95.5,true)")
  w.sql("INSERT INTO tbl VALUES (2,null,null,null)")
  w.sql("INSERT INTO tbl VALUES (null,'charlie',88.0,false)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "name IS NOT NULL")
  w.snapshot(t)
}

workload("read_empty_partition", "Empty partition filter result") { w =>
  w.sql("CREATE TABLE tbl (id BIGINT, part INT) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl SELECT id, CAST(id % 3 AS INT) FROM range(50)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "part = 99")
  w.snapshot(t)
}

workload("read_nested_struct", "Nested struct columns") { w =>
  w.sql("""CREATE TABLE tbl (
    id INT, info STRUCT<name: STRING, age: INT, address: STRUCT<city: STRING, zip: STRING>>
  ) USING delta""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30,'address',named_struct('city','NYC','zip','10001')))")
  w.sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25,'address',named_struct('city','LA','zip','90001')))")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("read_array", "Array columns") { w =>
  w.sql("CREATE TABLE tbl (id INT, tags ARRAY<STRING>, scores ARRAY<INT>) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,array('a','b','c'),array(10,20,30))")
  w.sql("INSERT INTO tbl VALUES (2,array('x'),array(99))")
  w.sql("INSERT INTO tbl VALUES (3,array(),array())")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("read_map", "Map columns") { w =>
  w.sql("CREATE TABLE tbl (id INT, props MAP<STRING, STRING>) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,map('color','red','size','large'))")
  w.sql("INSERT INTO tbl VALUES (2,map('color','blue'))")
  w.sql("INSERT INTO tbl VALUES (3,map())")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("read_large_schema", "25 columns") { w =>
  val colDefs = (1 to 24).map(i => s"col_$i BIGINT").mkString(", ")
  w.sql(s"CREATE TABLE tbl (id BIGINT, $colDefs) USING delta")
  val colExprs = (1 to 24).map(i => s"id * $i AS col_$i").mkString(", ")
  w.sql(s"INSERT INTO tbl SELECT id, $colExprs FROM range(5)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("read_special_chars", "Special chars in partition values") { w =>
  w.sql("CREATE TABLE tbl (id INT, category STRING) USING delta PARTITIONED BY (category)")
  w.sql("INSERT INTO tbl VALUES (1,'hello world'),(2,'foo=bar'),(3,'a/b')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "category = 'hello world'")
  w.snapshot(t)
}

workload("read_schema_evolution", "ADD COLUMN", "schema_evolution") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
  w.sql("ALTER TABLE tbl ADD COLUMN name STRING")
  w.sql("INSERT INTO tbl VALUES (6,'alice'),(7,'bob')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "name IS NOT NULL")
  w.snapshotHistory(t)
}

workload("read_rename_column", "RENAME COLUMN", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
  w.sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
  w.sql("INSERT INTO tbl VALUES (3,'charlie')")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("read_decimal", "Decimal types") { w =>
  w.sql("CREATE TABLE tbl (id INT, price DECIMAL(10,2), ratio DECIMAL(18,8)) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,99.99,0.12345678),(2,1234.56,3.14159265),(3,0.01,0.00000001)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "price > 100")
  w.snapshot(t)
}

workload("read_projection", "Column projection") { w =>
  w.sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE, category STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,'alice',95.5,'A'),(2,'bob',82.3,'B'),(3,'charlie',91.0,'A')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("id", "name"))
  w.read(t, columns = Seq("score"))
  w.snapshot(t)
}

workload("read_binary", "Binary column") { w =>
  w.sql("CREATE TABLE tbl (id INT, data BINARY) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,X'48454C4C4F'),(2,X'574F524C44'),(3,X'')")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("read_negative_version", "Error: negative version", "error") { w =>
  w.sql("CREATE TABLE tbl (value INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
  val t = w.table("tbl")
  w.read(t, version = -1)
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
