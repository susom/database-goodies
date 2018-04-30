/*
 * Copyright 2017 The Board of Trustees of The Leland Stanford Junior University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.susom.dbgoodies.etl;

import com.github.susom.database.Database;
import com.github.susom.database.Rows;
import com.github.susom.database.Schema;
import com.github.susom.database.Sql;
import com.github.susom.database.SqlArgs;
import com.github.susom.database.SqlSelect;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.CheckReturnValue;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for copying data and tables around in various ways.
 *
 * @author garricko
 */
public final class Etl {
  private static final Logger log = LoggerFactory.getLogger(Etl.class);

  @CheckReturnValue
  public static Save saveQuery(SqlSelect select) {
    return new Save(select);
  }

  // copyTable(), loadFile(), ...

  public static class Save {
    private final SqlSelect select;

    Save(SqlSelect select) {
      this.select = select;
    }

    @CheckReturnValue
    public SaveAsTable asTable(Supplier<Database> destination, String tableName) {
      return new SaveAsTable(destination, tableName, select);
    }

    @CheckReturnValue
    public SaveAsAvro asAvro(String path, String tableName) {
      return new SaveAsAvro(path, tableName, select);
    }
    // asCsv(), asTsv(), asExcel(), asJson(), asXml(), ...
  }

  public static class SaveAsTable {
    private final Supplier<Database> dbs;
    private final String tableName;
    private final SqlSelect select;
    private boolean dropTable;
    private boolean createTable;
    private int batchSize = 100000;
    private boolean batchCommit = true;
    private boolean alreadyLoggedTxWarning;

    SaveAsTable(Supplier<Database> dbs, String tableName, SqlSelect select) {
      this.dbs = dbs;
      this.tableName = tableName;
      this.select = select;
    }

    /**
     * Indicate you want the destination table to be created based on the
     * columns in the source. Otherwise it will assume the destination table
     * already exists, and will fail with an error if the table is missing.
     */
    @CheckReturnValue
    public SaveAsTable createTable() {
      createTable = true;
      return this;
    }

    /**
     * Indicate you want the destination table to be dropped (if it exists)
     * and created based on the columns in the source. If it does not exist
     * it silently proceeds to creating the table.
     */
    @CheckReturnValue
    public SaveAsTable dropAndCreateTable() {
      createTable = true;
      dropTable = true;
      return this;
    }

    /**
     * Control the size of batch inserts into the new table. By default batches
     * of 100,000 rows will be inserted and an explicit transaction commit will
     * occur after each batch.
     */
    @CheckReturnValue
    public SaveAsTable batchSize(int rows) {
      batchSize = rows;
      return this;
    }

    /**
     * Control whether explicit transaction commits will occur after each batch
     * of rows is inserted. By default commit will be called, but you can turn
     * it off with this method (so everything will occur in a single transaction).
     */
    @CheckReturnValue
    public SaveAsTable batchCommitOff() {
      batchCommit = false;
      return this;
    }

    /**
     * Actually begin the database operation.
     */
    public void start() {
      select.fetchSize(batchSize).query(rs -> {
        SqlArgs.Builder builder = null;
        List<SqlArgs> args = new ArrayList<>();

        while (rs.next()) {
          if (builder == null) {
            builder = SqlArgs.fromMetadata(rs);
            if (createTable) {
              if (dropTable) {
                dbs.get().dropTableQuietly(tableName);
              }
              new Schema().addTableFromRow(tableName, rs).schema().execute(dbs);
            }
          }
          args.add(builder.read(rs));
          if (args.size() >= batchSize) {
            dbs.get().toInsert(Sql.insert(tableName, args)).insertBatch();
            commitBatch();
            args.clear();
          }
        }

        if (args.size() > 0) {
          dbs.get().toInsert(Sql.insert(tableName, args)).insertBatch();
          commitBatch();
        }

        return null;
      });
    }

    private void commitBatch() {
      if (batchCommit) {
        if (dbs.get().options().allowTransactionControl()) {
          dbs.get().commitNow();
        } else if (!alreadyLoggedTxWarning) {
          log.warn("Not explicitly committing each batch of rows because you did not enable "
              + "transaction control on your database builder - see Builder.withTransactionControl().");
          alreadyLoggedTxWarning = true;
        }
      }
    }
  }

  public static class SaveAsAvro {
    private final String path;
    private final SqlSelect select;
    private final String tableName;
    private int batchSize = 100000;
    private final int TYPE_NONE = -99;

    SaveAsAvro(String path, String tableName, SqlSelect select) {
      this.path = path;
      this.tableName = tableName;
      this.select = select;
    }

    private void updateAvroRecord(GenericRecord record, final int type, Rows rs, String colName) {
      switch (type) {
        case Types.BOOLEAN:
          record.put(colName, Boolean.parseBoolean(rs.getStringOrNull(colName)));
          break;
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.NUMERIC:
          record.put(colName, rs.getIntegerOrNull(colName));
          break;
        case Types.BIGINT:
          record.put(colName, rs.getLongOrNull(colName));
          break;
        case Types.REAL:
        case 100:
          record.put(colName, rs.getFloatOrNull(colName));
          break;
        case Types.DOUBLE:
        case 101:
          record.put(colName, rs.getDoubleOrNull(colName));
          break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.BLOB:
          record.put(colName, rs.getBlobBytesOrNull(colName));
          break;
        case Types.CLOB:
        case Types.NCLOB:
          record.put(colName, rs.getClobStringOrEmpty(colName));
          break;
        case Types.TIMESTAMP:
          record.put(colName, rs.getDateOrNull(colName));
          break;
        case Types.NVARCHAR:
        case Types.VARCHAR:
        case Types.CHAR:
        case Types.NCHAR:
          record.put(colName, rs.getStringOrNull(colName));
          break;
        default:
          record.put(colName, rs.getStringOrNull(colName));
      }
    }

    /**
     * Actually begin the writing AVRO file.
     */
    public void start() {
      select.fetchSize(batchSize).query(rs -> {

        /* initialize susom database library elements */
        String[] names = null;
        ResultSetMetaData rsMeta = null;
        int type = TYPE_NONE;

        /* initialize apache AVRO library elements */
        String avroSchemaText = null;
        org.apache.avro.Schema schema = null;
        GenericRecord record = null;
        DatumWriter<GenericRecord> datumWriter = null;
        DataFileWriter<GenericRecord> dataFileWriter = null;
        AvroSchema avroSchema = null;

        /* initialize jaxson (json) library elements */
        ObjectMapper mapper = new ObjectMapper();
        while (rs.next()) {
          if (rsMeta == null) {
            /* Get table metadata */
            rsMeta = rs.getMetadata();

            /* Get column names */
            names = rs.getColumnLabels();

            /* define AVRO schema object */
            avroSchema = AvroSchema.getInstance();
            avroSchema.setName(tableName);

            /* Iterate through columns and add AVRO schema to object */
            for (int i = 0; i < names.length; i++) {
              AvroRecord recordJson = AvroRecord.getIntance();
              recordJson.setName(names[i]);
              type = rsMeta.getColumnType(i + 1);
              recordJson.getType().add(AvroRecord.getType(type));
              avroSchema.getFields().add(recordJson);
            }
            /* create avro json schema string */
            avroSchemaText = mapper.writeValueAsString(avroSchema);

            /* define AVRO schema */
            schema = new org.apache.avro.Schema.Parser().parse(avroSchemaText);
            log.debug("AvroSchema for table '%s': %s", tableName, avroSchemaText);

            /* add AVRO schema to records and avro file */
            datumWriter = new GenericDatumWriter<GenericRecord>(schema);
            dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
            dataFileWriter.create(schema, new File(path));
          }

          record = new GenericData.Record(schema);
          for (int i = 0; i < names.length; i++) {
            type = rsMeta.getColumnType(i + 1);
            /* Put the values into the avro record */
            updateAvroRecord(record, type, rs, names[i]);
          }
          /* Append record to avro file */
          dataFileWriter.append(record);
        }

        dataFileWriter.close();
        return null;
      });
    }
  }
}
