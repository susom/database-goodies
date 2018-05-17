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
import com.github.susom.database.DatabaseException;
import com.github.susom.database.Row;
import com.github.susom.database.Schema;
import com.github.susom.database.Sql;
import com.github.susom.database.SqlArgs;
import com.github.susom.database.SqlSelect;
import java.io.File;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
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
    public SaveAsAvro asAvro(String path, String schemaName, String tableName) {
      return new SaveAsAvro(path, schemaName, tableName, select);
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
    private final String filename;
    private final String schemaName;
    private final String tableName;
    private final SqlSelect select;
    private int fetchSize = 100000;

    SaveAsAvro(String filename, String schemaName, String tableName, SqlSelect select) {
      this.filename = filename;
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.select = select;
    }

    /**
     * Actually begin the database and file writing operations.
     */
    public void start() {
      select.fetchSize(fetchSize).query(rs -> {
        Builder builder = null;
        DataFileWriter<GenericRecord> writer = null;

        try {
          while (rs.next()) {
            if (builder == null) {
              builder = new Builder(schemaName, tableName, rs);
              writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(builder.schema()))
//                  .setCodec(CodecFactory.nullCodec())
                  .create(builder.schema(), new File(filename));
              log.debug("Using schema: \n" + builder.schema().toString(true));
            }

            writer.append(builder.read(rs));
          }
        } finally {
          if (writer != null) {
            writer.close();
          }
        }

        return null;
      });
    }

    /**
     * Allow control of the number of rows to be read at one time by the
     * underlying JDBC driver (if supported). Default value is 100,000 rows.
     */
    @CheckReturnValue
    SaveAsAvro fetchSize(int nbrRows) {
      fetchSize = nbrRows;
      return this;
    }
  }

  private static class Builder {
    private String[] names;
    private final int[] types;
    private final int[] precision;
    private final int[] scale;
    private org.apache.avro.Schema schema;
    private String schemaName;
    private String tableName;

    Builder(String schemaName, String tableName, Row r) {
      this.schemaName = schemaName;
      this.tableName = tableName;
      try {
        ResultSetMetaData metadata = r.getMetadata();
        int columnCount = metadata.getColumnCount();
        names = new String[columnCount];
        types = new int[columnCount];
        precision = new int[columnCount];
        scale = new int[columnCount];

        for (int i = 0; i < columnCount; i++) {
          names[i] = metadata.getColumnLabel(i + 1);
          types[i] = metadata.getColumnType(i + 1);
          precision[i] = metadata.getPrecision(i + 1);
          scale[i] = metadata.getScale(i + 1);
        }

        names = SqlArgs.tidyColumnNames(names);
      } catch (SQLException e) {
        throw new DatabaseException("Unable to retrieve metadata from ResultSet", e);
      }
    }

    org.apache.avro.Schema schema() {
      if (schema == null) {
        List<Field> fields = new ArrayList<>();

        for (int i = 0; i < names.length; i++) {
          switch (types[i]) {
          case Types.SMALLINT:
          case Types.INTEGER:
            fields.add(new org.apache.avro.Schema.Field(names[i],
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.INT)),
                null, Field.NULL_VALUE));
            break;
          case Types.BIGINT:
            fields.add(new org.apache.avro.Schema.Field(names[i],
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.LONG)),
                null, Field.NULL_VALUE));
            break;
          case Types.REAL:
          case 100: // Oracle proprietary it seems
            fields.add(new org.apache.avro.Schema.Field(names[i],
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.FLOAT)),
                null, Field.NULL_VALUE));
            break;
          case Types.DOUBLE:
          case 101: // Oracle proprietary it seems
            fields.add(new org.apache.avro.Schema.Field(names[i],
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.DOUBLE)),
                null, Field.NULL_VALUE));
            break;
          case Types.NUMERIC:
            if (precision[i] == 10 && scale[i] == 0) {
              // Oracle reports integer as numeric
              fields.add(new org.apache.avro.Schema.Field(names[i],
                  org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.INT)),
                  null, Field.NULL_VALUE));
            } else if (precision[i] == 19 && scale[i] == 0) {
              // Oracle reports long as numeric
              fields.add(new org.apache.avro.Schema.Field(names[i],
                  org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.LONG)),
                  null, Field.NULL_VALUE));
            } else {
              org.apache.avro.Schema bytes = org.apache.avro.Schema.create(Type.BYTES);
              bytes.addProp("logical_type", "decimal");
              bytes.addProp("precision", precision[i]);
              bytes.addProp("scale", scale[i]);
              fields.add(new org.apache.avro.Schema.Field(names[i],
                  org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), bytes),
                  null, Field.NULL_VALUE));
            }
            break;
          case Types.BINARY:
          case Types.VARBINARY:
          case Types.BLOB:
            fields.add(new org.apache.avro.Schema.Field(names[i],
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.BYTES)),
                null, Field.NULL_VALUE));
            break;
          case Types.CLOB:
          case Types.NCLOB:
            fields.add(new org.apache.avro.Schema.Field(names[i],
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.STRING)),
                null, Field.NULL_VALUE));
            break;
          case Types.TIMESTAMP:
            org.apache.avro.Schema date = org.apache.avro.Schema.create(Type.LONG);
            date.addProp("logical_type", "timestamp_millis");
            fields.add(new org.apache.avro.Schema.Field(names[i],
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), date),
                null, Field.NULL_VALUE));
            break;
          case Types.NVARCHAR:
          case Types.VARCHAR:
          case Types.CHAR:
          case Types.NCHAR:
            if (precision[i] >= 2147483647) {
              // Postgres seems to report clobs are varchar(2147483647)
              fields.add(new org.apache.avro.Schema.Field(names[i],
                  org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.STRING)),
                  null, Field.NULL_VALUE));
            } else {
              fields.add(new org.apache.avro.Schema.Field(names[i],
                  org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.STRING)),
                  null, Field.NULL_VALUE));
            }
            break;
          default:
            throw new DatabaseException("Don't know how to deal with column type: " + types[i]);
          }
        }

        schema = org.apache.avro.Schema.createRecord(tableName, null, schemaName, false, fields);
      }
      return schema;
    }

    @Nonnull
    GenericRecord read(Row r) {
      GenericRecord record = new GenericData.Record(schema());

      for (int i = 0; i < names.length; i++) {
        switch (types[i]) {
        case Types.SMALLINT:
        case Types.INTEGER:
          record.put(names[i], r.getIntegerOrNull());
          break;
        case Types.BIGINT:
          record.put(names[i], r.getLongOrNull());
          break;
        case Types.REAL:
        case 100: // Oracle proprietary it seems
          record.put(names[i], r.getFloatOrNull());
          break;
        case Types.DOUBLE:
        case 101: // Oracle proprietary it seems
          record.put(names[i], r.getDoubleOrNull());
          break;
        case Types.NUMERIC:
          if (precision[i] == 10 && scale[i] == 0) {
            // Oracle reports integer as numeric
            record.put(names[i], r.getIntegerOrNull());
          } else if (precision[i] == 19 && scale[i] == 0) {
            // Oracle reports long as numeric
            record.put(names[i], r.getLongOrNull());
          } else {
            BigDecimal bigDecimalOrNull = r.getBigDecimalOrNull();
            if (bigDecimalOrNull == null) {
              record.put(names[i], null);
            } else {
              if (bigDecimalOrNull.scale() != scale[i]) {
                //noinspection BigDecimalMethodWithoutRoundingCalled - shouldn't happen, allow exception to propagate
                bigDecimalOrNull = bigDecimalOrNull.setScale(scale[i]);
              }
              record.put(names[i], ByteBuffer.wrap(bigDecimalOrNull.unscaledValue().toByteArray()));
            }
          }
          break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.BLOB:
          byte[] bytesOrNull = r.getBlobBytesOrNull();
          record.put(names[i], bytesOrNull == null ? null : ByteBuffer.wrap(bytesOrNull));
          break;
        case Types.CLOB:
        case Types.NCLOB:
          record.put(names[i], r.getClobStringOrNull());
          break;
        case Types.TIMESTAMP:
          Date dateOrNull = r.getDateOrNull();
          record.put(names[i], dateOrNull == null ? null : dateOrNull.getTime());
          break;
        case Types.NVARCHAR:
        case Types.VARCHAR:
        case Types.CHAR:
        case Types.NCHAR:
          if (precision[i] >= 2147483647) {
            // Postgres seems to report clobs are varchar(2147483647)
            record.put(names[i], r.getClobStringOrNull());
          } else {
            record.put(names[i], r.getStringOrNull());
          }
          break;
        default:
          throw new DatabaseException("Don't know how to deal with column type: " + types[i]);
        }
      }

      return record;
    }
  }
}
