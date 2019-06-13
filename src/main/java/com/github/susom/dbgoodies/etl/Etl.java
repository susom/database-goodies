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
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

/**
 * Utility class for copying data and tables around in various ways.
 *
 * @author garricko
 * @author dbalraj
 * @author wencheng
 * @author jmesterh
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

    @CheckReturnValue
    public SaveAsBigQuery asBigQuery(String projectId, String datasetName, String tableName, String entryIdFields) {
      return new SaveAsBigQuery(projectId, datasetName, tableName, entryIdFields, select);
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
    private CodecFactory codec;
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

        Etl.Builder builder = new Etl.Builder(schemaName, tableName, rs);
        DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(builder.schema()))
            .setCodec(codec == null ? CodecFactory.nullCodec() : codec)
            .create(builder.schema(), new File(filename));
        log.debug("Using schema: \n" + builder.schema().toString(true));

        try {
          while (rs.next()) {
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
     * Saves a table as multiple Avro files, returning a list of the files created.
     * @param rowsPerFile how many rows per avro file
     * @return paths to created avro files
     */
    public List<String> start(long rowsPerFile) {
      List<String> files = new ArrayList<>();
      select.fetchSize(fetchSize).query(rs -> {
        int fileNo = 0;

        File avroFile = new File(getFilename(fileNo));
        files.add(avroFile.getAbsolutePath());

        Etl.Builder builder = new Etl.Builder(schemaName, tableName, rs);
        DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(
            new GenericDatumWriter<>(builder.schema()))
            .setCodec(codec == null ? CodecFactory.nullCodec() : codec)
            .create(builder.schema(), avroFile);
        log.debug("Using schema: \n" + builder.schema().toString(true));

        try {
          long rowCount = 0;
          while (rs.next()) {
            writer.append(builder.read(rs));
            if (rowsPerFile > 0 && (++rowCount > rowsPerFile)) {
              writer.close();
              avroFile = new File(getFilename(++fileNo));
              rowCount = 0;
              writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(builder.schema()))
                  .setCodec(codec == null ? CodecFactory.nullCodec() : codec)
                  .create(builder.schema(), avroFile);
              files.add(avroFile.getAbsolutePath());
            }
          }
        } finally {
          if (writer != null) {
            writer.close();
          }
        }
        return null;
      });
      return files;
    }

    /*
     Replaces %{PART} with the given file number, or appends it if %{PART} is not found
     */
    private String getFilename(int fileNo) {
      StringBuilder path = new StringBuilder();
      if (filename.contains("%{PART}")) {
        path.append(filename.replace("%{PART}", String.format("%03d", fileNo)));
      } else {
        int avroAt = filename.indexOf(".avro");
        if (avroAt > 0) {
          path.append(filename, 0, avroAt).append(String.format("-%03d", fileNo)).append(".avro");
        } else {
          path.append(filename).append(String.format("-%03d", fileNo));
        }
      }
      return path.toString();
    }

    /**
     * Allow control of the number of rows to be read at one time by the
     * underlying JDBC driver (if supported). Default value is 100,000 rows.
     */
    @CheckReturnValue
    public SaveAsAvro fetchSize(int nbrRows) {
      fetchSize = nbrRows;
      return this;
    }

    /**
     * Allow setting a custom codec
     */
    @CheckReturnValue
    public SaveAsAvro withCodec(CodecFactory codec) {
      this.codec = codec;
      return this;
    }
  }


  /**
   * BigQuery support.
   * by Wencheng
   */
  public static class SaveAsBigQuery {
    private final String projectId;
    private final String datasetName;
    private final String tableName;
    private final String entryIdFields;
    private final SqlSelect select;
    private int fetchSize = 100000;
    private int workerNumber = 1;
    private int batchSize = 500;
    private Map<String, String> labels;

    SaveAsBigQuery(String projectId, String datasetName, String tableName, String entryIdFields, SqlSelect select) {
      this.projectId = projectId;
      this.datasetName = datasetName;
      this.tableName = tableName;
      this.entryIdFields = entryIdFields;
      this.select = select;
    }

    /**
     * create a BigQueryWriter and hook up with Database select callback. when query finish, signal writer to turn off workers
     * <p>
     * the writer uses BigQuery stream api, which requires unique object id for each row. EntryIdFields is important, you should use primary key field list for this. BigQueryWriter will take this comma separated string input and compute object id for each row when upload to BigQuery
     * <p>
     * requres Google Client Credential file properly set up in env
     */
    public void start() throws Exception {
      if(this.labels == null){
        labels = new HashMap<>();
      }

      Optional<String> google_credential_file = Optional.ofNullable(System.getenv("GOOGLE_APPLICATION_CREDENTIALS")) ;

      BigQueryWriter.Builder builder = new BigQueryWriter.Builder()
              .withBigqueryProjectId(this.projectId)
              .withDataset(this.datasetName)
              .withTableName(this.tableName)
              .withEntryIdFields(this.entryIdFields.split(","))
              .withUploadBatchSize(this.batchSize)
              .withUploadThread(this.workerNumber)
              .withLabels(labels);

      google_credential_file.ifPresent(builder::withBigQueryCredentialFile);

      BigQueryWriter<Row> bqWriter = builder.build();

      CountDownLatch readerCompletedSignal = new CountDownLatch(1);
      bqWriter.setupWriter(readerCompletedSignal);
      select.fetchSize(fetchSize).query(bqWriter.databaseRowsHandler());
      readerCompletedSignal.countDown();
    }

    /**
     * configure database fetch size
     */
    @CheckReturnValue
    SaveAsBigQuery fetchSize(int nbrRows) {
      this.fetchSize = nbrRows;
      return this;
    }

    /**
     * configure number of upload workers
     */
    @CheckReturnValue
    SaveAsBigQuery workerNumber(int workerNum) {
      this.workerNumber = workerNum;
      return this;
    }

    /**
     * configure batch size of each BigQuery insert
     */
    @CheckReturnValue
    SaveAsBigQuery batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * add labels to BigQuery table if table gets created in this run. The entire map will be replaced by the provided new map.
     */
    @CheckReturnValue
    SaveAsBigQuery withLabels(Map<String, String> labels) {
      this.labels = labels;
      return this;
    }
    /**
     * add label in accumulative way
     */
    @CheckReturnValue
    SaveAsBigQuery addLabel(String name, String value ) {
      if(this.labels == null){
        this.labels = new HashMap<>();
      }
      this.labels.put(name,value);
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
          //For each row fetched by the query
          //Column names
          names[i] = metadata.getColumnLabel(i + 1);
          //Column data types
          types[i] = metadata.getColumnType(i + 1);
          //Column precision (applicable only for NUMBER and FLOAT data types)
          precision[i] = metadata.getPrecision(i + 1);
          //Column scale (applicable only for NUMBER and FLOAT data types)
          //metadata.getScale() is always returning -127 for NUMBER and FLOAT columns
          //Oracle database returns -127 if scale is unspecified for the column
          //Therefore if the scale is unspecified for the column, set it to the appropriate value based on the table schema
          scale[i] = metadata.getScale(i + 1);
          if (precision[i] == 126 && scale[i] == -127) {
            //FLOAT(126) column in Oracle; therefore no scale specified in the column data type
            //in GCP BigQuery, the NUMERIC data type is an exact numeric value with 38 digits of precision and 9 decimal digits of scale
            //Precision is the number of digits that the number contains
            //Scale is how many of these digits appear after the decimal point
            precision[i] = 38;
            scale[i] = 9;
          }
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
          //For each column in the table
          //Specify the schema in the AVRO file based on the column data type
          switch (types[i]) {
            case Types.TINYINT:
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
              //These are the columns with NUMBER and FLOAT data types in Oracle
              //Specific data types are:
              //  FLOAT(126), NUMBER and NUMBER(38) in pediatric side and
              //  FLOAT(126), NUMBER(18), NUMBER(38), NUMBER(12,2), NUMBER(18,2) in adult side
              //For pediatric side -->
              //  PRECISION = 0; SCALE = -127 for NUMBER
              //  PRECISION = 38; SCALE = 0 for NUMBER(38)
              //  PRECISION = 38; SCALE = 9 for FLOAT(126)
              //For adult side -->
              //  PRECISION = 0; SCALE = -127 for NUMBER
              //  PRECISION = 18; SCALE = 0 for NUMBER(18)
              //  PRECISION = 38; SCALE = 0 for NUMBER(38)
              //  PRECISION = 12; SCALE = 2 for NUMBER(12,2)
              //  PRECISION = 18; SCALE = 2 for NUMBER(18,2)
              //  PRECISION = 38; SCALE = 9 for FLOAT(126)
              //log.warn("\n Column with type NUMERIC = " + names[i] + "; PRECISION = " + precision[i] + "; SCALE = " + scale[i]);
              if (precision[i] == 0 && scale[i] == -127) {
                //NUMBER data type in Oracle
                //This was first set as an INTEGER but later changed it to LONG because of Numeric Overflow exception in Java JDBC
                //Integer in java is 4 bytes / 32 bits
                //Long in Java is 8 bytes / 64 bits
                fields.add(new org.apache.avro.Schema.Field(names[i],
                        //org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.INT)),
                        org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.LONG)),
                        null, Field.NULL_VALUE));
              } else if (precision[i] != 0 && scale[i] == 0) {
                //NUMBER(18), NUMBER(38) data types in Oracle
                //Long in Java is 8 bytes / 64 bits
                fields.add(new org.apache.avro.Schema.Field(names[i],
                        org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.LONG)),
                        null, Field.NULL_VALUE));
              } else if (precision[i] != 0 && scale[i] != 0) {
                //NUMBER(12,2), NUMBER(18,2), FLOAT(126) data types in Oracle
                //org.apache.avro.Schema bytes = org.apache.avro.Schema.create(Type.BYTES);
                //bytes.addProp("logical_type", "decimal");
                //bytes.addProp("precision", precision[i]); //38 or 12 or 18
                //bytes.addProp("scale", scale[i]); //9 or 2
                //Setting the above in an alternate way
                org.apache.avro.Schema bytes = LogicalTypes.decimal(precision[i], scale[i]).addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES));
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
              date.addProp(LogicalType.LOGICAL_TYPE_PROP, LogicalTypes.timestampMillis().getName());
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
      //FYI - timestamps or dates in Oracle are represented as a long number of milliseconds from the Unix epoch, 1 January 1970 00:00:00.000 UTC
      //FYI - decimals are encoded as a sequence of bytes containing the two's complement representation of the unscaled integer value in big-endian byte order
      //      the decimal fields, in particular, look a bit strange in their JSON representation, but rest assured that the data is stored in full fidelity in the actual AVRO encoding!
      GenericRecord record = new GenericData.Record(schema());

      for (int i = 0; i < names.length; i++) {
        switch (types[i]) {
          case Types.TINYINT:
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
            //These are the columns with NUMBER and FLOAT data types in Oracle
            //Specific data types are:
            //  FLOAT(126), NUMBER and NUMBER(38) in pediatric side
            //  FLOAT(126), NUMBER(18), NUMBER(38), NUMBER(12,2), NUMBER(18,2) in adult side and
            //For pediatric side -->
            //  PRECISION = 0; SCALE = -127 for NUMBER
            //  PRECISION = 38; SCALE = 0 for NUMBER(38)
            //  PRECISION = 38; SCALE = 9 for FLOAT(126)
            //For adult side -->
            //  PRECISION = 0; SCALE = -127 for NUMBER
            //  PRECISION = 18; SCALE = 0 for NUMBER(18)
            //  PRECISION = 38; SCALE = 0 for NUMBER(38)
            //  PRECISION = 12; SCALE = 2 for NUMBER(12,2)
            //  PRECISION = 18; SCALE = 2 for NUMBER(18,2)
            //  PRECISION = 38; SCALE = 9 for FLOAT(126)
            //log.warn("\n Column with type NUMERIC = " + names[i] + "; PRECISION = " + precision[i] + "; SCALE = " + scale[i]);
            if (precision[i] == 0 && scale[i] == -127) {
              //NUMBER data type in Oracle
              //This was first set as an INTEGER but later changed it to LONG because of Numeric Overflow exception in Java JDBC
              //Integer in java is 4 bytes / 32 bits
              //Long in Java is 8 bytes / 64 bits
              //record.put(names[i], r.getIntegerOrNull());
              record.put(names[i], r.getLongOrNull());
            } else if (precision[i] != 0 && scale[i] == 0) {
              //NUMBER(18), NUMBER(38) data types in Oracle
              //Long in Java is 8 bytes / 64 bits
              record.put(names[i], r.getLongOrNull());
            } else if (precision[i] != 0 && scale[i] != 0) {
              //NUMBER(12,2), NUMBER(18,2), FLOAT(126) data types in Oracle
              //Use a BigDecimal in Java for these types of columns
              BigDecimal bigDecimalOrNull = r.getBigDecimalOrNull();
              if (bigDecimalOrNull == null) {
                record.put(names[i], null);
              } else {
                //Use either RoundingMode.DOWN or RoundingMode.FLOOR to truncate a BigDecimal without rounding
                bigDecimalOrNull = bigDecimalOrNull.setScale(scale[i], RoundingMode.DOWN);
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
