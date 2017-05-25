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
import com.github.susom.database.Schema;
import com.github.susom.database.Sql;
import com.github.susom.database.SqlArgs;
import com.github.susom.database.SqlSelect;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.CheckReturnValue;
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
}
