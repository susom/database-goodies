/*
 * Copyright 2018 The Board of Trustees of The Leland Stanford Junior University.
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
package com.github.susom.dbgoodies.test;

import com.github.susom.database.ConfigFrom;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.DatabaseProvider.Builder;
import com.github.susom.database.Metric;
import java.util.List;
import com.github.susom.dbgoodies.etl.Etl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple example of how to copy all the tables from source to destination. The source
 * and the target are different databases.
 */
public class EtlCopyTables {
  private static final Logger log = LoggerFactory.getLogger(EtlCopyTables.class);

  // List of tables to be copied
  private static List<String> tables = null;
  // Table index in the table list
  private static int i = 0;

  public static void main(String[] args) {
    Metric metric = new Metric(true);

    // We use postgres here because hsqldb doesn't seem to handle the
    // volume of data well. If you want to use hsqldb, reduce j below.
    Builder dbb = DatabaseProvider.fromDriverManager(ConfigFrom.firstOf()
        .systemProperties()
        .defaultPropertyFiles()
        .excludePrefix("database.")
        .removePrefix("oracle.").get())
        // The ETL utilities do incremental commits in order to handle large tables
        .withTransactionControl();

    // Get all tables from the source database
    dbb.transact(dbs -> {
      tables = dbs.get().toSelect("select table_name from user_tables").queryStrings();
    });

    log.info("Number of tables to copy:" + tables.size());

    Builder dbb2 = DatabaseProvider.fromDriverManager(ConfigFrom.firstOf()
        .systemProperties()
        .defaultPropertyFiles()
        .excludePrefix("database.")
        .removePrefix("postgres.").get())
        // The ETL utilities do incremental commits in order to handle large tables
        .withTransactionControl();

    // iterate over all the "tables" and copy to the target database
    for (i = 0; i < tables.size(); i++) {
      dbb2.transact(dbs2 ->
          dbb.transact(dbs ->
              Etl.saveQuery(dbs.get().toSelect(String.format("select * from %s", tables.get(i))))
                  .asTable(dbs2, tables.get(i))
                  .dropAndCreateTable()
                  .start()
          )
      );
    }
    metric.checkpoint("copy");

    // Check the results
    dbb2.transact(dbs ->
        log.info("Tables copied to destination: " + dbs.get()
            .toSelect("select count(*) from information_schema.tables where table_schema='public'")
            .queryIntegerOrZero() + " in " + metric.getMessage())
    );
  }
}
