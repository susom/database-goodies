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
import com.github.susom.database.Schema;
import com.github.susom.database.SqlInsert;
import com.github.susom.dbgoodies.etl.Etl;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple example of how to copy tables between databases. The source
 * and destination could be the same database, or different databases (of different
 * types if desired).
 */
public class EtlCopyTableSample {
  private static final Logger log = LoggerFactory.getLogger(EtlCopyTableSample.class);

  public static void main(String[] args) {
    Metric metric = new Metric(true);

    // We use postgres here because hsqldb doesn't seem to handle the
    // volume of data well. If you want to use hsqldb, reduce j below.
    Builder dbb = DatabaseProvider.fromDriverManager(ConfigFrom.firstOf()
        .systemProperties()
        .defaultPropertyFiles()
        .excludePrefix("database.")
        .removePrefix("postgres.").get())
        // The ETL utilities do incremental commits in order to handle large tables
        .withTransactionControl();

    Builder dbb2 = DatabaseProvider.fromDriverManager(ConfigFrom.firstOf()
        .systemProperties()
        .defaultPropertyFiles()
        .excludePrefix("database.")
        .removePrefix("postgres.").get())
        // The ETL utilities do incremental commits in order to handle large tables
        .withTransactionControl();

    // Insert lots of made up data into a source table
    dbb.transact(dbs -> {
      dbs.get().dropTableQuietly("copy_source");
      metric.checkpoint("dropSource");
      new Schema().addTable("copy_source")
          .addColumn("id").primaryKey().table()
          .addColumn("random_number").asLong().schema().execute(dbs);
      Random random = new Random(27);
      for (int i = 0; i <= 1000; i++) {
        SqlInsert insert = dbs.get().toInsert("insert into copy_source (id, random_number) values (?,?)");
        for (int j = 1; j <= 100000; j++) {
          insert.argInteger(i * 100000 + j).argLong(random.nextLong()).batch();
        }
        insert.insertBatch();
        dbs.get().commitNow();
      }
    });
    metric.checkpoint("createSource");

    // Copy it to another table
    dbb2.transact(dbs2 ->
        dbb.transact(dbs ->
            Etl.saveQuery(dbs.get().toSelect("select id, random_number from copy_source"))
                .asTable(dbs2, "copy_destination")
                .dropAndCreateTable()
                .start()
        )
    );
    metric.checkpoint("copy");

    // Check the results
    dbb.transact(dbs ->
        log.info("Rows in destination: " + dbs.get()
            .toSelect("select count(*) from copy_destination")
            .queryIntegerOrZero() + " " + metric.getMessage())
    );
  }
}
