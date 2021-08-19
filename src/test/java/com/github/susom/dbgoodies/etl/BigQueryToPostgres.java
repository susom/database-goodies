/*
 * Copyright 2021 The Board of Trustees of The Leland Stanford Junior University.
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

import com.github.susom.database.DatabaseProvider;

/*
   To run this in IntelliJ be sure to select the "bigquery" and "postgresql" profiles in Maven
   to make sure the JDBC drivers and BigQuery-related libraries are on your classpath.

   The BigQuery drivers aren't distributed as a proper Maven artifact, so I used this script
   from Joe to create the artifact locally:

   https://github.com/susom/cohort-cr-cloud/blob/master/starr-tools/etc/bq-jdbc-install.sh
 */
public class BigQueryToPostgres {
  public static void main(String[] args) {
    DatabaseProvider.fromDriverManager(
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=som-irt-scci-dev;DefaultDataset=garrick_test;OAuthType=3;Timeout=120;"
    ).transact(bq -> {
      // Test that BigQuery connection works
      System.out.println("String result: " + bq.get().toSelect("select dt from imported_table").queryStringOrEmpty());

      // Save a table from BigQuery into Postgres local created by:
      // docker run -d --name goodies-pg96-db -v goodies-pg96-data:/var/lib/postgresql/data -e POSTGRES_PASSWORD=... -p 5432:5432/tcp postgres:9.6
      DatabaseProvider.fromDriverManager(
          "jdbc:postgresql://localhost/postgres",
          "postgres",
          "..."
      ).withTransactionControl().transact(pg -> {
        Etl.saveQuery(bq.get().toSelect("select * from imported_table")).asTable(pg, "imported_table").dropAndCreateTable().start();

        // See if our rows made it
        System.out.println("Row count in Postgres: " + pg.get().toSelect("select count(*) from imported_table").queryLongOrNull());
      });
    });
  }
}