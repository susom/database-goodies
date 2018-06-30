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
import com.github.susom.database.OptionsOverride;
import com.github.susom.dbgoodies.etl.Etl;
import java.math.BigDecimal;
import org.junit.Test;

/**
 * Unit tests to verify saving query results to Avro works correctly for Oracle database.
 */
public class EtlToAvroOracleTest extends EtlToAvroTest {
  DatabaseProvider createDatabaseProvider(OptionsOverride options) {
    return DatabaseProvider.fromDriverManager(ConfigFrom.firstOf()
        .systemProperties()
        .defaultPropertyFiles()
        .excludePrefix("database.")
        .removePrefix("oracle.").get()
    ).withOptions(options).withSqlParameterLogging().withSqlInExceptionMessages().create();
  }

  @Test
  public void float126() {
    db.ddl("create table dbtest (nbr_float FLOAT(126))").execute();

    db.toInsert("insert into dbtest values (?)").argBigDecimal(new BigDecimal("123.4567")).insert(1);

    Etl.saveQuery(db.toSelect("select * from dbtest")).asAvro("target/dbtest_float126.avro", null, "dbtest").start();

    // TODO read the avro back in and check it
  }
}
