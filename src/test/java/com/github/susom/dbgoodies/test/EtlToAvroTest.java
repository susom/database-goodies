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

import com.github.susom.database.Database;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.OptionsOverride;
import com.github.susom.database.Schema;
import com.github.susom.dbgoodies.etl.Etl;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests to verify saving query results to Avro works correctly. The tests
 * are in this abstract base class, and get executed by subclasses named and
 * configured for a specific vendor database.
 */
public abstract class EtlToAvroTest {
  private DatabaseProvider dbp;
  protected Database db;
  private Date now = new Date();

  @Before
  public void setupJdbc() {
    dbp = createDatabaseProvider(new OptionsOverride() {
      @Override
      public Date currentDate() {
        return now;
      }

      @Override
      public Calendar calendarForTimestamps() {
        return Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles"));
      }
    });
    db = dbp.get();
    db.dropTableQuietly("dbtest");
  }

  abstract DatabaseProvider createDatabaseProvider(OptionsOverride options);

  @After
  public void closeJdbc() {
    if (dbp != null) {
      dbp.commitAndClose();
    }
  }

  @Test
  public void saveAllTypesAsAvro() {
    new Schema().addTable("dbtest")
        .addColumn("nbr_integer").asInteger().primaryKey().table()
        .addColumn("nbr_long").asLong().table()
        .addColumn("nbr_float").asFloat().table()
        .addColumn("nbr_double").asDouble().table()
        .addColumn("nbr_big_decimal").asBigDecimal(19, 9).table()
        .addColumn("str_varchar").asString(80).table()
        .addColumn("str_fixed").asStringFixed(1).table()
        .addColumn("str_lob").asClob().table()
        .addColumn("bin_blob").asBlob().table()
        .addColumn("boolean_flag").asBoolean().table()
        .addColumn("date_millis").asDate().schema().execute(db);

    db.toInsert("insert into dbtest (nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar,"
        + " str_fixed, str_lob, bin_blob, boolean_flag, date_millis) values (?,?,?,?,?,?,?,?,?,?,?)")
        .argInteger(Integer.MAX_VALUE).argLong(Long.MAX_VALUE).argDouble((double) Float.MAX_VALUE)
        .argDouble(Double.MAX_VALUE).argBigDecimal(new BigDecimal("123.456"))
        .argString("hello").argString("Z").argClobString("hello again")
        .argBlobBytes(new byte[] { '1', '2' }).argBoolean(true).argDateNowPerApp().insert(1);

    db.toInsert("insert into dbtest (nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar,"
        + " str_fixed, str_lob, bin_blob, boolean_flag, date_millis) values (?,?,?,?,?,?,?,?,?,?,?)")
        .argInteger(Integer.MIN_VALUE).argLong(Long.MIN_VALUE).argDouble(0.000001d)
        .argDouble(Double.MIN_VALUE).argBigDecimal(new BigDecimal("-123.456"))
        .argString("goodbye").argString("A").argClobString("bye again")
        .argBlobBytes(new byte[] { '3', '4' }).argBoolean(false).argDateNowPerApp().insert(1);

    Etl.saveQuery(db.toSelect("select * from dbtest")).asAvro("target/dbtest.avro", null, "dbtest").start();
  }

  @Test
  public void nullInteger() {
    new Schema()
        .addTable("dbtest")
        .addColumn("pk").primaryKey().table()
        .addColumn("nbr_integer").asInteger().schema().execute(db);

    db.toInsert("insert into dbtest values (?,?)").argLong(1L).argInteger(null).insert(1);

    Etl.saveQuery(db.toSelect("select * from dbtest")).asAvro("target/dbtest2.avro", null, "dbtest").start();
  }

  @Test
  public void nullValues() {
    new Schema()
        .addTable("dbtest")
        .addColumn("pk").primaryKey().table()
        .addColumn("nbr_integer").asInteger().table()
        .addColumn("nbr_long").asLong().table()
        .addColumn("nbr_float").asFloat().table()
        .addColumn("nbr_double").asDouble().table()
        .addColumn("nbr_big_decimal").asBigDecimal(19, 9).table()
        .addColumn("str_varchar").asString(80).table()
        .addColumn("str_fixed").asStringFixed(1).table()
        .addColumn("str_lob").asClob().table()
        .addColumn("bin_blob").asBlob().table()
        .addColumn("date_millis").asDate().table().schema().execute(db);

    db.toInsert("insert into dbtest values (?,?,?,?,?,?,?,?,?,?,?)").argLong(1L).argInteger(null).argLong(null)
        .argFloat(null).argDouble(null).argBigDecimal(null).argString(null).argString(null).argClobString(null)
        .argBlobBytes(null).argDate(null).insert(1);

    Etl.saveQuery(db.toSelect("select * from dbtest")).asAvro("target/dbtest3.avro", null, "dbtest").start();
  }
}
