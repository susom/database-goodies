package com.github.susom.dbgoodies.etl;

import com.github.susom.database.ConfigFrom;
import com.github.susom.database.Database;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.OptionsOverride;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class SaveAsBigQueryTest  {

    DatabaseProvider dbp;
    Database db;
    @Before
    public void setUp() throws Exception {

        dbp = DatabaseProvider.fromDriverManager(ConfigFrom.firstOf()
                .systemProperties()
                .defaultPropertyFiles()
                .excludePrefix("database.")
                .removePrefix("hsqldb.").get()
        ).withOptions(new OptionsOverride() {
            @Override
            public Date currentDate() {
                return new Date();
            }

            @Override
            public Calendar calendarForTimestamps() {
                return Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles"));
            }
        }).withSqlParameterLogging().withSqlInExceptionMessages().create();

        db = dbp.get();
        db.dropTableQuietly("dbtest");
        db.ddl("create table dbtest (id int,nbr_float DECIMAL(10,4))").execute();
        db.toInsert("insert into dbtest values (?,?)")
                .argInteger(1)
                .argBigDecimal(new BigDecimal("123.4567")).insert(1);
    }

    @After
    public void tearDown() throws Exception {
        if (dbp != null) {
            dbp.commitAndClose();
        }
    }

    @Test
    public void saveQuery() throws Exception {
        Etl.saveQuery(db.toSelect("select * from dbtest")).asBigQuery("som-rit-phi-starr-miner-dev", "rit_clarity_note_non_phi", "dbtest", "id").fetchSize(10).batchSize(10).workerNumber(2).addLabel("creator","database-goodies unit test").start();

    }
}