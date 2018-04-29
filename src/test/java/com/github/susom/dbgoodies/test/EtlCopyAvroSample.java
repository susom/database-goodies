package com.github.susom.dbgoodies.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.github.susom.database.Config;
import com.github.susom.database.Database;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.DatabaseProvider.Builder;
import com.github.susom.database.Rows;
import com.github.susom.database.RowsHandler;
import com.github.susom.database.Sql;
import com.github.susom.database.SqlSelect;
import com.github.susom.dbgoodies.etl.Etl;

/**
 * 
 * @author Bhaskar
 *
 */
public class EtlCopyAvroSample {

  private static final String databaseDriver = "org.hsqldb.jdbcDriver";
  private static final String databaseURL = "jdbc:hsqldb:hsql://localhost/testdb";
  private static final String databaseUser = "sa";
  private static final String databasePassword = "";
  
  /* Test data constants */
  private static final String QUERY = "select ID, FIRSTNAME, LASTNAME, SALARY from EMPLOYEE";
  private static final String AVRO_FILE = "Employee.avro";
  private static final String TABLE = "EMPLOYEE";

  public static void main(String[] args) throws Exception {
    Provider sampleProject = Provider.getInstance();
    sampleProject.setupJdbc(EtlCopyAvroSample.databaseURL);
    
    Database sampleHsqlDb = sampleProject.getDatabase();
    SqlSelect sqlSelectObject = sampleHsqlDb.toSelect(QUERY);

    Etl.Save hsqlData = Etl.saveQuery(sqlSelectObject);
    Etl.SaveAsAvro avro = hsqlData.asAvro(AVRO_FILE, TABLE);
    avro.start();

    sampleProject.closeJdbc();
  }
}
