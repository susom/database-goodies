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
 * @author Biarca.inc
 *
 */
public class EtlCopyAvroSample {

  private static final String DatabaseURL = "jdbc:hsqldb:hsql://localhost/testdb";
  
  /* Test data constants */
  private static final String HOME_DIR = "user.home";
  private static final String CUR_DIR = ".";
  private static final String QUERY = "select * from user_details";
  private static final String AVRO_FILE = "/user_details.avro";
  private static final String TABLE = "user_details";
  private static final boolean iaAppend = true;

  public static void main(String[] args) throws Exception {
    String homeDir = null;
    Provider hsqlProvider = Provider.getInstance();
    hsqlProvider.setupJdbc(DatabaseURL);
    
    Database sampleHsqlDb = hsqlProvider.getDatabase();
    SqlSelect sqlSelectObject = sampleHsqlDb.toSelect(QUERY);

    homeDir = System.getProperty(HOME_DIR);
    StringBuilder fileName = new StringBuilder((homeDir != null) ? homeDir: CUR_DIR);
    fileName.append(AVRO_FILE);
    
    Etl.Save hsqlData = Etl.saveQuery(sqlSelectObject);
    Etl.SaveAsAvro avro = hsqlData.asAvro(fileName.toString(), TABLE, iaAppend);
    avro.start();

    hsqlProvider.closeJdbc();
    fileName = null;
  }
}
