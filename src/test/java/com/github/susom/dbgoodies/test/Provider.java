package com.github.susom.dbgoodies.test;

import com.github.susom.database.Database;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.DatabaseProvider.Builder;

public class Provider {
  private static Provider singleton = new Provider();
  protected DatabaseProvider dbp;
  protected Database db;

  public Provider() {}
  
  public static Provider getInstance() {
    return singleton;
  }

  public DatabaseProvider createDatabaseProvider(String dbURL) throws Exception {
    Builder dbb = DatabaseProvider.fromDriverManager(dbURL);
    return dbb.create();
  }

  public void setupJdbc(String dbURL) throws Exception {
    dbp = createDatabaseProvider(dbURL);
    db = dbp.get();
  }

  public Database getDatabase() {
    return db;
  }

  public void closeJdbc() throws Exception {
    if (dbp != null) {
      dbp.commitAndClose();
    }
  }
}
