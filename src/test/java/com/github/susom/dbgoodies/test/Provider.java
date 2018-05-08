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
package com.github.susom.dbgoodies.test;

import com.github.susom.database.Database;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.DatabaseProvider.Builder;

/**
 * 
 * @author Biarca.inc
 *
 */
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
