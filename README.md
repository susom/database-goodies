## Database Goodies

This is extension of project [database-goodies](https://github.com/susom/database-goodies).

### Features

* Added functionality to convert database tables to AVRO file format


### Getting Started

Initialise the database that you want to convert into AVRO file:

```
Database db = DatabaseProvider.fromDriverManager("jdbc:hsqldb:hsql://localhost/testdb").create().get();

```

Write the sql "select" statement with columns you are interested or simply write "*" for entire column selection:

```
SqlSelect sqlSelectObject = db.toSelect("select * from user_details");

```

Inject the "select" object to the ETL interface

```
Etl.Save hsqlData = Etl.saveQuery(sqlSelectObject);

```

Pass on the target file name with path and table name and execute:

```

Etl.SaveAsAvro avro = hsqlData.asAvro("user_details.avro", "user_details");
avro.start();

```
