package com.github.susom.dbgoodies.etl;

import com.github.susom.database.*;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.util.DateTime;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.Schema;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;

/**
 * BigQuery writer
 *
 * @author wencheng
 */
public class BigQueryWriter<T> {

  private static final Logger log = LoggerFactory.getLogger(BigQueryWriter.class);

  private BlockingQueue<List<InsertAllRequest.RowToInsert>> uploadQueue;

  private volatile CountDownLatch readerCompletedSignal;

  private String bigqueryProjectId;
  private String googleCredentialFile;

  private volatile CountDownLatch bqSchemaReady;

  volatile private Schema bgSchema;
  private FieldList fields;

  private TableInfo tableInfo;
  private String[] entryIdFields;
  private int uploadBatchSize;

  private String tableName;
  private String dataset;
  private String[] columnNames;
  private int[] precisions;
  private int[] scales;
  private int[] columnTypes;
  private Map<String,String> labels;

  private int uploadThread;


  private AtomicLong totalCnt = new AtomicLong();

  private volatile long startTs = Instant.now().getEpochSecond();
  private volatile long lastBatchStartTs = Instant.now().getEpochSecond();

  private List<Thread> workers = new ArrayList<>();

  private String lastId = null;

  private BigQueryWriter() {
  }

  void setupWriter(CountDownLatch _readerCompletedSignal) throws Exception {
    this.bqSchemaReady = new CountDownLatch(1);
    this.readerCompletedSignal = _readerCompletedSignal;
    this.uploadQueue = new LinkedBlockingDeque<>();


    for(int i = 0;i< uploadThread; i++){
        Thread t = createUploadWorker(i);
        workers.add(t);
    }


    workers.forEach(Thread::start);


    new Thread(()->{
      try {
        this.readerCompletedSignal.await();
        Thread.sleep(5000);

        while(uploadQueue.size()>0){
          log.info("wait for queue clear up before shutdown workers");
          Thread.sleep(5000);
        }


        for(int i = 0;i< uploadThread; i++){
          List<InsertAllRequest.RowToInsert> deadSignal = new ArrayList<>();
          deadSignal.add(null);
          uploadQueue.add(deadSignal);
        }

        log.info("interrupting all workers");
        workers.forEach(Thread::interrupt);

        workers.clear();

        log.info("worker clean up is done by"+Thread.currentThread().getName());
      } catch (InterruptedException e) {
        log.error(e.getMessage(),e);
      }

    }).start();

  }

  private void prepareBqTableSchemaFromDbRow(Row r){
    TableId tableId = TableId.of(dataset, tableName);
    TableDefinition tableDefinition = StandardTableDefinition.of(setBqSchemaFromDbRow(r));
    this.tableInfo = TableInfo.newBuilder(tableId, tableDefinition).setLabels(this.labels).build();
  }

  private Table createBqTableFromDbRow(BigQuery bigquery){
      Table table = null;
    try {
        table = bigquery.create(tableInfo);
        bqSchemaReady.countDown();
      }catch (Exception e){
        log.warn("Unable to create table, check if table already exists", e);
        table = checkTable(bigquery);
      }
      return table;
  }

  private Table checkTable(BigQuery bigquery){
    TableId tableId = TableId.of(dataset, tableName);
    Table table = bigquery.getTable(tableId);
    if(table!=null)
      fields = Objects.requireNonNull(table.getDefinition().getSchema()).getFields();


    log.info("checking table result:"+(table!=null?table.toString():null));
    bqSchemaReady.countDown();
    return table;
  }


  /**
   *
   * TODO review the logic about precision in original code in com.github.susom.database
   * @param r
   * @return
   */
  private com.google.cloud.bigquery.Schema setBqSchemaFromDbRow(Row r) {
    List<Field> schemaFields = new ArrayList<>();

    try {
      ResultSetMetaData metadata = r.getMetadata();

      int columnCount = metadata.getColumnCount();

      columnNames = new String[columnCount];
      for (int i = 0; i < columnCount; i++) {
        columnNames[i] = metadata.getColumnName(i + 1);
      }
      columnNames = SqlArgs.tidyColumnNames(columnNames);
      columnTypes = new int[columnCount];
      precisions = new int[columnCount];
      scales = new int[columnCount];

      for (int i = 0; i < columnCount; i++) {
        int columnType = metadata.getColumnType(i + 1);
        columnTypes[i] = columnType;
        LegacySQLTypeName fieldType; //LegacySQLTypeName
        String fieldName = columnNames[i];



        switch (columnType) {
          case Types.SMALLINT:
          case Types.INTEGER:
            fieldType = LegacySQLTypeName.INTEGER;
            break;
          case Types.BIGINT:
            fieldType = LegacySQLTypeName.INTEGER;
            break;
          case Types.REAL:
          case Types.FLOAT:
          case 100: // Oracle proprietary it seems
            fieldType = LegacySQLTypeName.FLOAT;
            break;
          case Types.DOUBLE:
          case 101: // Oracle proprietary it seems
            fieldType = LegacySQLTypeName.FLOAT;
            break;
          case Types.DECIMAL:
          case Types.NUMERIC:

            int precision = metadata.getPrecision(i + 1);
            int scale = metadata.getScale(i + 1);
            precisions[i] = precision;
            scales[i] = scale;
            fieldType = LegacySQLTypeName.NUMERIC; //LegacySQLTypeName.BYTES
            break;

          case Types.BINARY:
          case Types.VARBINARY:
          case Types.BLOB:
            fieldType = LegacySQLTypeName.BYTES;
            break;
          case Types.CLOB:
          case Types.NCLOB:
            fieldType = LegacySQLTypeName.STRING;
            break;
          case Types.TIMESTAMP:
            fieldType = LegacySQLTypeName.TIMESTAMP;
            break;
          case Types.NVARCHAR:
          case Types.VARCHAR:
          case Types.CHAR:
          case Types.NCHAR:
            fieldType = LegacySQLTypeName.STRING;
            break;
          default:
            throw new DatabaseException("Don't know how to deal with column columnType: " + columnType);
        }


        schemaFields.add(Field.of(fieldName, fieldType));
      }
    } catch (SQLException e) {
      throw new DatabaseException("Unable to retrieve metadata from ResultSet", e);
    }
    bgSchema = com.google.cloud.bigquery.Schema.of(schemaFields);
    fields = bgSchema.getFields();
    return bgSchema;
  }



  private InsertAllRequest.RowToInsert createInsertFromDbArgs(Row r) {
    Map<String, Object> row = new HashMap<>();
    String rowId = null;

    for (int i = 0; i < columnNames.length; i++) {


      switch (columnTypes[i]) {
        case Types.SMALLINT:
        case Types.INTEGER:
          row.put(columnNames[i], r.getIntegerOrNull(columnNames[i]));
          break;
        case Types.BIGINT:
          row.put(columnNames[i], r.getLongOrNull(columnNames[i]));
          break;
        case Types.REAL:
        case 100: // Oracle proprietary it seems
          row.put(columnNames[i], r.getFloatOrNull(columnNames[i]));
          break;
        case Types.DOUBLE:
        case Types.FLOAT: //bigquery float is double precision
        case 101: // Oracle proprietary it seems
          row.put(columnNames[i], r.getDoubleOrNull(columnNames[i]));
          break;
        case Types.DECIMAL:
        case Types.NUMERIC:
          // 9 decimal digits of scale allowed by BigQuery
          BigDecimal v = r.getBigDecimalOrNull(columnNames[i]);
          if(v == null){
            row.put(columnNames[i],null);
          }else if(v.scale()>9){
            try{
              row.put(columnNames[i],v.setScale(9, RoundingMode.HALF_UP));
            }catch (ArithmeticException e){
              row.put(columnNames[i], null);
            }
          }else{
            row.put(columnNames[i],v);
          }
          break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.BLOB:
          row.put(columnNames[i], r.getBlobBytesOrNull(columnNames[i]));
          break;
        case Types.CLOB:
        case Types.NCLOB:
          row.put(columnNames[i], r.getClobStringOrNull(columnNames[i]));
          break;
        case Types.TIMESTAMP:
          //'2014-09-15T01:07:00.000-07:00' '2014-09-18 01:10:00'
          Date temp = r.getDateOrNull(columnNames[i]);
          if(temp!=null) {
//            log.info(temp.toString());
            row.put(columnNames[i], new DateTime(temp)); //new DateTime(temp)
          }else{
            row.put(columnNames[i], temp);//null
          }
          break;
        case Types.NVARCHAR:
        case Types.VARCHAR:
        case Types.CHAR:
        case Types.NCHAR:
          if (precisions[i] >= 2147483647) {
            // Postgres seems to report clobs are varchar(2147483647)
            row.put(columnNames[i], r.getClobStringOrNull(columnNames[i]));
          } else {
            row.put(columnNames[i], r.getStringOrNull(columnNames[i]));
          }
          break;
        default:
          throw new DatabaseException("Don't know how to deal with column type: " + columnTypes[i]);
      }
    }

    if(entryIdFields!=null && entryIdFields.length>0)
      return InsertAllRequest.RowToInsert.of(getRowId(row), row);
    else
      return InsertAllRequest.RowToInsert.of(row);

  }

  private String getRowId(Map<String, Object> row){
    String id = "";
    for (String f : entryIdFields){
      id+= ("_"+ (row.get(f)!=null?row.get(f):row.get(f.toLowerCase())));
    }
    return id;
  }


  public void deleteBqTable(){
    log.error("not implemented yet");
  }


  /*
   * Current Google BigQuery quota limit https://cloud.google.com/bigquery/quotas#streaming_inserts :
   * 1. Maximum row size: 1 MB. Exceeding this value will cause invalid errors.
   * 2. HTTP request size limit: 10 MB. Exceeding this value will cause invalid errors.
   * 3. Maximum rows per second: 100,000 rows per second, per project. Exceeding this amount will cause quotaExceeded errors. The maximum number of rows per second per table is also 100,000. You can use all of this quota on one table or you can divide this quota among several tables in a project.
   * 4. Maximum rows per request: 10,000 rows per request. We recommend a maximum of 500 rows. Batching can increase performance and throughput to a point, but at the cost of per-request latency. Too few rows per request and the overhead of each request can make ingestion inefficient. Too many rows per request and the throughput may drop. We recommend using about 500 rows per request, but experimentation with representative data (schema and data sizes) will help you determine the ideal batch size.
   * 5. Maximum bytes per second: 100 MB per second, per table. Exceeding this amount will cause quotaExceeded errors.
   */
  RowsHandler<T> databaseRowsHandler(){
    return new RowsHandler<T>(){

      @Override
      public T process(Rows rs) throws Exception {
        List<InsertAllRequest.RowToInsert> batchInsertData = new ArrayList<>();


        long batchByteSize = 0;
        try{

          while (rs.next()) {

            if (bgSchema == null) {
              setBqSchemaFromDbRow(rs);
              if (bqSchemaReady.getCount()!=0L) {
//                createBqTableFromDbRow(rs);
                prepareBqTableSchemaFromDbRow(rs);
                uploadQueue.add( new ArrayList<InsertAllRequest.RowToInsert>() ); //signal worker to create/check schema


              }
            }
            bqSchemaReady.await();

            InsertAllRequest.RowToInsert rowToInsert = createInsertFromDbArgs(rs);
            batchByteSize += rowToInsert.toString().length();
            batchInsertData.add(rowToInsert);



            if ((uploadBatchSize!=-1 && batchInsertData.size() >= uploadBatchSize) || batchByteSize >= 10000000) {

                List<InsertAllRequest.RowToInsert> batchInsertDataLoad = batchInsertData;


                uploadQueue.add(batchInsertDataLoad);
                totalCnt.addAndGet(batchInsertData.size());
                long timeTook = (Instant.now().getEpochSecond()-lastBatchStartTs);
                long totalTimeTook = Instant.now().getEpochSecond() - startTs;
                if(timeTook!=0)
                  log.info("db "+tableName+" total:"+totalCnt.get()+" time "+timeTook+" s speed:"+ totalCnt.get()/totalTimeTook +" r/s");
                else
                  System.out.print("*");

                lastBatchStartTs = Instant.now().getEpochSecond() ;
                batchByteSize = 0;
//              batchInsertData.clear();
                batchInsertData = new ArrayList<>();
            }

            if(uploadQueue.size()>uploadThread * 2){
              log.info("wait queue clear up");
              while(true) {
                System.out.print("~"+uploadQueue.size());
                Thread.sleep(2000);
                if(uploadQueue.size()<=uploadThread * 2) {
                  break;
                }
              }
            }

          }
        }catch (Exception e){
          throw e;
        }


        if (batchInsertData.size() > 0) {
          log.info("push final batch");

          uploadQueue.add(batchInsertData);
          totalCnt.addAndGet(batchInsertData.size());
          long timeTook = (Instant.now().getEpochSecond()-startTs);
          log.info("last batch db "+tableName+" totalCnt:"+totalCnt.get()+" total time: "+timeTook+" s ");
        }

        return null;
      }
    };
  }


  public synchronized com.google.auth.Credentials  getGoogleCredential(String credentialFile, List<String> scopes) throws IOException {

    GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(credentialFile), new HttpTransportFactory() {
      @Override
      public HttpTransport create() {
        try {
          return newTrustedTransport();
        } catch (Exception e) {
          log.error("failed to create new Google Trusted Transport ");
        }
        return null;
      }
    })
      .createScoped(scopes);
    return credentials;
  }


    private Thread createUploadWorker (int workerId){
      return new Thread(){

          @Override
          public void run() {
              try {

                  log.info("start worker"+Thread.currentThread().getName());
                BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder();

                BigQueryOptions bigqueryOpt =
                  optionsBuilder.setCredentials(getGoogleCredential(googleCredentialFile,
                  Lists.newArrayList("https://www.googleapis.com/auth/bigquery")))
                  .setProjectId(bigqueryProjectId)
                  .build();

                  BigQuery bigquery= bigqueryOpt.getService();

                  Table table = null;

                  while (!Thread.currentThread().isInterrupted()){
                      List<InsertAllRequest.RowToInsert> payload = uploadQueue.poll(5, TimeUnit.SECONDS); //.take();

                      if(payload==null){ //continue to poll
                        continue;
                      }

                      if(payload.size()==1 && payload.get(0)==null){ //dead signal
                        log.info("received shutdown signal, shutting down worker"+Thread.currentThread().getName());
                        return;
                      }
                      if(payload.size()==0){ //be the one to create or check table
                        table = createBqTableFromDbRow(bigquery);
                        continue;
                      }

                      bqSchemaReady.await();
                      //log.info("Upload worker:"+Thread.currentThread().getName());
                      if(table==null){
                        table = checkTable(bigquery);
                      }
                      int retryCount = 0;
                      int maxTry = 12;
                      while(retryCount<maxTry){
                          try {
                              InsertAllResponse response = table.insert(payload);

                                System.out.print("<");
                              if(response.getInsertErrors().size()>0){
                                  log.error(response.getInsertErrors().toString());
                                  throw new Exception("BigQuery failed");
                              }else{
                                lastId = payload.get(payload.size()-1).getId();
                                System.out.print(workerId+">");
                                payload.clear();

                                break;
                              }
                          }catch (Exception e){
                            log.error("Error. Retry count:"+retryCount, e);
                            if(retryCount<8){
                                double waitFor = Math.pow(2,retryCount);
                                log.info("retry after "+waitFor+" seconds");
                                Thread.sleep((long)waitFor*1000);
                            }else {
                                log.info("retry after 5 minutes");
                                Thread.sleep(300000L);
                            }
                            if(retryCount==maxTry-1){
                                uploadQueue.add(payload);
                            }
                          }

                          retryCount++;
                      }

//                      payload.forEach(r->{r.getContent().clear();});
//                      payload.clear();

                  }



              } catch (Exception e) {
                  log.error("upload worker stopped", e);
              }



            log.info("finished worker"+Thread.currentThread().getName());

          }
      };
  }


  public static final class BigQueryWriterBuilder {
    String bigqueryProjectId;
    String googleCredentialFile;
    String[] entryIdFields;
    int uploadBatchSize;
    String dataset;
    String tableName;
    int uploadThread;
    Map<String,String> labels;

    private BigQueryWriterBuilder() {
    }

    public static BigQueryWriterBuilder aBigQueryWriter() {
      return new BigQueryWriterBuilder();
    }

    public BigQueryWriterBuilder withBigqueryProjectId(String bigqueryProjectId) {
      this.bigqueryProjectId = bigqueryProjectId;
      return this;
    }

    public BigQueryWriterBuilder withEntryIdFields(String[] entryIdFields) {
      this.entryIdFields = entryIdFields;
      return this;
    }

    public BigQueryWriterBuilder withUploadBatchSize(int uploadBatchSize) {
      this.uploadBatchSize = uploadBatchSize;
      return this;
    }

    public BigQueryWriterBuilder withDataset(String dataset) {
      this.dataset = dataset;
      return this;
    }

    public BigQueryWriterBuilder withTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public BigQueryWriterBuilder withUploadThread(int uploadThread) {
      this.uploadThread = uploadThread;
      return this;
    }

    public BigQueryWriterBuilder withBigQueryCredentialFile(String googleCredentialFile) {
      this.googleCredentialFile = googleCredentialFile;
      return this;
    }

    public BigQueryWriterBuilder withLabels(Map<String,String> labels){
      this.labels = labels;
      return this;
    }

    public BigQueryWriter<Row> build() throws Exception {
      BigQueryWriter<Row> bigQueryWriter = new BigQueryWriter<>();
      bigQueryWriter.bigqueryProjectId = this.bigqueryProjectId;
      bigQueryWriter.dataset = this.dataset;
      bigQueryWriter.tableName = this.tableName;
      bigQueryWriter.uploadThread = this.uploadThread;
      bigQueryWriter.entryIdFields = this.entryIdFields;
      bigQueryWriter.uploadBatchSize = this.uploadBatchSize;
      bigQueryWriter.googleCredentialFile = this.googleCredentialFile;
      bigQueryWriter.bigqueryProjectId = this.bigqueryProjectId;
      bigQueryWriter.labels = this.labels;
      return bigQueryWriter;
    }
  }
}
