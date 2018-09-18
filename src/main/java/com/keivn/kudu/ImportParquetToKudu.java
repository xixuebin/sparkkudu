package com.keivn.kudu;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.ListTablesResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class ImportParquetToKudu {
  private static String tableName;
  private static String KUDU_MASTER;
  private static KuduClient client;
  private static KuduTable table;
  private static KuduSession kuduSession;

  static {
    tableName = "impala::impala_kudu.clicklog";
    KUDU_MASTER = "ip-10-12-254-200.ec2.internal:7051";

    client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
    try {
      table = client.openTable(tableName);
    } catch (KuduException e) {
      e.printStackTrace();
    }
    kuduSession = client.newSession();
  }

  public static void main(String[] args) throws KuduException {

    ListTablesResponse AA = client.getTablesList();
    List<String> tablesList = AA.getTablesList();
    for (String tn : tablesList){
      System.out.println(tn);
    }

//    deleteAllRow();

    writeParquetData();


  }

  public static void writeParquetData() throws KuduException {
    SparkSession session;

    session = SparkSession.builder().appName(ImportParquetToKudu.class.getSimpleName())
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.eventLog.enabled", true)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.binaryAsString", true)
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "20")
        .config("spark.default.parallelism", "60")
        .master("local[*]")
        .getOrCreate();

    session.sparkContext().setLogLevel("error");
//  LOG.info("1.Load Data==================================================================");
    Dataset<Row> ds_test = session.read().parquet("/Users/xixuebin/IdeaProjects/apache/kudu/java/sparkkudu/src/main/resources/test-data.parq");
    ds_test.printSchema();
    ds_test.show(10, false);
    ds_test = ds_test.withColumn("part", functions.date_format(functions.col("time_stamp"), "yyyy-MM-dd"));
    ds_test.printSchema();

    ds_test.show(10, false);

    List<Row> rows = ds_test.collectAsList();
    String[] columns =  ds_test.columns();

    for (Row row: rows
        ) {
      System.out.println(row.getString(28));
      Insert insert = table.newInsert();
      PartialRow kuduRow = insert.getRow();
      for (int i= 0; i<row.size(); i++){

        kuduRow.addString(columns[i],row.getString(i));
      }
      kuduSession.apply(insert);
    }

    kuduSession.flush();

    System.out.println(ds_test.count());
  }

  public static void deleteAllRow()
      throws KuduException {

    KuduScanner scanner =
        client.newScannerBuilder(table)
            .setProjectedColumnNames(Lists.<String>newArrayList("transaction_id"))
            .build();
    while (scanner.hasMoreRows()) {
      for (RowResult row : scanner.nextRows()) {
        String transaction_id =  row.getString("transaction_id");
        Delete delete = table.newDelete();
        delete.getRow().addString("transaction_id", transaction_id);
        System.out.println("delete row " + transaction_id  + " success!");
        kuduSession.apply(delete);
      }
    }
    kuduSession.flush();
  }

}
