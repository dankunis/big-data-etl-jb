package bigdata.load;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface BigDataLoader {
    static void uploadDfToDatabase(Dataset<Row> dataset, String dbName, SparkSession spark) {
        /*
         Quick solution to avoid errors with overriding an existing table.
         Please note that I understand that this is a bad approach.
         */
        spark.sql(String.format("drop table if exists %s", dbName));
        dataset.write().saveAsTable(dbName);
    }
}
