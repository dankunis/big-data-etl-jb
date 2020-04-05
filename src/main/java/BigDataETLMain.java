import bigdata.extract.BigDataExtractor;
import bigdata.load.BigDataLoader;
import bigdata.transformation.BigDataTransformator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class BigDataETLMain {
    public static void main(String[] args) {
        String warehouseLocation = new File("outputDB/spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Big Data ETL App")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        // Extract data
        InputStream csvInput = BigDataETLMain.class
                .getResourceAsStream("GlobalLandTemperaturesByMajorCity.csv");
        Dataset<Row> dataset = null;
        try {
            dataset = BigDataExtractor
                    .extractDataFrom(csvInput, spark);
        } catch (Exception e) {
            System.out.println("[ERROR] Couldn't load the CSV");
            e.printStackTrace();
            System.exit(1);
        }

        // Transform data
        Map<String, Dataset<Row>> aggregations = new HashMap<>();

        Dataset<Row> citiesTempByYear = BigDataTransformator.buildCitiesWeatherByYearDataset(dataset);
        aggregations.put("CitiesWeatherByYear", citiesTempByYear);
        Dataset<Row> citiesTempByCentury = BigDataTransformator.buildCitiesWeatherByCenturyDataset(citiesTempByYear);
        aggregations.put("CitiesWeatherByCentury", citiesTempByCentury);
        Dataset<Row> citiesTempOverall = BigDataTransformator.buildCitiesWeatherOverallDataset(citiesTempByCentury);
        aggregations.put("CitiesWeatherOverall", citiesTempOverall);

        Dataset<Row> countriesTempByYear = BigDataTransformator.buildCountriesWeatherByYearDataset(citiesTempByYear);
        aggregations.put("CountriesWeatherByYear", countriesTempByYear);
        Dataset<Row> countriesTempByCentury = BigDataTransformator.buildCountriesWeatherByCenturyDataset(countriesTempByYear);
        aggregations.put("CountriesWeatherByCentury", countriesTempByCentury);
        Dataset<Row> countriesTempOverall = BigDataTransformator.buildCountriesWeatherOverallDataset(countriesTempByCentury);
        aggregations.put("CountriesWeatherOverall", countriesTempOverall);

        // Load data
        for (String dbName: aggregations.keySet()) {
            BigDataLoader.uploadDfToDatabase(aggregations.get(dbName), dbName, spark);
        }

        spark.sql("SELECT * FROM CountriesWeatherOverall").show();
    }
}
