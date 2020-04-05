package bigdata.extract;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public interface BigDataExtractor {
    static Dataset<Row> extractDataFrom(InputStream inputStream, SparkSession spark) throws Exception {
        String csvPath = BigDataExtractorUtils
                .createTempFileFromInput("weather", "csv", inputStream);
        return spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(csvPath);
    }
}

class BigDataExtractorUtils {
    static String createTempFileFromInput(String namePrefix,
                                                  String extension,
                                                  InputStream input) throws Exception {
        File tempFile = File.createTempFile(namePrefix, extension);
        OutputStream outputStream = new FileOutputStream(tempFile);
        IOUtils.copy(input, outputStream);
        tempFile.deleteOnExit();

        return tempFile.getPath();
    }
}
