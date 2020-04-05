package bigdata.transformation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public interface BigDataTransformator {

    static Dataset<Row> buildCitiesWeatherByYearDataset(Dataset<Row> weatherDataset) {
        return weatherDataset.withColumn("Year", functions.year(weatherDataset.col("dt")))
                .groupBy("City", "Year", "Country")
                .agg(
                        functions.avg("AverageTemperature").as("YearAvg"),
                        functions.min("AverageTemperature").as("YearMin"),
                        functions.max("AverageTemperature").as("YearMax")
                );
    }

    /**
     * As input expects to get the result of
     * buildCitiesTemperatureDatasetByYear(Dataset<Row> weatherDataset)
     * @param cityDatasetByYear - formed by buildCitiesTemperatureDatasetByYear
     * @return formed dataset with counted temp by each century
     */
    static Dataset<Row> buildCitiesWeatherByCenturyDataset(Dataset<Row> cityDatasetByYear) {
        return cityDatasetByYear
                .withColumn("Century",
                        cityDatasetByYear.col("Year").divide(100).plus(1).cast(DataTypes.IntegerType))
                .groupBy("City", "Century", "Country")
                .agg(
                        functions.avg("YearAvg").as("CenturyAvg"),
                        functions.min("YearMin").as("CenturyMin"),
                        functions.max("YearMax").as("CenturyMax")
                );
    }

    /**
     * As input expects to get the result of
     * buildCitiesTemperatureDatasetByCentury(Dataset<Row> weatherDatasetByYear)
     * @param cityDatasetByCentury - formed by buildCitiesTemperatureDatasetByCentury
     * @return formed dataset with counted temp by all time
     */
    static Dataset<Row> buildCitiesWeatherOverallDataset(Dataset<Row> cityDatasetByCentury) {
        return cityDatasetByCentury
                .groupBy("City", "Country")
                .agg(
                        functions.avg("CenturyAvg").as("OverallAvg"),
                        functions.min("CenturyMin").as("OverallMin"),
                        functions.max("CenturyMax").as("OverallMax")
                );
    }

    static Dataset<Row> buildCountriesWeatherByYearDataset(Dataset<Row> cityDatasetByYear) {
        return cityDatasetByYear
                .groupBy("Country", "Year")
                .agg(
                        functions.min("YearMin").as("YearMin"),
                        functions.max("YearMax").as("YearMax")
                );
    }

    static Dataset<Row> buildCountriesWeatherByCenturyDataset(Dataset<Row> countryDatasetByYear) {
        return countryDatasetByYear
                .withColumn("Century",
                        countryDatasetByYear.col("Year").divide(100).plus(1).cast(DataTypes.IntegerType))
                .groupBy("Country", "Century")
                .agg(
                        functions.min("YearMin").as("CenturyMin"),
                        functions.max("YearMax").as("CenturyMax")
                );
    }

    static Dataset<Row> buildCountriesWeatherOverallDataset(Dataset<Row> countryDatasetByCentury) {
        return countryDatasetByCentury
                .groupBy("Country")
                .agg(
                        functions.min("CenturyMin").as("OverallMin"),
                        functions.max("CenturyMax").as("OverallMax")
                );
    }
}
