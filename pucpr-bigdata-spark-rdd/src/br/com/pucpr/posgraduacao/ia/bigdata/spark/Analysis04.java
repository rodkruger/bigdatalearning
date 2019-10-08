package br.com.pucpr.posgraduacao.ia.bigdata.spark;

import br.com.pucpr.posgraduacao.ia.bigdata.spark.enums.TransactionColsEnum;
import br.com.pucpr.posgraduacao.ia.bigdata.spark.keys.CommodityYearKey;
import br.com.pucpr.posgraduacao.ia.bigdata.spark.utils.AnalysisUtils;
import br.com.pucpr.posgraduacao.ia.bigdata.spark.utils.AvgCount;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Average of weight by commodity, grouped by year
 */
public class Analysis04 {

    public static void main(String args[]) {
        // Application logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Enable to use as threads as we need
        SparkConf conf = new SparkConf().setAppName("analysis04").setMaster("local[*]");

        // Setting up the Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the values from the .csv data source
        JavaRDD<String> values = sc.textFile("in/transactions.csv");

        // Filter the first line
        values = values.filter(line -> !line.startsWith("country_or_area;year"));

        // Mapping the occurence of a transaction for an year
        JavaPairRDD<CommodityYearKey, AvgCount> transactions = values.mapToPair(getCommodityByYear());

        // Group all the occurences and sum all the values
        transactions = transactions.reduceByKey((x, y) ->
                new AvgCount(x.getCount() + y.getCount(), x.getSum() + y.getSum()));

        // Computing the average
        JavaPairRDD<CommodityYearKey, Double> average = transactions.mapValues(x -> x.getSum() / x.getCount());

        // Sort by the key
        average = average.sortByKey();

        // Just to read the output in a more user friendly-way ... don't worry, I know about the memory consumption and
        // cluster considerations! :)
        average = average.coalesce(1);

        // Analysis done!
        average.saveAsTextFile("out/analysis04.csv");
    }

    /**
     * Function to create a Tuple for a commodity and One-occurence of a transaction for it
     *
     * @return
     */
    private static PairFunction<String, CommodityYearKey, AvgCount> getCommodityByYear() {
        PairFunction<String, CommodityYearKey, AvgCount> func;

        func = transaction -> {
            String[] values = transaction.split(AnalysisUtils.COLSEPARATOR);

            CommodityYearKey key = new CommodityYearKey(values[TransactionColsEnum.COMMODITY.getValue()],
                    values[TransactionColsEnum.YEAR.getValue()]);

            String weight = values[TransactionColsEnum.WEIGHTKG.getValue()];
            AvgCount value = new AvgCount(1, Double.valueOf(StringUtils.isNotEmpty(weight) ? Double.valueOf(weight) : 0.0d));

            return new Tuple2<>(key, value);
        };

        return func;
    } // end getCommodityByYear()

} // end Analysis04