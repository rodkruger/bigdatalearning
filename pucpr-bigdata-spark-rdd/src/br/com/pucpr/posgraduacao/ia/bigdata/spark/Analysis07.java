package br.com.pucpr.posgraduacao.ia.bigdata.spark;

import br.com.pucpr.posgraduacao.ia.bigdata.spark.enums.TransactionColsEnum;
import br.com.pucpr.posgraduacao.ia.bigdata.spark.utils.AnalysisUtils;
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
 * Commodity with the most expensive tradeUsd / weight
 */
public class Analysis07 {

    public static void main(String args[]) {
        // Application logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Enable to use as threads as we need
        SparkConf conf = new SparkConf().setAppName("analysis07").setMaster("local[*]");

        // Setting up the Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the values from the .csv data source
        JavaRDD<String> values = sc.textFile("in/transactions.csv");

        // Filter the first line
        values = values.filter(line -> !line.startsWith("country_or_area;year"));

        // Mapping the occurence of a transaction for a commodity
        JavaPairRDD<String, Double> transactions = values.mapToPair(getCommodity());

        // Group all the occurences and sum all the values
        transactions = transactions.reduceByKey((x, y) -> x + y);

        // Ok, all transactions processed ... now, time to sort descending, and the top will be the commodity with more
        // transactions. To do this easilly in Spark, we will convert the Pair RDD to a RDD of Tuple2<>, LoL !!!
        JavaRDD<Tuple2<String, Double>> sortedRDD = transactions.rdd().toJavaRDD();

        // Sort by value (_2)
        sortedRDD = sortedRDD.sortBy(Tuple2::_2, false, 1);

        // Just to read the output in a more user friendly-way ... don't worry, I know about the memory consumption and
        // cluster considerations! :)
        sortedRDD = sortedRDD.coalesce(1);

        // Analysis done!
        sortedRDD.saveAsTextFile("out/analysis07.csv");
    }

    /**
     * Function to create a Tuple for a commodity and One-occurence of a transaction for it
     *
     * @return
     */
    private static PairFunction<String, String, Double> getCommodity() {
        PairFunction<String, String, Double> func;

        func = transaction -> {
            String[] values = transaction.split(AnalysisUtils.COLSEPARATOR);
            String year = values[TransactionColsEnum.COMMODITY.getValue()];

            String weight = values[TransactionColsEnum.WEIGHTKG.getValue()];
            String tradeUsd = values[TransactionColsEnum.TRADEUSD.getValue()];

            Double dWeight = StringUtils.isNotEmpty(weight) ? Double.valueOf(weight) : 0.0d;
            Double dTradeUsd = StringUtils.isNotEmpty(tradeUsd) ? Double.valueOf(tradeUsd) : 0.0d;

            Double value = dWeight > 0 ? dTradeUsd / dWeight : 0.0d;

            return new Tuple2<>(year, value);
        };

        return func;
    } // end getCommodity()

} // end Analysis03