package br.com.pucpr.posgraduacao.ia.bigdata.spark;

import br.com.pucpr.posgraduacao.ia.bigdata.spark.enums.TransactionColsEnum;
import br.com.pucpr.posgraduacao.ia.bigdata.spark.utils.AnalysisUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Commodity with more transactions in 2016 in Brazil
 */
public class Analysis03 {

    public static void main(String args[]) {
        // Application logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Enable to use as threads as we need
        SparkConf conf = new SparkConf().setAppName("analysis03").setMaster("local[*]");

        // Setting up the Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the values from the .csv data source
        JavaRDD<String> values = sc.textFile("in/transactions.csv");

        // Filter the first line
        values = values.filter(line -> (
                AnalysisUtils.getValue(line, TransactionColsEnum.YEAR.getValue()).equals("2016") &&
                        AnalysisUtils.getValue(line, TransactionColsEnum.COUNTRY.getValue()).equals("Brazil")
        ));

        // Mapping the occurence of a transaction for a commodity
        JavaPairRDD<String, Integer> transactions = values.mapToPair(getCommodity());

        // Group all the occurences and sum all the values
        transactions = transactions.reduceByKey((x, y) -> x + y);

        // Ok, all transactions processed ... now, time to sort descending, and the top will be the commodity with more
        // transactions. To do this easilly in Spark, we will convert the Pair RDD to a RDD of Tuple2<>, LoL !!!
        JavaRDD<Tuple2<String, Integer>> sortedRDD = transactions.rdd().toJavaRDD();

        // Sort by value (_2)
        sortedRDD = sortedRDD.sortBy(Tuple2::_2, false, 1);

        // Just to read the output in a more user friendly-way ... don't worry, I know about the memory consumption and
        // cluster considerations! :)
        sortedRDD = sortedRDD.coalesce(1);

        // Analysis done!
        sortedRDD.saveAsTextFile("out/analysis03.csv");
    }

    /**
     * Function to create a Tuple for a commodity and One-occurence of a transaction for it
     *
     * @return
     */
    private static PairFunction<String, String, Integer> getCommodity() {
        PairFunction<String, String, Integer> func;

        func = transaction -> {
            String[] values = transaction.split(AnalysisUtils.COLSEPARATOR);
            String year = values[TransactionColsEnum.COMMODITY.getValue()];
            Integer occurrence = Integer.valueOf(1);
            return new Tuple2<>(year, occurrence);
        };

        return func;
    } // end getYear()

} // end Analysis02