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
 * Process the number of transactions by commodity, only in Brazil
 */
public class Analysis01 {

    public static void main(String args[]) {
        // Application logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Enable to use as threads as we need
        SparkConf conf = new SparkConf().setAppName("analysis01").setMaster("local[*]");

        // Setting up the Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the values from the .csv data source
        JavaRDD<String> values = sc.textFile("in/transactions.csv");

        // Filter all the transactions occurred in Brazil
        values = values.filter(line -> AnalysisUtils.getValue(line, TransactionColsEnum.COUNTRY.getValue()).equals("Brazil"));

        // Mapping the occurence of a transaction for an specific commodity
        JavaPairRDD<String, Integer> transactions = values.mapToPair(getCommodity());

        // Group all the occurences and sum all the values
        transactions = transactions.reduceByKey((x, y) -> x + y);

        // Sort by key
        transactions = transactions.sortByKey();
        
        // Just to read the output in a more user friendly-way ... don't worry, I know about the memory consumption and
        // cluster considerations! :)
        transactions = transactions.coalesce(1);

        // Analysis done!
        transactions.saveAsTextFile("out/analysis01.csv");
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
            String commodity = values[TransactionColsEnum.COMMODITY.getValue()];
            Integer occurrence = Integer.valueOf(1);
            return new Tuple2<>(commodity, occurrence);
        };

        return func;
    } // end getCommodity()

} // end Analysis01