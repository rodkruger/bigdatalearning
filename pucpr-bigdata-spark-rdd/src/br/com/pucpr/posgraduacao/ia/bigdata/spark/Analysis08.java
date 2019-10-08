package br.com.pucpr.posgraduacao.ia.bigdata.spark;

import br.com.pucpr.posgraduacao.ia.bigdata.spark.enums.TransactionColsEnum;
import br.com.pucpr.posgraduacao.ia.bigdata.spark.keys.FlowYearKey;
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
 * Transactions by flow and year
 */
public class Analysis08 {

    public static void main(String args[]) {
        // Application logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Enable to use as threads as we need
        SparkConf conf = new SparkConf().setAppName("analysis08").setMaster("local[*]");

        // Setting up the Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the values from the .csv data source
        JavaRDD<String> values = sc.textFile("in/transactions.csv");

        // Filter the first line
        values = values.filter(line -> !line.startsWith("country_or_area;year"));

        // Mapping the occurence of a transaction for an year
        JavaPairRDD<FlowYearKey, Integer> transactions = values.mapToPair(getFlowByYear());

        // Group all the occurences and sum all the values
        transactions = transactions.reduceByKey((x, y) -> x + y);

        // Sort by the key
        transactions = transactions.sortByKey();

        // Just to read the output in a more user friendly-way ... don't worry, I know about the memory consumption and
        // cluster considerations! :)
        transactions = transactions.coalesce(1);

        // Analysis done!
        transactions.saveAsTextFile("out/analysis08.csv");
    }

    /**
     * Function to create a Tuple for a flow and One-occurence of a transaction for it
     *
     * @return
     */
    private static PairFunction<String, FlowYearKey, Integer> getFlowByYear() {
        PairFunction<String, FlowYearKey, Integer> func;

        func = transaction -> {
            String[] values = transaction.split(AnalysisUtils.COLSEPARATOR);

            FlowYearKey key = new FlowYearKey(values[TransactionColsEnum.FLOW.getValue()],
                    values[TransactionColsEnum.YEAR.getValue()]);

            Integer value = Integer.valueOf(1);

            return new Tuple2<>(key, value);
        };

        return func;
    } // end getFlowByYear()

} // end Analysis08