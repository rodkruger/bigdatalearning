package br.com.pucpr.posgraduacao.ia.bigdata.analysis06;

import br.com.pucpr.posgraduacao.ia.bigdata.analysis04.AverageOfTransactionsWritable;
import br.com.pucpr.posgraduacao.ia.bigdata.analysis04.CommodityPerYear;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer used to calculate an average of a specific key (commodity, year, etc)
 */
public class Analysis06Reducer extends Reducer<CommodityPerYear, AverageOfTransactionsWritable, CommodityPerYear, DoubleWritable> {

    @Override
    public void reduce(CommodityPerYear word, Iterable<AverageOfTransactionsWritable> values, Context con)
            throws IOException, InterruptedException {

        double sum = 0;
        double count = 0;

        for (AverageOfTransactionsWritable value : values) {
            sum += value.getSum();
            count += value.getCount();
        }

        double average = sum / count;

        con.write(word, new DoubleWritable(average));
    } // end reduce()

} // end Analysis06Reducer
