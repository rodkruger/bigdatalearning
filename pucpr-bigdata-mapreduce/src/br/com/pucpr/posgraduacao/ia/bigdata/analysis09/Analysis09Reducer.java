package br.com.pucpr.posgraduacao.ia.bigdata.analysis09;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer used to count all transactions by flow and year
 */
public class Analysis09Reducer extends Reducer<FlowPerYear, IntWritable, FlowPerYear, IntWritable> {

    @Override
    public void reduce(FlowPerYear word, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;

        for (IntWritable value : values) {
            count += value.get();
        }

        context.write(word, new IntWritable(count));
        
    } // end reduce()

} // end Analysis09Reducer
