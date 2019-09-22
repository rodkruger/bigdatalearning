package br.com.pucpr.posgraduacao.ia.bigdata.analysis02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Counts all transactions occurred for an especific key (commodity, country etc)
 */
public class Analysis02Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        context.write(word, new IntWritable(sum));
    } // end reduce()

}
