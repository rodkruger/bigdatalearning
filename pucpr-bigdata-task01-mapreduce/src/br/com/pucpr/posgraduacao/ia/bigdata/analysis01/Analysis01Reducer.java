package br.com.pucpr.posgraduacao.ia.bigdata.analysis01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Counts all transactions occurred for an especific key (commodity, country etc)
 */
public class Analysis01Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable valor : values) {
            sum += valor.get();
        }

        con.write(word, new IntWritable(sum));
    } // end reduce()

} // end Analysis01Reducer
