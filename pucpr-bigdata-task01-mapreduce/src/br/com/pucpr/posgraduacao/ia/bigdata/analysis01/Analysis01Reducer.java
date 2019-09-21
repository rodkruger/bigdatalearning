package br.com.pucpr.posgraduacao.ia.bigdata.analysis01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Analysis01Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    public void reduce(Text word, Iterable<IntWritable> values, Context con)
            throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable valor : values) {
            sum += valor.get();
        }

        // Escrever no contexto
        con.write(word, new IntWritable(sum));
    }

}
