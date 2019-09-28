package br.com.pucpr.posgraduacao.ia.bigdata.analysis03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Map the commodity with more transactions
 */
public class Analysis03_01Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * Maximum number of transactions
     */
    private int maxTransactionsCount;

    /**
     * Commodity with more transactions
     */
    private String maxCommodity;

    @Override
    public void map(LongWritable key, Text value, Mapper.Context con) throws IOException, InterruptedException {

        // read the line in String format
        String line = value.toString();

        // splitting by tab (.csv format used)
        String[] info = line.split("\t");

        int numberOfTransactions = Integer.valueOf(info[1]);

        if (numberOfTransactions > this.maxTransactionsCount) {
            this.maxCommodity = info[0];
            this.maxTransactionsCount = numberOfTransactions;
        }
    } // end map()

    @Override
    protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
        Text outputKey = new Text(this.maxCommodity);
        IntWritable outputValue = new IntWritable(this.maxTransactionsCount);

        context.write(outputKey, outputValue);
    } // end cleanup()

}
