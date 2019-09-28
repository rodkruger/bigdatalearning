package br.com.pucpr.posgraduacao.ia.bigdata.analysis02;

import br.com.pucpr.posgraduacao.ia.bigdata.constants.TransactionsConstants;
import br.com.pucpr.posgraduacao.ia.bigdata.enums.TransactionColsEnum;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Collect all years occured in all transactions
 */
public class Analysis02Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // read the line in String format
        String line = value.toString();

        // ignore the headline
        if (line.startsWith(TransactionsConstants.HEADLINE)) {
            return;
        }

        // splitting by semicolon (.csv format used)
        String[] values = line.split(";");

        String year = values[TransactionColsEnum.YEAR.getValue()];

        Text outputKey = new Text(year);
        IntWritable outputValue = new IntWritable(1);

        context.write(outputKey, outputValue);

    } // end map()

} // fim Analysis02Mapper
