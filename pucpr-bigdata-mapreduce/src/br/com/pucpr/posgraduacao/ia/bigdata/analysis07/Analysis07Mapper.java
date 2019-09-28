package br.com.pucpr.posgraduacao.ia.bigdata.analysis07;

import br.com.pucpr.posgraduacao.ia.bigdata.constants.TransactionsConstants;
import br.com.pucpr.posgraduacao.ia.bigdata.enums.TransactionColsEnum;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Maps the maximum commercial code found in the base
 */
public class Analysis07Mapper extends Mapper<LongWritable, Text, Text, Text> {

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

        // read the values
        String commCode = values[TransactionColsEnum.COMMCODE.getValue()];

        context.write(new Text(commCode), new Text(commCode));
    } // end map()

} // end Analysis07Mapper
