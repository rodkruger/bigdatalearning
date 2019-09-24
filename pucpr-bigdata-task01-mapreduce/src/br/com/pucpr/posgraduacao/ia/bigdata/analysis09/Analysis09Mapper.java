package br.com.pucpr.posgraduacao.ia.bigdata.analysis09;

import br.com.pucpr.posgraduacao.ia.bigdata.constants.TransactionsConstants;
import br.com.pucpr.posgraduacao.ia.bigdata.enums.TransactionColsEnum;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper used to group by flow and year
 */
public class Analysis09Mapper extends Mapper<LongWritable, Text, FlowPerYear, IntWritable> {

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
        String flow = values[TransactionColsEnum.FLOW.getValue()];

        String year = values[TransactionColsEnum.YEAR.getValue()];

        // create the key-pair value
        FlowPerYear outputKey = new FlowPerYear(flow, year);

        context.write(outputKey, new IntWritable(1));

    } // end map()

} // end Analysis09Mapper
