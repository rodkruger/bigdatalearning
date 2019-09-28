package br.com.pucpr.posgraduacao.ia.bigdata.analysis03;

import br.com.pucpr.posgraduacao.ia.bigdata.constants.TransactionsConstants;
import br.com.pucpr.posgraduacao.ia.bigdata.enums.TransactionColsEnum;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper used to lookup all transactions occured at 2016 and in Brazil
 */
public class Analysis03Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        // read the line in String format
        String line = value.toString();

        // ignore the headline
        if (line.startsWith(TransactionsConstants.HEADLINE)) {
            return;
        }

        // splitting by semicolon (.csv format used)
        String[] values = line.split(";");

        String country = values[TransactionColsEnum.COUNTRY.getValue()];

        if ("Brazil".equals(country)) {

            String year = values[TransactionColsEnum.YEAR.getValue()];

            if ("2016".equals(year)) {

                String commodity = values[TransactionColsEnum.COMMODITY.getValue()];

                Text outputKey = new Text(commodity);
                IntWritable outputValue = new IntWritable(1);

                context.write(outputKey, outputValue);
            }
        }
    } // end map()

} // end Analysis03Mapper
