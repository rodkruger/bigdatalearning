package br.com.pucpr.posgraduacao.ia.bigdata.analysis01;

import br.com.pucpr.posgraduacao.ia.bigdata.enums.TransactionColsEnum;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper used to lookup all transactions occurred in Brazil
 */
public class Analysis01Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // read the line in String format
        String line = value.toString();

        // splitting by semicolon (.csv format used)
        String[] values = line.split(";");

        String country = values[TransactionColsEnum.COUNTRY.getValue()];

        if ("Brazil".equals(country)) {
            String commodity = values[TransactionColsEnum.COMMODITY.getValue()];

            Text outputKey = new Text(commodity);
            IntWritable outputValue = new IntWritable(1);

            context.write(outputKey, outputValue);
        }
    } // end map()

} // end Analysis01Mapper