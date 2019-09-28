package br.com.pucpr.posgraduacao.ia.bigdata.analysis05;

import br.com.pucpr.posgraduacao.ia.bigdata.analysis04.AverageOfTransactionsWritable;
import br.com.pucpr.posgraduacao.ia.bigdata.analysis04.CommodityPerYear;
import br.com.pucpr.posgraduacao.ia.bigdata.constants.TransactionsConstants;
import br.com.pucpr.posgraduacao.ia.bigdata.enums.TransactionColsEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper used to group the weight by commodity and year, in Brazil
 */
public class Analysis05Mapper extends Mapper<LongWritable, Text, CommodityPerYear, AverageOfTransactionsWritable> {

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

        // read the values
        String country = values[TransactionColsEnum.COUNTRY.getValue()];

        if ("Brazil".equals(country)) {
            String commodity = values[TransactionColsEnum.COMMODITY.getValue()];

            String year = values[TransactionColsEnum.YEAR.getValue()];

            String weight = values[TransactionColsEnum.WEIGHTKG.getValue()];

            double dWeight = 0.0d;

            if (StringUtils.isNotEmpty(weight)) {
                dWeight = Double.valueOf(weight);
            }

            // create the key-pair value
            CommodityPerYear outputKey = new CommodityPerYear(commodity, year);
            AverageOfTransactionsWritable outputValue = new AverageOfTransactionsWritable(1, dWeight);

            context.write(outputKey, outputValue);
        }

    } // end map()

} // end Analysis05Mapper
