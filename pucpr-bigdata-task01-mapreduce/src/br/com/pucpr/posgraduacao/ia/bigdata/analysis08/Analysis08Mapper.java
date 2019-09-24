package br.com.pucpr.posgraduacao.ia.bigdata.analysis08;

import br.com.pucpr.posgraduacao.ia.bigdata.constants.TransactionsConstants;
import br.com.pucpr.posgraduacao.ia.bigdata.enums.TransactionColsEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Group the trade use per weight
 */
public class Analysis08Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

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
        String commodity = values[TransactionColsEnum.COMMODITY.getValue()];

        String tradeUsd = values[TransactionColsEnum.TRADEUSD.getValue()];

        String weight = values[TransactionColsEnum.WEIGHTKG.getValue()];

        // converting the values
        double dTradeUsd = StringUtils.isEmpty(tradeUsd) ? 0.0d : Double.valueOf(tradeUsd);

        double dWeight = StringUtils.isEmpty(weight) ? 0.0d : Double.valueOf(weight);

        double pricePerWeight = dWeight != 0.0d ? dTradeUsd / dWeight : 0.0d;

        // write to the context
        context.write(new Text(commodity), new DoubleWritable(pricePerWeight));

    } // end map()

} // end Analysis08Mapper
