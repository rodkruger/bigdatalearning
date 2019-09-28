package br.com.pucpr.posgraduacao.ia.bigdata.analysis08;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Looks for the most expensive commodity per weight
 */
public class Analysis08Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    /**
     * Most expensive commodity
     */
    private String mostExpensiveCommodity;

    /**
     * Price per weight of the most expensive commodity
     */
    private Double pricePerWeight;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        this.mostExpensiveCommodity = "";
        this.pricePerWeight = 0.0d;
    } // end setup()

    @Override
    public void reduce(Text word, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        for (DoubleWritable value : values) {

            if (value.get() > this.pricePerWeight) {
                this.pricePerWeight = value.get();
                this.mostExpensiveCommodity = word.toString();
            }
        }

    } // end reduce()

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        context.write(new Text(this.mostExpensiveCommodity), new DoubleWritable(this.pricePerWeight));
    } // end cleanup()

} // end Analysis08Reducer
