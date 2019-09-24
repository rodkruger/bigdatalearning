package br.com.pucpr.posgraduacao.ia.bigdata.analysis07;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Find the maximum code in a list of key-pair values
 */
public class Analysis07Reducer extends Reducer<Text, Text, Text, Text> {

    private String maxCommCode;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        this.maxCommCode = "";
    } // end setup()

    @Override
    public void reduce(Text word, Iterable<Text> values, Context con)
            throws IOException, InterruptedException {

        for (Text value : values) {
            String commCode = value.toString();

            if (commCode.compareTo(maxCommCode) > 0) {
                this.maxCommCode = commCode;
            }
        }

    } // end reduce()

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        context.write(new Text("maxCommCode"), new Text(this.maxCommCode));
    } // end cleanup()

} // end Analysis07Reducer
