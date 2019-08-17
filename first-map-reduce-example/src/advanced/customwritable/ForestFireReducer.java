package advanced.customwritable;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ForestFireReducer extends Reducer<Text, ForestFireWritable, Text, ForestFireWritable> {

    public void reduce(Text key,
                       Iterable<ForestFireWritable> values,
                       Context context) throws IOException, InterruptedException {

        float maxVento = 0;
        float maxTemperatura = 0;

        for (ForestFireWritable vlr : values) {

            if (vlr.getVento() > maxVento) {
                maxVento = vlr.getVento();
            }

            if (vlr.getTemperatura() > maxTemperatura) {
                maxTemperatura = vlr.getTemperatura();
            }
        }

        ForestFireWritable valor = new ForestFireWritable(maxVento, maxTemperatura, key.toString());

        // escrevendo a saida
        context.write(key, valor);
    }
}


