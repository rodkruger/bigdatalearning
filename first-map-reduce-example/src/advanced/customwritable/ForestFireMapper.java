package advanced.customwritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ForestFireMapper extends Mapper<Object, Text, Text, ForestFireWritable> {

    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {

        // lendo uma linha
        String linha = value.toString();

        // quebrando em colunas
        String[] colunas = linha.split(",");

        // obtendo a temperatura
        float vento = Float.parseFloat(colunas[10]);
        float temperatura = Float.parseFloat(colunas[8]);
        String mes = colunas[2];

        // criando o valor
        ForestFireWritable valor = new ForestFireWritable(vento, temperatura, mes);

        // mandando para o reduce
        context.write(new Text(mes), valor);
    }
}