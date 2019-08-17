package advanced.customwritable;

import basic.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "forestfire-estudante");

        // Registramos as classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        // regitrar os tipos de saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FireAvgTempWritable.class);

        // Definindo arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // lendo uma linha
            String linha = value.toString();

            // quebrando em colunas
            String[] colunas = linha.split(",");

            // obtendo a temperatura
            float temperatura = Float.parseFloat(colunas[8]);

            // criando o valor
            FireAvgTempWritable valor = new FireAvgTempWritable(1, temperatura);

            // mandando para o reduce
            con.write(new Text("media"), valor);
        }
    }

    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            float sumValores = 0;
            float sumN = 0;

            for (FireAvgTempWritable vlr : values) {
                sumN += vlr.getN();
                sumValores += vlr.getSoma();
            }

            float media = sumValores / sumN;

            // escrevendo a saida
            con.write(new Text("media"), new FloatWritable(media));
        }
    }

}
