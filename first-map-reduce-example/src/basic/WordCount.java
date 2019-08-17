package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

import java.io.FileOutputStream;
import java.io.IOException;


public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount-professor");

        // Incluir os Maps e Reduces

        // Registrar as classes
        j.setJarByClass(WordCount.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);

        // Definindo os tipos de saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // Definindo arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // Formato geral
    // Mapper<Tipo da chave de entrada,
    //        Tipo da entrada,
    //        Tipo da chave de saída,
    //        Tipo da saída>
    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Obtendo o bloco em formato de String
            String line = value.toString();

            // Divisão por espaços
            String[] palavras = line.split(" ");

            for (String palavra : palavras) {
                // Emitir um (chave, valor)
                // chave = palavra
                // valor = 1

                palavra = palavra.replace(",", "");
                palavra = palavra.replace(".", "");

                Text chaveSaida = new Text(palavra);
                IntWritable valorSaida = new IntWritable(1);

                con.write(chaveSaida, valorSaida);
            }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable valor : values) {
                sum += valor.get();
            }

            // Escrever no contexto
            con.write(word, new IntWritable(sum));
        }
    }
}