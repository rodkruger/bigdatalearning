package pairrdd.aggregation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class WordCountReduceByKey {

    public static void main(String args[]) {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[*]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando o arquivo
        JavaRDD<String> linhas = sc.textFile("in/word_count.text");

        // quebrando em palavras
        JavaRDD<String> words = linhas.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // conversao para pairRDD
        JavaPairRDD<String, Integer> ocorrencias =
                words.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

        // calculando a soma das ocorrencias por palavra
        JavaPairRDD<String, Integer> contagem = ocorrencias.reduceByKey((x, y) -> x + y);

        contagem.saveAsTextFile("output/contagem_reduce.text");
    }
}
