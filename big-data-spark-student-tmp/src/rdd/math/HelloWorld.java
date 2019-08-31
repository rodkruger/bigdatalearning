package rdd.math;

import javafx.application.Application;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HelloWorld {

    public static void main(String args[]) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de todas as threads
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[*]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // leitura de dados
        JavaRDD<String> lines = sc.textFile("in/word_count.text");

        // quebrando em palavras
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // contagem
        Map<String, Long> contagemPorPalavra = words.countByValue();

        // apresentar resultados
        for (Map.Entry<String, Long> cnt : contagemPorPalavra.entrySet()) {
            System.out.println(cnt.getKey() + " - " + cnt.getValue());
        }
    }
}