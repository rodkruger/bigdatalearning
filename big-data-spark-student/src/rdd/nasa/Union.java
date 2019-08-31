package rdd.nasa;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Union {
    public static void main(String args[]) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de 2 threads
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[1]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando arquivos originais
        JavaRDD<String> nasaA = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> nasaB = sc.textFile("in/nasa_19950801.tsv");

        // removendo o header do arquivo B
        String cabecalho = nasaB.first();
        nasaB = nasaB.filter(line -> !line.equalsIgnoreCase(cabecalho));

        JavaRDD<String> uniao = nasaA.union(nasaB);
        uniao.saveAsTextFile("output/nasacombinado.tsv");
    }
}
