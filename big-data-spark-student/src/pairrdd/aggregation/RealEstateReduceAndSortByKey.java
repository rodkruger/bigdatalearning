package pairrdd.aggregation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

public class RealEstateReduceAndSortByKey {

    public static void main(String args[]) {

        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("realEstate").setMaster("local[*]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando dados
        JavaRDD<String> linhas = sc.textFile("in/RealEstate.csv");

        // remover cabecalho
        linhas = linhas.filter(line -> !line.contains("Bedrooms"));

        // criar PRDD (# de quartos, <preco, ocorrencia>)
        int ixQuartos = 3;
        int ixPreco = 2;

        JavaPairRDD<Integer, AvgCount> pRDD = linhas.mapToPair(line -> new Tuple2<>(
                Integer.valueOf(line.split(",")[ixQuartos]),
                new AvgCount(1, Double.valueOf(line.split(",")[ixPreco])))
        );

        JavaPairRDD<Integer, AvgCount> reduced = pRDD.reduceByKey((x, y) ->
                new AvgCount(x.getContagem() + y.getContagem(), x.getValor() + y.getValor()));

        JavaPairRDD<Integer, Double> rddMedia = reduced.mapValues(vlr -> vlr.getValor() / vlr.getContagem());

        rddMedia = rddMedia.sortByKey();

        System.out.println("Preço médio por número de quartos");

        for (Tuple2<Integer, Double> rs : rddMedia.collect()) {
            System.out.println(rs._1() + "\t" + rs._2());
        }
    }
}