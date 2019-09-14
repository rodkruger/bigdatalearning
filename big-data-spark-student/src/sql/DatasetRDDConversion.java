package sql;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.codehaus.janino.Java;

import static org.apache.spark.sql.functions.col;

public class DatasetRDDConversion {

    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de 2 threads
        SparkConf conf = new SparkConf().setAppName("houseprice").setMaster("local[2]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("houseprice").master("local[2]").getOrCreate();

        // Lendo arquivo em um RDD
        JavaRDD<String> linhas =
                sc.textFile("in/2016-stack-overflow-survey-responses.csv");

        linhas = linhas.filter(l -> !l.split(COMMA_DELIMITER, -1)[2].equals("country"));

        // criando RDD<Response>
        JavaRDD<Response> rddResponse =
                linhas.map(l -> {
                    String[] valores = l.split(COMMA_DELIMITER, -1);
                    String pais = valores[2];
                    double idade = NumberUtils.toDouble(valores[6]);
                    double salario = NumberUtils.toDouble(valores[14]);
                    return new Response(pais, idade, salario);
                });

        // RDD -> Dataset<Response>
        Dataset<Response> dataset =
                session.createDataset(rddResponse.rdd(), Encoders.bean(Response.class));

        dataset.printSchema();
        dataset.show();

        // Dataset<Response> -> RDD
        JavaRDD<Response> rddConvertido = dataset.toJavaRDD();
        for (Response r : rddConvertido.collect()) {
            System.out.println(r);
        }

    }
}