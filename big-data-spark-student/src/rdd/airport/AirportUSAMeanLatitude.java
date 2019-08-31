package rdd.airport;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class AirportUSAMeanLatitude {

    // a regular expression which matches commas but not commas within double quotations
    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String args[]) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de 2 threads
        SparkConf conf = new SparkConf().setAppName("airport").setMaster("local[1]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // leitura do arquivo
        JavaRDD<String> aeroportos = sc.textFile("in/airports.text");

        // indice do pais em cada linha
        int ixPais = 3;
        int ixLatitude = 6;

        // Filtrando aeroportos no Brasil
        JavaRDD<String> aeroportosUsa = null;

        aeroportosUsa =
                aeroportos.filter(airport -> airport.split(COMMA_DELIMITER)[ixPais].equalsIgnoreCase("\"United States\""));

        JavaRDD<Double> latitudes =
                aeroportosUsa.map(airport -> Double.valueOf(airport.split(COMMA_DELIMITER)[ixLatitude]));

        double soma = latitudes.reduce((a, b) -> a + b);
        long n = latitudes.count();

        System.out.println(soma / n);
    }
}
