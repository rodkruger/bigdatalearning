package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class StronglyTypedStackOverflow {

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("houseprice").master("local[2]").getOrCreate();

        // Carregando os dados
        Dataset<Row> dados =
                session.read().
                        option("header", "true").
                        csv("in/2016-stack-overflow-survey-responses.csv");

        // Obtendo apenas as colunas de interesse
        Dataset<Row> selected =
                dados.select(col("country"),
                        col("age_midpoint").as("ageMidPoint").cast("double"),
                        col("salary_midpoint").as("salaryMidPoint").cast("double"));

        // Convertendo para um Dataset<Response>

        Dataset<Response> convertido = selected.as(Encoders.bean(Response.class));

        convertido.filter(r -> r.getCountry().equals("Brazil"));
        convertido.show();
    }

}
