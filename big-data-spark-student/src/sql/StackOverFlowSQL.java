package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class StackOverFlowSQL {

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("stackoverflow").master("local[2]").getOrCreate();

        // Leitor de dados
        DataFrameReader df = session.read();

        // Lendo os dados
        Dataset<Row> dados = df.option("header", "true").csv("in/2016-stack-overflow-survey-responses.csv");

        // Visualizando o schema
        // dados.printSchema();

        // Visualizar o começo do arquivo
        // dados.show(10);

        // so_region, star_wars_vs_star_trek
        // Dataset<Row> selecionado = dados.select(col("so_region"), col("star_wars_vs_star_trek"));
        // selecionado.show(5);

        // Filtrando pelo pais (Brasil)
        // Dataset<Row> filter = dados.filter(col("country").equalTo("Brazil"));
        // filter.show(5);

        dados = dados.
                withColumn("salary_midPoint", col("salary_midPoint").cast("integer")).
                withColumn("age_midpoint", col("age_midpoint").cast("integer"));

        // dados.printSchema();

        // Filtrando com idade menor que 20
        // Dataset<Row> menorVinte = dados.filter(col("age_midpoint").$less(20));

        // menorVinte.show(20);

        // Ordenando pelo salario
        // dados.orderBy(col("salary_midPoint").desc()).show(20);

        // Agrupamento
        RelationalGroupedDataset grpPais = dados.groupBy("country");
        grpPais.agg(avg(col("salary_midPoint"))).orderBy(col("avg(salary_midPoint)").desc()).show(20);

        // Fechar sessao
        session.stop();
    }
}