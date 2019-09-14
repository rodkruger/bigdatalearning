package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class HousePriceSQL {

    private static final String PRICE = "Price";
    private static final String PRICE_SQ_FT = "Price SQ Ft";

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("houseprice").master("local[2]").getOrCreate();

        // Leitor de dados
        DataFrameReader df = session.read();

        // Lendo os dados
        Dataset<Row> dados = df.option("header", "true").csv("in/RealEstate.csv");

        // Tratando o tipo de dados das colunas
        dados = dados.
                withColumn(PRICE, col(PRICE).cast("double")).
                withColumn(PRICE_SQ_FT, col(PRICE_SQ_FT).cast("double"));

        RelationalGroupedDataset grpLoc = dados.groupBy("Location");

        Dataset<Row> dadosAgg;

        dadosAgg = grpLoc.
                agg(max(PRICE), avg(PRICE_SQ_FT)).
                orderBy(col("avg(Price SQ Ft)").desc());

        dadosAgg.show(20);

        // Fechar sessao
        session.stop();

    }
}
