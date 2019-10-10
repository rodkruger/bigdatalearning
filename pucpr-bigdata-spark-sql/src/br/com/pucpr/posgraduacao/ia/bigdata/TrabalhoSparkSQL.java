package br.com.pucpr.posgraduacao.ia.bigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.commons.configuration.PropertyConverter.toDouble;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class TrabalhoSparkSQL {

    public static void main(String args[]) {

        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("omml").master("local[2]").getOrCreate();

        DataFrameReader dfr = session.read();

        Dataset<Row> basico = dfr.option("header", "true").
                csv("in/ommlbd_basico.csv");

        Dataset<Row> empresarial = dfr.option("header", "true").
                csv("in/ommlbd_empresarial.csv");

        Dataset<Row> renda = dfr.option("header", "true").
                csv("in/ommlbd_renda.csv");

        Dataset<Row> familiar = dfr.option("header", "true").
                csv("in/ommlbd_familiar.csv");

        Dataset<Row> regional = dfr.option("header", "true").
                csv("in/ommlbd_regional.csv");

        //visualizando o schema
        //dados.printSchema();

        //casting
        basico = basico.withColumn("TEMPOCPF", col("TEMPOCPF").cast("integer"))
                .withColumn("DISTCENTROCIDADE", col("DISTCENTROCIDADE").cast("integer"))
                .withColumn("DISTZONARISCO", col("DISTZONARISCO").cast("integer"))
                .withColumn("QTDENDERECO", col("QTDENDERECO").cast("integer"))
                .withColumn("QTDEMAIL", col("QTDEMAIL").cast("integer"))
                .withColumn("QTDCELULAR", col("QTDCELULAR").cast("integer"))
                .withColumn("QTDFONEFIXO", col("QTDFONEFIXO").cast("integer"));

        empresarial = empresarial.withColumn("INDICEEMPREGO", col("INDICEEMPREGO").cast("integer"))
                .withColumn("PORTEEMPREGADOR", col("PORTEEMPREGADOR").cast("integer"))
                .withColumn("SEGMENTACAO", col("SEGMENTACAO").cast("integer"))
                .withColumn("SEGMENTACAOCOBRANCA", col("SEGMENTACAOCOBRANCA").cast("integer"))
                .withColumn("SEGMENTACAOECOM", col("SEGMENTACAOECOM").cast("integer"))
                .withColumn("SEGMENTACAOFIN", col("SEGMENTACAOFIN").cast("integer"))
                .withColumn("SEGMENTACAOTELECOM", col("SEGMENTACAOTELECOM").cast("integer"));

        renda = renda.withColumn("ESTIMATIVARENDA", col("ESTIMATIVARENDA").cast("integer"))
                .withColumn("QTDDECLARACAOISENTA", col("QTDDECLARACAOISENTA").cast("integer"))
                .withColumn("QTDDECLARACAO10", col("QTDDECLARACAO10").cast("integer"))
                .withColumn("QTDDECLARACAOREST10", col("QTDDECLARACAOREST10").cast("integer"))
                .withColumn("QTDDECLARACAOPAGAR10", col("QTDDECLARACAOPAGAR10").cast("integer"))
                .withColumn("ANOSULTIMARESTITUICAO", col("ANOSULTIMARESTITUICAO").cast("integer"))
                .withColumn("ANOSULTIMADECLARACAO", col("ANOSULTIMADECLARACAO").cast("integer"))
                .withColumn("ANOSULTIMADECLARACAOPAGAR", col("ANOSULTIMADECLARACAOPAGAR").cast("integer"));

        familiar = familiar.withColumn("QTDPESSOASCASA", col("QTDPESSOASCASA").cast("integer"))
                .withColumn("MENORRENDACASA", col("MENORRENDACASA").cast("integer"))
                .withColumn("MAIORRENDACASA", col("MAIORRENDACASA").cast("integer"))
                .withColumn("SOMARENDACASA", col("SOMARENDACASA").cast("integer"))
                .withColumn("MEDIARENDACASA", col("MEDIARENDACASA").cast("integer"))
                .withColumn("MAIORIDADECASA", col("MAIORIDADECASA").cast("integer"))
                .withColumn("MENORIDADECASA", col("MENORIDADECASA").cast("integer"))
                .withColumn("MEDIAIDADECASA", col("MEDIAIDADECASA").cast("integer"));

        regional = regional.withColumn("IDADEMEDIACEP", col("IDADEMEDIACEP").cast("integer"))
                .withColumn("PERCENTMASCCEP", col("PERCENTMASCCEP").cast("integer"))
                .withColumn("PERCENTFEMCEP", col("PERCENTFEMCEP").cast("integer"))
                .withColumn("PERCENTANALFABETOCEP", col("PERCENTANALFABETOCEP").cast("integer"))
                .withColumn("PERCENTPRIMARIOCEP", col("PERCENTPRIMARIOCEP").cast("integer"))
                .withColumn("PERCENTFUNDAMENTALCEP", col("PERCENTFUNDAMENTALCEP").cast("integer"))
                .withColumn("PERCENTMEDIOCEP", col("PERCENTMEDIOCEP").cast("integer"))
                .withColumn("PERCENTSUPERIORCEP", col("PERCENTSUPERIORCEP").cast("integer"))
                .withColumn("PERCENTMESTRADOCEP", col("PERCENTMESTRADOCEP").cast("integer"))
                .withColumn("PERCENTDOUTORADOCEP", col("PERCENTDOUTORADOCEP").cast("integer"))
                .withColumn("PERCENTBOLSAFAMILIACEP", col("PERCENTBOLSAFAMILIACEP").cast("integer"))
                .withColumn("PERCENTFUNCIONARIOPUBLICOCEP", col("PERCENTFUNCIONARIOPUBLICOCEP").cast("integer"))
                .withColumn("MEDIARENDACEP", col("MEDIARENDACEP").cast("integer"))
                .withColumn("PIBMUNICIPIO", col("PIBMUNICIPIO").cast("integer"))
                .withColumn("QTDUTILITARIOMUNICIPIO", col("QTDUTILITARIOMUNICIPIO").cast("integer"))
                .withColumn("QTDAUTOMOVELMUNICIPIO", col("QTDAUTOMOVELMUNICIPIO").cast("integer"))
                .withColumn("QTDCAMINHAOMUNICIPIO", col("QTDCAMINHAOMUNICIPIO").cast("integer"))
                .withColumn("QTDCAMINHONETEMUNICIPIO", col("QTDCAMINHONETEMUNICIPIO").cast("integer"))
                .withColumn("QTDMOTOMUNICIPIO", col("QTDMOTOMUNICIPIO").cast("integer"))
                .withColumn("PERCENTPOPZONAURBANA", col("PERCENTPOPZONAURBANA").cast("integer"))
                .withColumn("IDHMUNICIPIO", col("IDHMUNICIPIO").cast("integer"));

        //familiar.printSchema();

        //EXERCICIO 1
        Dataset<Row> exercicio1_dataset = basico.select(col("SAFRA"), col("ORIENTACAO_SEXUAL"));

        RelationalGroupedDataset exercicio1 = exercicio1_dataset.groupBy("SAFRA", "ORIENTACAO_SEXUAL");

        exercicio1_dataset = exercicio1.agg(count(col("ORIENTACAO_SEXUAL"))).orderBy(col("SAFRA"), col("ORIENTACAO_SEXUAL"));
        exercicio1_dataset = exercicio1_dataset.coalesce(1);
        exercicio1_dataset.toJavaRDD().saveAsTextFile("out/exercicio1.csv");

        //EXERCICIO 2
        Dataset<Row> exercicio2_dataset = basico.select(col("SAFRA"), col("QTDEMAIL"));

        exercicio2_dataset = exercicio2_dataset.filter(col("QTDEMAIL").$greater(-1));
        exercicio2_dataset = exercicio2_dataset.coalesce(1);
        exercicio2_dataset.groupBy("SAFRA").max("QTDEMAIL").toJavaRDD().saveAsTextFile("out/exercicio2_1.csv");
        exercicio2_dataset.groupBy("SAFRA").min("QTDEMAIL").toJavaRDD().saveAsTextFile("out/exercicio2_2.csv");

        //EXERCICIO 3
        Dataset<Row> joined_exer3 = basico.join(renda, basico.col("HS_CPF").equalTo(renda.col("HS_CPF")), "left_outer");
        Dataset<Row> exercicio3_dataset = joined_exer3.select(col("SAFRA"), col("ESTIMATIVARENDA"));

        exercicio3_dataset = exercicio3_dataset.filter(col("ESTIMATIVARENDA").$greater(10000));

        RelationalGroupedDataset exercicio3 = exercicio3_dataset.groupBy("SAFRA");
        exercicio3_dataset = exercicio3.agg(count(col("ESTIMATIVARENDA"))).orderBy(col("SAFRA"));
        exercicio3_dataset = exercicio3_dataset.coalesce(1);
        exercicio3_dataset.toJavaRDD().saveAsTextFile("out/exercicio3.csv");

        //EXERCICIO 4
        Dataset<Row> exercicio4_dataset = joined_exer3.select(col("SAFRA"), col("BOLSAFAMILIA"));
        exercicio4_dataset = exercicio4_dataset.filter(col("BOLSAFAMILIA").equalTo(1));

        RelationalGroupedDataset exercicio4 = exercicio4_dataset.groupBy("SAFRA");
        exercicio4_dataset = exercicio4.agg(count(col("BOLSAFAMILIA"))).orderBy(col("SAFRA"));
        exercicio4_dataset = exercicio4_dataset.coalesce(1);
        exercicio4_dataset.toJavaRDD().saveAsTextFile("out/exercicio4.csv");

        //EXERCICIO 5
        Dataset<Row> joined_exer5 = basico.join(familiar, basico.col("HS_CPF").equalTo(familiar.col("HS_CPF")), "left_outer");
        Dataset<Row> exercicio5_dataset = joined_exer5.select(col("SAFRA"), col("FUNCIONARIOPUBLICOCASA"));

        Double percentageTESTE = (toDouble(exercicio5_dataset.filter(col("FUNCIONARIOPUBLICOCASA").equalTo(1))
                .filter(col("SAFRA").equalTo("TESTE")).count())
                / toDouble(exercicio5_dataset.filter(col("SAFRA").equalTo("TESTE")).count())) * 100;

        System.out.println("Percentual da base de teste: " + percentageTESTE);

        Double percentageTREINO = (toDouble(exercicio5_dataset.filter(col("FUNCIONARIOPUBLICOCASA").equalTo(1))
                .filter(col("SAFRA").equalTo("TREINO")).count())
                / toDouble(exercicio5_dataset.filter(col("SAFRA").equalTo("TREINO")).count())) * 100;

        System.out.println("Percentual da base de treino: " + percentageTREINO);

        //EXERCICIO 6
        Dataset<Row> joined_exer6 = basico.join(regional, basico.col("HS_CPF").equalTo(regional.col("HS_CPF")), "left_outer");
        Dataset<Row> exercicio6_dataset = joined_exer6.select(col("SAFRA"), col("IDHMUNICIPIO"));

        Double percTESTE010 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(0, 10))
                .filter(col("SAFRA").equalTo("TESTE")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TESTE")).count())) * 100;

        Double percTESTE1020 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(10, 20))
                .filter(col("SAFRA").equalTo("TESTE")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TESTE")).count())) * 100;

        Double percTESTE2030 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(20, 30))
                .filter(col("SAFRA").equalTo("TESTE")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TESTE")).count())) * 100;

        Double percTESTE3040 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(30, 40))
                .filter(col("SAFRA").equalTo("TESTE")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TESTE")).count())) * 100;

        Double percTESTE4050 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(40, 50))
                .filter(col("SAFRA").equalTo("TESTE")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TESTE")).count())) * 100;

        Double percTESTE5060 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(50, 60))
                .filter(col("SAFRA").equalTo("TESTE")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TESTE")).count())) * 100;

        Double percTESTE6070 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(60, 70))
                .filter(col("SAFRA").equalTo("TESTE")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TESTE")).count())) * 100;

        Double percTESTE7080 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(70, 80))
                .filter(col("SAFRA").equalTo("TESTE")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TESTE")).count())) * 100;

        Double percTESTE8090 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(80, 90))
                .filter(col("SAFRA").equalTo("TESTE")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TESTE")).count())) * 100;

        Double percTESTE90100 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(90, 100))
                .filter(col("SAFRA").equalTo("TESTE")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TESTE")).count())) * 100;

        System.out.println("Percentual do IDH de 0 a 10 da base de teste: " + percTESTE010);
        System.out.println("Percentual do IDH de 10 a 20 da base de teste: " + percTESTE1020);
        System.out.println("Percentual do IDH de 20 a 30 da base de teste: " + percTESTE2030);
        System.out.println("Percentual do IDH de 30 a 40 da base de teste: " + percTESTE3040);
        System.out.println("Percentual do IDH de 40 a 50 da base de teste: " + percTESTE4050);
        System.out.println("Percentual do IDH de 50 a 60 da base de teste: " + percTESTE5060);
        System.out.println("Percentual do IDH de 60 a 70 da base de teste: " + percTESTE6070);
        System.out.println("Percentual do IDH de 70 a 80 da base de teste: " + percTESTE7080);
        System.out.println("Percentual do IDH de 80 a 90 da base de teste: " + percTESTE8090);
        System.out.println("Percentual do IDH de 90 a 100 da base de teste: " + percTESTE90100);

        Double percTREINO010 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(0, 10))
                .filter(col("SAFRA").equalTo("TREINO")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TREINO")).count())) * 100;

        Double percTREINO1020 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(10, 20))
                .filter(col("SAFRA").equalTo("TREINO")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TREINO")).count())) * 100;

        Double percTREINO2030 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(20, 30))
                .filter(col("SAFRA").equalTo("TREINO")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TREINO")).count())) * 100;

        Double percTREINO3040 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(30, 40))
                .filter(col("SAFRA").equalTo("TREINO")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TREINO")).count())) * 100;

        Double percTREINO4050 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(40, 50))
                .filter(col("SAFRA").equalTo("TREINO")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TREINO")).count())) * 100;

        Double percTREINO5060 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(50, 60))
                .filter(col("SAFRA").equalTo("TREINO")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TREINO")).count())) * 100;

        Double percTREINO6070 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(60, 70))
                .filter(col("SAFRA").equalTo("TREINO")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TREINO")).count())) * 100;

        Double percTREINO7080 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(70, 80))
                .filter(col("SAFRA").equalTo("TREINO")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TREINO")).count())) * 100;

        Double percTREINO8090 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(80, 90))
                .filter(col("SAFRA").equalTo("TREINO")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TREINO")).count())) * 100;

        Double percTREINO90100 = (toDouble(exercicio6_dataset.filter(col("IDHMUNICIPIO").between(90, 100))
                .filter(col("SAFRA").equalTo("TREINO")).count())
                / toDouble(exercicio6_dataset.filter(col("SAFRA").equalTo("TREINO")).count())) * 100;

        System.out.println("Percentual do IDH de 0 a 10 da base de treino: " + percTREINO010);
        System.out.println("Percentual do IDH de 10 a 20 da base de treino: " + percTREINO1020);
        System.out.println("Percentual do IDH de 20 a 30 da base de treino: " + percTREINO2030);
        System.out.println("Percentual do IDH de 30 a 40 da base de treino: " + percTREINO3040);
        System.out.println("Percentual do IDH de 40 a 50 da base de treino: " + percTREINO4050);
        System.out.println("Percentual do IDH de 50 a 60 da base de treino: " + percTREINO5060);
        System.out.println("Percentual do IDH de 60 a 70 da base de treino: " + percTREINO6070);
        System.out.println("Percentual do IDH de 70 a 80 da base de treino: " + percTREINO7080);
        System.out.println("Percentual do IDH de 80 a 90 da base de treino: " + percTREINO8090);
        System.out.println("Percentual do IDH de 90 a 100 da base de treino: " + percTREINO90100);

        //EXERCICIO 7
        Dataset<Row> joined_exer7 = basico.join(renda, basico.col("HS_CPF").equalTo(renda.col("HS_CPF")), "left_outer");
        Dataset<Row> exercicio7_dataset = joined_exer7.select(col("SAFRA"), col("DISTZONARISCO"), col("ESTIMATIVARENDA"));
        Dataset<Row> exercicio7_filtrado = exercicio7_dataset.filter(col("DISTZONARISCO").$less(5000));
        exercicio7_filtrado.filter(col("ESTIMATIVARENDA").$greater(7000));

        RelationalGroupedDataset exercicio7 = exercicio7_filtrado.groupBy("SAFRA");
        exercicio7_filtrado = exercicio7.agg(count(col("ESTIMATIVARENDA"))).orderBy(col("SAFRA"));
        exercicio7_filtrado = exercicio7_filtrado.coalesce(1);
        exercicio7_filtrado.toJavaRDD().saveAsTextFile("out/exercicio7.csv");

        //EXERCICIO 8
        Dataset<Row> joined_exer8 = basico.join(renda, basico.col("HS_CPF").equalTo(renda.col("HS_CPF")), "left_outer")
                .join(empresarial, basico.col("HS_CPF").equalTo(empresarial.col("HS_CPF")), "left_outer");
        Dataset<Row> exercicio8_dataset = joined_exer8.select(col("SAFRA"), col("TARGET"),
                col("SOCIOEMPRESA"), col("ESTIMATIVARENDA"));
        Dataset<Row> exercicio8_filtrado = exercicio8_dataset.filter(col("ESTIMATIVARENDA").$greater(5000));

        RelationalGroupedDataset exercicio8 = exercicio8_filtrado.groupBy("SAFRA", "TARGET", "SOCIOEMPRESA");
        exercicio8_filtrado = exercicio8.agg(count(col("ESTIMATIVARENDA"))).orderBy(col("SAFRA"), col("TARGET"), col("SOCIOEMPRESA"));
        exercicio8_filtrado = exercicio8_filtrado.coalesce(1);
        exercicio8_filtrado.toJavaRDD().saveAsTextFile("out/exercicio8.csv");
    }
}
