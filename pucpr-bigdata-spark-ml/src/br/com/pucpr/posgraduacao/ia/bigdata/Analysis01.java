package br.com.pucpr.posgraduacao.ia.bigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import static org.apache.spark.sql.functions.col;

public class Analysis01 {

    public static final String KEY = "HS_CPF";

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("pucpr-bigdata-spark-ml").master("local[2]").getOrCreate();

        // Leitor de dados
        DataFrameReader df = session.read();

        // Lendo os dados
        Dataset<Row> basicos = df.option("header", "true").csv("in/ommlbd_basico.csv");
        Dataset<Row> empresarial = df.option("header", "true").csv("in/ommlbd_empresarial.csv");
        Dataset<Row> familiar = df.option("header", "true").csv("in/ommlbd_familiar.csv");
        Dataset<Row> regional = df.option("header", "true").csv("in/ommlbd_regional.csv");
        Dataset<Row> renda = df.option("header", "true").csv("in/ommlbd_renda.csv");

        Dataset<Row> unificado = basicos.join(empresarial, KEY).
                join(familiar, KEY).
                join(regional, KEY).
                join(renda, KEY);

        Dataset<Row> treinamento = unificado.filter(col("SAFRA").equalTo("TREINO"));
        System.out.println("Tamanho da base de treinamento: " + treinamento.count());

        Dataset<Row> teste = unificado.filter(col("SAFRA").equalTo("TESTE"));
        System.out.println("Tamanho da base de teste: " + teste.count());

        JavaRDD<Row> treinamentoRdd = treinamento.toJavaRDD();
        JavaRDD<Row> testeRdd = teste.toJavaRDD();

        /* Treinamento */
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{
                        "DISTZONARISCO",
                        "QTDENDERECO",
                        "QTDEMAIL",
                        "QTDCELULAR"
                })
                .setOutputCol("features");

        DecisionTreeClassifier regression = new DecisionTreeClassifier()
                .setLabelCol("TARGET")
                .setFeaturesCol("features");

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{
                assembler, regression
        });

        treinamento = treinamento.
                withColumn("DISTZONARISCO", col("DISTZONARISCO").cast("double")).
                withColumn("QTDENDERECO", col("QTDENDERECO").cast("double")).
                withColumn("QTDEMAIL", col("QTDEMAIL").cast("double")).
                withColumn("QTDCELULAR", col("QTDCELULAR").cast("double")).
                withColumn("TARGET", col("TARGET").cast("integer"));

        teste = teste.
                withColumn("DISTZONARISCO", col("DISTZONARISCO").cast("double")).
                withColumn("QTDENDERECO", col("QTDENDERECO").cast("double")).
                withColumn("QTDEMAIL", col("QTDEMAIL").cast("double")).
                withColumn("QTDCELULAR", col("QTDCELULAR").cast("double")).
                withColumn("TARGET", col("TARGET").cast("integer"));

        PipelineModel model = pipeline.fit(treinamento);

        Dataset<Row> predictions = model.transform(teste);

        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictions);

        // Precision by threshold
        JavaRDD<Tuple2<Object, Object>> precision = metrics.precisionByThreshold().toJavaRDD();
        System.out.println("Precision by threshold: " + precision.collect());
    }
}
