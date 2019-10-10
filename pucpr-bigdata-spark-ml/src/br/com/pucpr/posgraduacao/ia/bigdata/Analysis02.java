package br.com.pucpr.posgraduacao.ia.bigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import static org.apache.spark.sql.functions.col;

public class Analysis02 {

    public static final String KEY = "HS_CPF";

    public static final String[] FEATURES = new String[]{
            "TEMPOCPF", "DISTCENTROCIDADE", "ORIENTACAO_SEXUAL", "RELIGIAO", "DISTZONARISCO", "QTDENDERECO",
            "QTDEMAIL", "QTDCELULAR", "CELULARPROCON", "QTDFONEFIXO", "TELFIXOPROCON", "INDICEEMPREGO",
            "PORTEEMPREGADOR", "SOCIOEMPRESA", "FUNCIONARIOPUBLICO", "SEGMENTACAO", "SEGMENTACAOCOBRANCA",
            "SEGMENTACAOECOM", "SEGMENTACAOFIN", "SEGMENTACAOTELECOM", "QTDPESSOASCASA", "MENORRENDACASA",
            "MAIORRENDACASA", "SOMARENDACASA", "MEDIARENDACASA", "MAIORIDADECASA", "MENORIDADECASA",
            "MEDIAIDADECASA", "INDICMENORDEIDADE", "COBRANCABAIXOCASA", "COBRANCAMEDIOCASA", "COBRANCAALTACASA",
            "SEGMENTACAOFINBAIXACASA", "SEGMENTACAOFINMEDIACASA", "SEGMENTACAOALTACASA", "BOLSAFAMILIACASA",
            "FUNCIONARIOPUBLICOCASA", "IDADEMEDIACEP", "PERCENTMASCCEP", "PERCENTFEMCEP", "PERCENTANALFABETOCEP",
            "PERCENTPRIMARIOCEP", "PERCENTFUNDAMENTALCEP", "PERCENTMEDIOCEP", "PERCENTSUPERIORCEP",
            "PERCENTMESTRADOCEP", "PERCENTDOUTORADOCEP", "PERCENTBOLSAFAMILIACEP", "PERCENTFUNCIONARIOPUBLICOCEP",
            "MEDIARENDACEP", "PIBMUNICIPIO", "QTDUTILITARIOMUNICIPIO", "QTDAUTOMOVELMUNICIPIO", "QTDCAMINHAOMUNICIPIO",
            "QTDCAMINHONETEMUNICIPIO", "QTDMOTOMUNICIPIO", "PERCENTPOPZONAURBANA", "IDHMUNICIPIO",
            "ESTIMATIVARENDA", "QTDDECLARACAOISENTA", "QTDDECLARACAO10", "QTDDECLARACAOREST10",
            "QTDDECLARACAOPAGAR10", "RESTITUICAOAGENCIAALTARENDA", "BOLSAFAMILIA",
            "ANOSULTIMARESTITUICAO", "ANOSULTIMADECLARACAO", "ANOSULTIMADECLARACAOPAGAR"
    };

    public static void main(String args[]) {

        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        //------------------------------------------------------------------------------------------------------------
        // FIRST PART: Read the data, prepare the training and test datasets
        // Initializing the session
        SparkSession session = SparkSession.builder().appName("pucpr-bigdata-spark-ml").master("local[*]").getOrCreate();

        // Reading data
        DataFrameReader df = session.read();

        // Creating one Dataset for each part the data
        Dataset<Row> basicos = df.option("header", "true").csv("in/ommlbd_basico.csv");
        Dataset<Row> empresarial = df.option("header", "true").csv("in/ommlbd_empresarial.csv");
        Dataset<Row> familiar = df.option("header", "true").csv("in/ommlbd_familiar.csv");
        Dataset<Row> regional = df.option("header", "true").csv("in/ommlbd_regional.csv");
        Dataset<Row> renda = df.option("header", "true").csv("in/ommlbd_renda.csv");

        // Joining into one DataSet
        Dataset<Row> joinedDataset = basicos.join(empresarial, KEY).
                join(familiar, KEY).
                join(regional, KEY).
                join(renda, KEY);

        // Setting the column corresponding to the Label
        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("TARGET")
                .setOutputCol("indexedLabel")
                .fit(joinedDataset);

        // Setting the columns corresponding to Features
        VectorAssembler featureIndexer = new VectorAssembler()
                .setInputCols(FEATURES)
                .setOutputCol("indexedFeatures")
                .setHandleInvalid("keep");

        // Splitting the database into Training and Test data
        Dataset<Row> trainingDataset = joinedDataset.filter(col("SAFRA").equalTo("TREINO"));
        trainingDataset = setFeatureTypeToDouble(trainingDataset);

        Dataset<Row> testDataset = joinedDataset.filter(col("SAFRA").equalTo("TESTE"));
        testDataset = setFeatureTypeToDouble(testDataset);

        // Declaring a Decision Tree Classifier, and setting the corresponding columns of Label and Features
        LinearSVC dt = new LinearSVC()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures")
                .setMaxIter(10)
                .setRegParam(0.1);

        // Setting two new columns, corresponding to the predictions and predicted Label
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        //------------------------------------------------------------------------------------------------------------
        // SECOND PART: Chain the pipeline, train the classifier and test it

        // Chain the pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, dt, labelConverter});

        // Train the model
        PipelineModel model = pipeline.fit(trainingDataset);

        // Test the model
        Dataset<Row> predictions = model.transform(testDataset);

        //------------------------------------------------------------------------------------------------------------
        // THIRD PART: Analyze the predictions, and show the metrics

        // Extracting only the predicted result and compare to the true label
        Dataset<Row> predictionAndLabel =
                predictions.select("indexedLabel", "predictedLabel").
                        withColumn("predictedLabel", col("predictedLabel").cast("double")).
                        withColumn("indexedLabel", col("indexedLabel").cast("double"));

        // Creating a new Java Pair RDD of the information above, to put into the metrics
        JavaPairRDD<Object, Object> predictionAndLabels = predictionAndLabel.toJavaRDD().mapToPair(p ->
                new Tuple2<>(p.get(0), p.get(1)));

        // Get evaluation metrics
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());

        // Confusion matrix
        Matrix confusion = metrics.confusionMatrix();
        System.out.println("Confusion matrix: \n" + confusion);

        // Overall statistics
        System.out.println("Accuracy = " + metrics.accuracy());

        // Weighted stats
        System.out.format("Weighted precision = %f\n", metrics.weightedPrecision());
        System.out.format("Weighted recall = %f\n", metrics.weightedRecall());
        System.out.format("Weighted F1 score = %f\n", metrics.weightedFMeasure());
        System.out.format("Weighted false positive rate = %f\n", metrics.weightedFalsePositiveRate());

        // Get evaluation metrics
        BinaryClassificationMetrics binMetrics = new BinaryClassificationMetrics(predictionAndLabel);

        // AUPRC
        System.out.println("Area under precision-recall curve = " + binMetrics.areaUnderPR());

        // AUROC
        System.out.println("Area under ROC = " + binMetrics.areaUnderROC());

        // Precision by threshold
        JavaRDD<Tuple2<Object, Object>> precision = binMetrics.precisionByThreshold().toJavaRDD();
        System.out.println("Precision by threshold: " + precision.collect());

        // Recall by threshold
        JavaRDD<?> recall = binMetrics.recallByThreshold().toJavaRDD();
        System.out.println("Recall by threshold: " + recall.collect());

        // F Score by threshold
        JavaRDD<?> f1Score = binMetrics.fMeasureByThreshold().toJavaRDD();
        System.out.println("F1 Score by threshold: " + f1Score.collect());

        JavaRDD<?> f2Score = binMetrics.fMeasureByThreshold(2.0).toJavaRDD();
        System.out.println("F2 Score by threshold: " + f2Score.collect());

        // Precision-recall curve
        JavaRDD<?> prc = binMetrics.pr().toJavaRDD();
        System.out.println("Precision-recall curve: " + prc.collect());

        // Thresholds
        JavaRDD<Double> thresholds = precision.map(t -> Double.parseDouble(t._1().toString()));

        // ROC Curve
        JavaRDD<?> roc = binMetrics.roc().toJavaRDD();
        System.out.println("ROC curve: " + roc.collect());

        // AUPRC
        System.out.println("Area under precision-recall curve = " + binMetrics.areaUnderPR());

        // AUROC
        System.out.println("Area under ROC = " + binMetrics.areaUnderROC());
    }

    public static Dataset setFeatureTypeToDouble(Dataset ds) {
        Dataset adjustedDs = ds;

        for (String featureName : FEATURES) {
            adjustedDs = adjustedDs.withColumn(featureName, col(featureName).cast("double"));
        }

        return adjustedDs;
    }
}
