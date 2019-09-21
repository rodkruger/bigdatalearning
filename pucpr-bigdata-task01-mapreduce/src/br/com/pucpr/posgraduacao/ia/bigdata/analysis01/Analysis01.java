package br.com.pucpr.posgraduacao.ia.bigdata.analysis01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Analysis01 {

    public static void main(String[] args) throws Exception {

        Analysis01 analysis = new Analysis01(args);

        if (analysis.runAnalysis01()) {
            analysis.runAnalysis02();
        }
        
    }

    private Configuration configuration;

    private String transactionsFilePath;

    private String transactionsCountFilePath;

    private String outputDirectory;

    public Analysis01(String args[]) throws IOException {
        this.configuration = new Configuration();

        String[] files = new GenericOptionsParser(this.configuration, args).getRemainingArgs();

        this.transactionsFilePath = files[0];
        this.outputDirectory = files[1];
        this.transactionsCountFilePath = this.outputDirectory + "/analysis01.csv";

        BasicConfigurator.configure();
    }

    private boolean runAnalysis01() throws IOException, InterruptedException, ClassNotFoundException {
        // arquivo de entrada
        Path input = new Path(this.transactionsFilePath);

        // arquivo de saida
        Path output = new Path(this.transactionsCountFilePath);

        // criacao do job e seu nome
        Job j = new Job(this.configuration, "analysis01-job");

        // Incluir os Maps e Reduces
        // Registrar as classes

        j.setJarByClass(Analysis01.class);
        j.setMapperClass(Analysis01Mapper.class);
        j.setReducerClass(Analysis01Reducer.class);

        // Definindo os tipos de saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // Definindo arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        return j.waitForCompletion(true);
    }

    public boolean runAnalysis02() throws IOException, InterruptedException, ClassNotFoundException {
        // arquivo de entrada
        Path input = new Path(this.transactionsCountFilePath);

        // arquivo de saida
        Path output = new Path(this.outputDirectory + "/finalanalysis.csv");

        // criacao do job e seu nome
        Job j = new Job(this.configuration, "finalanalysis-job");

        // Incluir os Maps e Reduces
        // Registrar as classes
        j.setJarByClass(Analysis01_01Mapper.class);
        j.setMapperClass(Analysis01_01Mapper.class);

        // Definindo os tipos de saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // Definindo arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        return j.waitForCompletion(true);
    }
}