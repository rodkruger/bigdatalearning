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

/**
 * Analysis 01 - find the commodity with more transactions
 */
public class Analysis01 {

    /**
     * Main method. Instantiate the analysis class and run them
     *
     * @param args 1 - Input path for transactions file
     *             2 - Output path for the analysis
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Analysis01 analysis = new Analysis01(args);

        if (analysis.runAnalysis01()) {
            analysis.runAnalysis02();
        }

    } // end main()

    /**
     * Map Reduce configuration
     */
    private Configuration configuration;

    /**
     * Path to the transactions file
     */
    private String transactionsFilePath;

    /**
     * Path to the transactions per commodity
     */
    private String transactionsCountFilePath;

    /**
     * Output path for the files
     */
    private String outputDirectory;

    public Analysis01(String args[]) throws IOException {
        this.configuration = new Configuration();

        String[] files = new GenericOptionsParser(this.configuration, args).getRemainingArgs();

        this.transactionsFilePath = files[0];
        this.outputDirectory = files[1];
        this.transactionsCountFilePath = this.outputDirectory + "/analysis01.csv";

        BasicConfigurator.configure();
    } // end Analysis01()

    /**
     * Executes a counting by all commodities in Brazil
     *
     * @return true - job executed successfully / false - job not executed successfully
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public boolean runAnalysis01() throws IOException, InterruptedException, ClassNotFoundException {
        // create the input path for the transactions file
        Path input = new Path(this.transactionsFilePath);

        // create the output path for the transactions count file
        Path output = new Path(this.transactionsCountFilePath);

        // job creation
        Job job = new Job(this.configuration, "analysis01-job");

        // set the mapper and reducer classes
        job.setJarByClass(Analysis01.class);
        job.setMapperClass(Analysis01Mapper.class);
        job.setReducerClass(Analysis01Reducer.class);

        // set the output classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set the files for input and output
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // execute the job and wait for its completion. the results are written in an intermediate file, that
        // will be analyzed for another job in the sequence
        return job.waitForCompletion(true);
    } // end runAnalysis01()

    /**
     * Executes a select for the commodity with more transactions
     *
     * @return true - job executed successfully / false - job not executed successfully
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public boolean runAnalysis02() throws IOException, InterruptedException, ClassNotFoundException {
        // create the input path for the transactions count file
        Path input = new Path(this.transactionsCountFilePath);

        // create the output path for the commodity with more transactions
        Path output = new Path(this.outputDirectory + "/analysis01_01.csv");

        // criacao do job e seu nome
        Job job = new Job(this.configuration, "analysis01_01-job");

        // set the mapper and reducer classes
        job.setJarByClass(Analysis01_01Mapper.class);
        job.setMapperClass(Analysis01_01Mapper.class);

        // set the output classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set the files for input and output
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // execute the job and wait for its completion. the results are written in the final file, that will contain
        // the commodity with more transactions
        return job.waitForCompletion(true);
    } // end runAnalysis02()

} // end Analysis01