package br.com.pucpr.posgraduacao.ia.bigdata.analysis09;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * Count all transactions grouped by flow/year
 */
public class Analysis09 {

    /**
     * Main method. Instantiate the analysis class and run them
     *
     * @param args 1 - Input path for transactions file
     *             2 - Output path for the analysis
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Analysis09 analysis = new Analysis09(args);

        System.exit(analysis.runAnalysis01() ? 0 : 1);

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
     * Path to the transactions by flow and year file
     */
    private String transactionsByFlowAndYear;

    /**
     * Output path for the files
     */
    private String outputDirectory;

    public Analysis09(String args[]) throws IOException {
        this.configuration = new Configuration();

        String[] files = new GenericOptionsParser(this.configuration, args).getRemainingArgs();

        this.transactionsFilePath = files[0];
        this.outputDirectory = files[1];
        this.transactionsByFlowAndYear = this.outputDirectory + "/analysis09.csv";

        BasicConfigurator.configure();

    } // end Analysis09()

    /**
     * Executes a select to find the maximum product code
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
        Path output = new Path(this.transactionsByFlowAndYear);

        // job creation
        Job job = new Job(this.configuration, "analysis09-job");

        // set the mapper and reducer classes
        job.setJarByClass(Analysis09.class);
        job.setMapperClass(Analysis09Mapper.class);
        job.setReducerClass(Analysis09Reducer.class);

        // set the output classes
        job.setMapOutputKeyClass(FlowPerYear.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(FlowPerYear.class);
        job.setOutputValueClass(IntWritable.class);

        // set the files for input and output
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // execute the job and wait for its completion. the results are written in the final file, that will contain
        // the weight average of each commodity by year
        return job.waitForCompletion(true);
    } // end runAnalysis01()

} // end Analysis09
