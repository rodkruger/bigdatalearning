package br.com.pucpr.posgraduacao.ia.bigdata.analysis02;

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
 * Analysis 02 - group quantity of financial transactions per year
 */
public class Analysis02 {

    /**
     * Main method. Instantiate the analysis class and run them
     *
     * @param args 1 - Input path for transactions file
     *             2 - Output path for the analysis
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Analysis02 analysis = new Analysis02(args);

        analysis.runAnalysis01();

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
     * Transactions per year file
     */
    private String transactionsYearFilePath;

    /**
     * Output path for the files
     */
    private String outputDirectory;

    public Analysis02(String args[]) throws IOException {
        this.configuration = new Configuration();

        String[] files = new GenericOptionsParser(this.configuration, args).getRemainingArgs();

        this.transactionsFilePath = files[0];
        this.outputDirectory = files[1];
        this.transactionsYearFilePath = this.outputDirectory + "/analysis02.csv";

        BasicConfigurator.configure();
    } // end Analysis02()

    /**
     * Executes a grouping of transactions per year
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
        Path output = new Path(this.transactionsYearFilePath);

        // job creation
        Job job = new Job(this.configuration, "analysis02-job");

        // set the mapper and reducer classes
        job.setJarByClass(Analysis02.class);
        job.setMapperClass(Analysis02Mapper.class);
        job.setReducerClass(Analysis02Reducer.class);

        // set the output classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set the files for input and output
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // execute the job and wait for its completion. the results are written in the final file, that will contain
        // the number of transactions per year
        return job.waitForCompletion(true);
    } // end runAnalysis01()

}
