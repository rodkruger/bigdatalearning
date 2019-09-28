package br.com.pucpr.posgraduacao.ia.bigdata.analysis07;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * Analysis 07 - look for the maximum commercial code
 */
public class Analysis07 {

    /**
     * Main method. Instantiate the analysis class and run them
     *
     * @param args 1 - Input path for transactions file
     *             2 - Output path for the analysis
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Analysis07 analysis = new Analysis07(args);

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
     * Path to the maximum product code
     */
    private String maxCommCode;

    /**
     * Output path for the files
     */
    private String outputDirectory;

    public Analysis07(String args[]) throws IOException {
        this.configuration = new Configuration();

        String[] files = new GenericOptionsParser(this.configuration, args).getRemainingArgs();

        this.transactionsFilePath = files[0];
        this.outputDirectory = files[1];
        this.maxCommCode = this.outputDirectory + "/analysis07.csv";

        BasicConfigurator.configure();
    } // end Analysis07()

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
        Path output = new Path(this.maxCommCode);

        // job creation
        Job job = new Job(this.configuration, "analysis07-job");

        // set the mapper and reducer classes
        job.setJarByClass(Analysis07.class);
        job.setMapperClass(Analysis07Mapper.class);
        job.setReducerClass(Analysis07Reducer.class);

        // set the output classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set the files for input and output
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // execute the job and wait for its completion. the results are written in the final file, that will contain
        // the weight average of each commodity by year
        return job.waitForCompletion(true);
    } // end runAnalysis01()

} // end Analysis07
