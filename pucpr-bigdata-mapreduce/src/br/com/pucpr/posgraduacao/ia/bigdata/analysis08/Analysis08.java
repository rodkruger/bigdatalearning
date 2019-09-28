package br.com.pucpr.posgraduacao.ia.bigdata.analysis08;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Analysis08 {

    /**
     * Main method. Instantiate the analysis class and run them
     *
     * @param args 1 - Input path for transactions file
     *             2 - Output path for the analysis
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Analysis08 analysis = new Analysis08(args);

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
     * Path to the most expensive commodity
     */
    private String mostExpensiveCommodity;

    /**
     * Output path for the files
     */
    private String outputDirectory;

    public Analysis08(String args[]) throws IOException {
        this.configuration = new Configuration();

        String[] files = new GenericOptionsParser(this.configuration, args).getRemainingArgs();

        this.transactionsFilePath = files[0];
        this.outputDirectory = files[1];
        this.mostExpensiveCommodity = this.outputDirectory + "/analysis08.csv";

        BasicConfigurator.configure();
    } // end Analysis08()

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
        Path output = new Path(this.mostExpensiveCommodity);

        // job creation
        Job job = new Job(this.configuration, "analysis08-job");

        // set the mapper and reducer classes
        job.setJarByClass(Analysis08.class);
        job.setMapperClass(Analysis08Mapper.class);
        job.setReducerClass(Analysis08Reducer.class);

        // set the output classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // set the files for input and output
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // execute the job and wait for its completion. the results are written in the final file, that will contain
        // the weight average of each commodity by year
        return job.waitForCompletion(true);
    } // end runAnalysis01()

} // end Analysis08
