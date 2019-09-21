package br.com.pucpr.posgraduacao.ia.bigdata.analysis01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Analysis01Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context con)
            throws IOException, InterruptedException {

        // Obtendo o bloco em formato de String
        String line = value.toString();

        // Divisão por espaços
        String[] info = line.split(";");

        String pais = info[0];

        if ("Brazil".equals(pais)) {
            String mercadoria = info[3];

            Text chaveSaida = new Text(mercadoria);
            IntWritable valorSaida = new IntWritable(1);

            con.write(chaveSaida, valorSaida);
        }
    }
}