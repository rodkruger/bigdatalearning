package br.com.pucpr.posgraduacao.ia.bigdata.analysis01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Analysis01_01Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private int numero_transacoes;

    private String mercadoria;

    @Override
    public void map(LongWritable key, Text value, Mapper.Context con)
            throws IOException, InterruptedException {

        // Obtendo o bloco em formato de String
        String line = value.toString();

        // Divisão por espaços
        String[] info = line.split("\t");

        int numero_transacoes = Integer.valueOf(info[info.length - 1]);

        if (numero_transacoes > this.numero_transacoes) {
            this.mercadoria = info[0];
            this.numero_transacoes = numero_transacoes;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Text chaveSaida = new Text(this.mercadoria);
        IntWritable valorSaida = new IntWritable(this.numero_transacoes);

        context.write(chaveSaida, valorSaida);
    }
}