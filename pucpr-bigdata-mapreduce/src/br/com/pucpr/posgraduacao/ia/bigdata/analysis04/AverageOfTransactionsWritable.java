package br.com.pucpr.posgraduacao.ia.bigdata.analysis04;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Sum all the weights and counts the occurences of an specific key
 */
public class AverageOfTransactionsWritable implements Writable {

    /**
     * number of transactions
     */
    private int count;

    /**
     * sum of weights
     */
    private double sum;

    public AverageOfTransactionsWritable() {

    }

    public AverageOfTransactionsWritable(int count, double sum) {
        this.count = count;
        this.sum = sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(this.count));
        dataOutput.writeUTF(String.valueOf(this.sum));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.count = Integer.valueOf(dataInput.readUTF());
        this.sum = Double.valueOf(dataInput.readUTF());
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return "AverageOfTransactions{" +
                "count=" + count +
                ", sum=" + sum +
                '}';
    }
}