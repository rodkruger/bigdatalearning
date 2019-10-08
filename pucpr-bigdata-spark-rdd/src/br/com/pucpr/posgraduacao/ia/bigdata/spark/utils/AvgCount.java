package br.com.pucpr.posgraduacao.ia.bigdata.spark.utils;

import java.io.Serializable;

/**
 * Compute an average of transactions
 */
public class AvgCount implements Serializable {

    /**
     * Counting of values
     */
    private int count;

    /**
     * Sum of all values
     */
    private double sum;

    public AvgCount() {

    }

    public AvgCount(int count, double sum) {
        this.count = count;
        this.sum = sum;
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
    
} // end AvgCount
