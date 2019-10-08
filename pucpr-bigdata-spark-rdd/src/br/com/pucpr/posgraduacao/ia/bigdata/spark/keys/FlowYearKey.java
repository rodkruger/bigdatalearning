package br.com.pucpr.posgraduacao.ia.bigdata.spark.keys;

import java.io.Serializable;
import java.util.Objects;

/**
 * Key to group transactions by flow and year
 */
public class FlowYearKey implements Serializable, Comparable {

    /**
     * A flow
     */
    private String flow;

    /**
     * An year
     */
    private String year;

    public FlowYearKey() {

    }

    public FlowYearKey(String flow, String year) {
        this.flow = flow;
        this.year = year;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlowYearKey that = (FlowYearKey) o;
        return Objects.equals(flow, that.flow) &&
                Objects.equals(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flow, year);
    }

    @Override
    public String toString() {
        return getFlow().concat("/").concat(getYear());
    }

    @Override
    public int compareTo(Object o) {
        return this.toString().compareTo(o.toString());
    }
}
