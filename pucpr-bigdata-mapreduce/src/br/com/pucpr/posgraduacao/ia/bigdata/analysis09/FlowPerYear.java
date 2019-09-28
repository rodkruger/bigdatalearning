package br.com.pucpr.posgraduacao.ia.bigdata.analysis09;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Group by flow and year
 */
public class FlowPerYear implements WritableComparable<FlowPerYear> {

    /**
     * A flow
     */
    private String flow;

    /**
     * An year
     */
    private String year;

    public FlowPerYear() {

    }

    public FlowPerYear(String flow, String year) {
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
    public int compareTo(FlowPerYear o) {
        int compare = o.getFlow().compareTo(getFlow());

        if (compare == 0) {
            return o.getYear().compareTo(getYear());
        } else {
            return compare;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(this.flow));
        dataOutput.writeUTF(String.valueOf(this.year));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.flow = dataInput.readUTF();
        this.year = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return getFlow() + "\t" + getYear();
    }

} // end FlowPerYear
