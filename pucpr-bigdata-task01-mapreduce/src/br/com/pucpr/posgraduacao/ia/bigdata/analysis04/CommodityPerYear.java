package br.com.pucpr.posgraduacao.ia.bigdata.analysis04;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Group by commodity and year
 */
public class CommodityPerYear implements WritableComparable<CommodityPerYear> {

    /**
     * a commodity
     */
    private String commodity;

    /**
     * an year
     */
    private String year;

    public CommodityPerYear() {

    }

    public CommodityPerYear(String commodity, String year) {
        this.commodity = commodity;
        this.year = year;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public int compareTo(CommodityPerYear o) {
        int compare = o.getCommodity().compareTo(getCommodity());

        if (compare == 0) {
            return o.getYear().compareTo(getYear());
        } else {
            return compare;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(this.commodity));
        dataOutput.writeUTF(String.valueOf(this.year));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.commodity = dataInput.readUTF();
        this.year = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return getCommodity() + "\t" + getYear();
    }

} // end CommodityPerYear
