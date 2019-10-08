package br.com.pucpr.posgraduacao.ia.bigdata.spark.keys;

import java.io.Serializable;
import java.util.Objects;

/**
 * Key to grupo transactions by commodity and year
 */
public class CommodityYearKey implements Serializable, Comparable {

    /**
     * A commodity
     */
    private String commodity;

    /**
     * An year
     */
    private String year;

    public CommodityYearKey() {
    }

    public CommodityYearKey(String commodity, String year) {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommodityYearKey that = (CommodityYearKey) o;
        return Objects.equals(commodity, that.commodity) &&
                Objects.equals(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, year);
    }

    @Override
    public String toString() {
        return getCommodity().concat("/").concat(getYear());
    }

    @Override
    public int compareTo(Object o) {
        return this.toString().compareTo(o.toString());
    }

} // end CommodityYearKey