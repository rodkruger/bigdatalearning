package sql;

import java.io.Serializable;

public class Response implements Serializable {

    private String country;
    private Double ageMidPoint;
    private Double salaryMidPoint;

    public Response() {
    }

    public Response(String country, Double ageMidPoint, Double salaryMidPoint) {
        this.country = country;
        this.ageMidPoint = ageMidPoint;
        this.salaryMidPoint = salaryMidPoint;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Double getAgeMidPoint() {
        return ageMidPoint;
    }

    public void setAgeMidPoint(Double ageMidPoint) {
        this.ageMidPoint = ageMidPoint;
    }

    public Double getSalaryMidPoint() {
        return salaryMidPoint;
    }

    public void setSalaryMidPoint(Double salaryMidPoint) {
        this.salaryMidPoint = salaryMidPoint;
    }

    @Override
    public String toString() {
        return "Response{" +
                "country='" + country + '\'' +
                ", ageMidPoint=" + ageMidPoint +
                ", salaryMidPoint=" + salaryMidPoint +
                '}';
    }
}