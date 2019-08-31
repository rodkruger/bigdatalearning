package pairrdd.aggregation;

import java.io.Serializable;

public class AvgCount implements Serializable {

    private int contagem;
    private double valor;

    public AvgCount() {

    }

    public AvgCount(int contagem, double valor) {
        this.contagem = contagem;
        this.valor = valor;
    }

    public int getContagem() {
        return contagem;
    }

    public void setContagem(int contagem) {
        this.contagem = contagem;
    }

    public double getValor() {
        return valor;
    }

    public void setValor(double valor) {
        this.valor = valor;
    }

    @Override
    public String toString() {
        return "AvgCount{" +
                "contagem=" + contagem +
                ", valor=" + valor +
                '}';
    }
}
