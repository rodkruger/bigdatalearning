package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ForestFireWritable implements Writable {

    private float vento;
    private float temperatura;
    private String mes;

    public ForestFireWritable() {
    }

    public ForestFireWritable(float vento, float temperatura, String mes) {
        this.vento = vento;
        this.temperatura = temperatura;
        this.mes = mes;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.vento = Float.parseFloat(in.readUTF());
        this.temperatura = Float.parseFloat(in.readUTF());
        this.mes = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(this.vento));
        out.writeUTF(String.valueOf(this.temperatura));
        out.writeUTF(String.valueOf(this.mes));
    }

    public float getVento() {
        return vento;
    }

    public void setVento(float vento) {
        this.vento = vento;
    }

    public float getTemperatura() {
        return temperatura;
    }

    public void setTemperatura(float temperatura) {
        this.temperatura = temperatura;
    }

    public String getMes() {
        return mes;
    }

    public void setMes(String mes) {
        this.mes = mes;
    }

    @Override
    public String toString() {
        return "ForestFireWritable{" +
                "vento=" + vento +
                ", temperatura=" + temperatura +
                ", mes='" + mes + '\'' +
                '}';
    }
}
