package Bean;

public class SensorReading {
    private String name;
    private Long ts;
    private double temp;

    public SensorReading() {
    }

    public SensorReading(String name, Long ts, double temp) {
        this.name = name;
        this.ts = ts;
        this.temp = temp;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public double getTemp() {
        return temp;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "name='" + name + '\'' +
                ", ts=" + ts +
                ", temp=" + temp +
                '}';
    }
}
