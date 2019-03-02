package wjc.bigdata.flink.complexeventprocessing;


import java.util.Objects;

public class TemperatureEvent extends MonitoringEvent {

    private double temperature;

    public TemperatureEvent(String machineName) {
        super(machineName);
    }

    public TemperatureEvent(String machineName, double temperature) {
        super(machineName);
        this.temperature = temperature;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TemperatureEvent that = (TemperatureEvent) o;
        return Double.compare(that.temperature, temperature) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), temperature);
    }

    @Override
    public String toString() {

        return "TemperatureEvent [getTemperature()=" + getTemperature()
                + ", getMachineName()=" + getMachineName() + "]";
    }

}
