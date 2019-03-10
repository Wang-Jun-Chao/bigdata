package wjc.bigdata.flink.complexeventprocessing;

import java.util.Objects;

public abstract class MonitoringEvent {

    private String machineName;

    public MonitoringEvent(String machineName) {
        this.machineName = machineName;
    }

    public String getMachineName() {
        return machineName;
    }

    public void setMachineName(String machineName) {
        this.machineName = machineName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MonitoringEvent that = (MonitoringEvent) o;
        return Objects.equals(machineName, that.machineName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(machineName);
    }
}
