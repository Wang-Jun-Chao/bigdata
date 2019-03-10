package wjc.bigdata.flink.complexeventprocessing;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class EventDeserializationSchema implements DeserializationSchema<TemperatureEvent> {

    @Override
    public TypeInformation<TemperatureEvent> getProducedType() {
        return TypeExtractor.getForClass(TemperatureEvent.class);
    }

    @Override
    public TemperatureEvent deserialize(byte[] message) throws IOException {
        String str = new String(message, StandardCharsets.UTF_8);

        String[] parts = str.split("=");
        return new TemperatureEvent(parts[0], Double.parseDouble(parts[1]));
    }

    @Override
    public boolean isEndOfStream(TemperatureEvent nextElement) {
        return false;
    }

}
