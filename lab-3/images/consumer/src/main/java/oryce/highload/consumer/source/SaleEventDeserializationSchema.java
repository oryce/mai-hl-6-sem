package oryce.highload.consumer.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class SaleEventDeserializationSchema implements DeserializationSchema<SaleEvent> {

    @Override
    public SaleEvent deserialize(byte[] bytes) throws IOException {
        return objectMapper().readValue(bytes, SaleEvent.class);
    }

    @Override
    public boolean isEndOfStream(SaleEvent saleEvent) {
        return false;
    }

    @Override
    public TypeInformation<SaleEvent> getProducedType() {
        return TypeInformation.of(SaleEvent.class);
    }

    private transient ObjectMapper objectMapper;

    private ObjectMapper objectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule());
        }
        return objectMapper;
    }
}
