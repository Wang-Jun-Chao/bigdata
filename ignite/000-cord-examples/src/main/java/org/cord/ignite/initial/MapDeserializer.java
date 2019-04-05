package  org.cord.ignite.initial;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;

/**
 * Gson Map解析器
 * @author: cord
 * @date: 2019/1/19 0:01
 */
public class MapDeserializer implements JsonDeserializer<Map<String, String>> {

    @Override
    public Map<String, String> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        final JsonObject jsonObject = json.getAsJsonObject();
        Map<String, String> map = new HashMap<>();
        jsonObject.entrySet().forEach(e -> {
            map.put(LOWER_UNDERSCORE.to(LOWER_CAMEL, e.getKey()), e.getValue().isJsonNull() ? null : e.getValue().toString());
        });
        return map;
    }
}
