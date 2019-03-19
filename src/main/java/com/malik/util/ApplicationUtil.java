package com.malik.util;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class ApplicationUtil implements Serializable {

    /**
     * Convert String JSON to Map
     * @param json -> String JSON
     * @return Map
     */
    public Map<String, Object> convertJSONToMap(String json) throws IOException {
        return new ObjectMapper().readValue(json, Map.class);
    }
}
