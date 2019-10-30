package com.kristi.sink.file;

import static org.apache.kafka.common.config.ConfigDef.*;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class FileSinkConfig extends AbstractConfig {
    public static final String FILEPATH_CONFIG = "file";
    private static final String FILEPATH_DOC = "Path to output file to write data read from the topic";

    public FileSinkConfig(Map<String, String> originals) {
        super(config(), originals);
    }

    public static ConfigDef config() {
        return new ConfigDef().define(FILEPATH_CONFIG, Type.STRING, Importance.HIGH, FILEPATH_DOC);
    }
}
