package com.kristi.sink.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSinkConnector extends SinkConnector {
    private static Logger LOGGER = LoggerFactory.getLogger(FileSinkConnector.class);

    private Map<String, String> properties;

    public void start(Map<String, String> properties) {
        this.properties = properties;

        LOGGER.info("Starting connector...");
        try {
            new FileSinkConfig(properties);
        } catch (ConfigException ce) {
            LOGGER.error("Couldn't start connector due to configuration error: {} ", ce.getMessage());
        }
    }

    public Class<? extends Task> taskClass() {
        return FileSinkTask.class;
    }

    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>(properties);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    public void stop() {

    }

    public ConfigDef config() {
        return FileSinkConfig.config();
    }

    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }
}
