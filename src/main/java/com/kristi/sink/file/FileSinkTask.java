package com.kristi.sink.file;

import static com.kristi.sink.file.FileSinkConfig.FILEPATH_CONFIG;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSinkTask extends SinkTask {
    private static Logger LOGGER = LoggerFactory.getLogger(FileSinkTask.class);

    private String filePath;
    private FileWriter csvWriter;

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        filePath = map.get(FILEPATH_CONFIG);
        try {
            csvWriter = new FileWriter(filePath);
            csvWriter.append("Topic,Partition id,Offset,Payload").append("\n").flush();
        } catch (IOException e) {
            LOGGER.warn("Couldn't write to csv file", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        records.forEach(record -> {
            try {
                csvWriter.append(record.topic()).append(",")
                        .append(record.kafkaPartition().toString()).append(",")
                        .append(String.valueOf(record.kafkaOffset())).append(",")
                        .append(record.value().toString()).append("\n").flush();
            } catch (IOException e) {
                LOGGER.warn("Couldn't write to csv file", e);
            }
        });

    }

    @Override
    public void stop() {
        try {
            if (Objects.nonNull(csvWriter)) {
                csvWriter.close();
            }
        } catch (IOException e) {
            LOGGER.warn("Couldn't close csv writer", e);
        }
    }
}