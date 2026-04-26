package dev.miron.connect.temp;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

public class TempSourceConnector extends SourceConnector {
    static final String TOPIC = "topic";
    static final String SENSORS = "sensors";
    static final String RATE = "messages.per.sensor.per.second";

    private Map<String, String> config;

    private static final ConfigDef DEF = new ConfigDef()
            .define(TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Target topic")
            .define(SENSORS, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "Number of sensors")
            .define(RATE, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "Messages per sensor per second");

    @Override public void start(Map<String, String> props) { config = props; }
    @Override public Class<? extends Task> taskClass() { return TempSourceTask.class; }
    @Override public List<Map<String, String>> taskConfigs(int maxTasks) { return List.of(config); }
    @Override public void stop() {}
    @Override public ConfigDef config() { return DEF; }
    @Override public String version() { return "1.0.0"; }
}
