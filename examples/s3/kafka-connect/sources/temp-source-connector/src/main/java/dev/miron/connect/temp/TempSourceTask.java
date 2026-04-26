package dev.miron.connect.temp;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.time.Instant;
import java.util.*;

public class TempSourceTask extends SourceTask {
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name("dev.miron.temp.Reading")
        .field("sensorId", Schema.OPTIONAL_STRING_SCHEMA)
        .field("latitude", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("longitude", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("timestamp", Timestamp.builder().optional().build())
        .field("temperatureF", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    private record Sensor(String id, double lat, double lon) {}

    private String topic;
    private int perSensorPerSec;
    private List<Sensor> sensors;
    private long[] seq;
    private long nextMs;
    private volatile boolean running;
    private final Random noise = new Random();

    @Override public String version() { return "1.0.0"; }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(TempSourceConnector.TOPIC);
        int n = Integer.parseInt(props.get(TempSourceConnector.SENSORS));
        perSensorPerSec = Integer.parseInt(props.get(TempSourceConnector.RATE));

        if (topic == null || topic.isBlank() || n < 1 || perSensorPerSec < 1) {
            throw new ConnectException("topic/sensors/messages.per.sensor.per.second must be set and > 0");
        }

        Random seeded = new Random((topic + "|" + n + "|" + perSensorPerSec).hashCode());
        sensors = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            sensors.add(new Sensor(
                    id(seeded),
                    Math.toDegrees(Math.asin(seeded.nextDouble() * 2 - 1)), // roughly uniform over sphere
                    seeded.nextDouble() * 360 - 180
            ));
        }

        seq = new long[n];
        for (int i = 0; i < n; i++) {
            Map<String, Object> off = context.offsetStorageReader().offset(Map.of("sensorId", sensors.get(i).id()));
            seq[i] = off == null ? 0 : ((Number) off.getOrDefault("seq", -1)).longValue() + 1;
        }

        running = true;
        nextMs = System.currentTimeMillis();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        while (running) {
            long sleep = nextMs - System.currentTimeMillis();
            if (sleep <= 0) break;
            Thread.sleep(Math.min(sleep, 200));
        }
        if (!running) return null;
        nextMs += 1000;

        Instant now = Instant.now();
        java.util.Date ts = java.util.Date.from(now);
        ArrayList<SourceRecord> out = new ArrayList<>(sensors.size() * perSensorPerSec);

        for (int i = 0; i < sensors.size(); i++) {
            Sensor s = sensors.get(i);
            double tempF = tempF(s.lat(), s.lon(), now);

            for (int j = 0; j < perSensorPerSec; j++) {
                long currentSeq = seq[i]++;

                Struct v = new Struct(VALUE_SCHEMA)
                        .put("sensorId", s.id())
                        .put("latitude", s.lat())
                        .put("longitude", s.lon())
                        .put("timestamp", ts)
                        .put("temperatureF", tempF);

                out.add(new SourceRecord(
                        Map.of("sensorId", s.id()),
                        Map.of("seq", currentSeq),
                        topic,
                        Schema.STRING_SCHEMA, s.id(),
                        VALUE_SCHEMA, v
                ));
            }
        }
        return out;
    }

    @Override
    public void stop() { running = false; }

    private static String id(Random r) {
        return Long.toUnsignedString(r.nextLong(), 36) + Long.toUnsignedString(r.nextLong(), 36);
    }

    private double tempF(double lat, double lon, Instant t) {
        double utcHour = (t.getEpochSecond() % 86_400) / 3600d;
        double localHour = (utcHour + lon / 15d + 24) % 24;          // local solar time by longitude
        double baseC = 30 - Math.abs(lat) * 0.5;                     // colder away from equator
        double dayC = 6 * Math.sin(2 * Math.PI * (localHour - 15) / 24); // max around 15:00
        double noiseC = noise.nextGaussian() * 1.5;                  // random noise
        return Math.round(((baseC + dayC + noiseC) * 9 / 5 + 32) * 10d) / 10d;
    }
}
