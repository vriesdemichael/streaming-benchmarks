package flink.benchmark;

import avro.Ping;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PingProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(PingProcessor.class);

    private static final String JOB_NAME = "kafka (kafka -> avro -> kafka)";
    private static final String PROPERTIES_PATH = "flinkJob.properties";

    // Constants for configuring the kafka connector
    private static final String KAFKA_BOOTSTRAP = "bootstrap.servers";
    private static final String KAFKA_GROUP = "group.id";
    private static final String KAFKA_TOPIC = "kafka.topic";

    private static final String REDIS_HOST = "redis.host";
    private static final String REDIS_PORT = "redis.port";

    public static void main(String[] args) throws Exception {

        if (args == null || args.length == 0){
            System.err.println("Specify the config file in the first argument please.");
            System.exit(1);
        }

        String confPath = args[0];

        //Get parameters from properties file or args.
        ParameterTool parameters;
        File f = new File(confPath);
        if(f.exists() && !f.isDirectory()) {
            LOG.debug("Found parameters in file " + PROPERTIES_PATH);
            parameters = ParameterTool.fromPropertiesFile(confPath);
        } else {
            LOG.debug("Using args[] as parameters");
            parameters = ParameterTool.fromArgs(args);
        }

        Properties kakfaProperties = new Properties();
        kakfaProperties.setProperty(KAFKA_BOOTSTRAP, parameters.getRequired(KAFKA_BOOTSTRAP));
        kakfaProperties.setProperty(KAFKA_GROUP, parameters.getRequired(KAFKA_GROUP));

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env
            .addSource(
                new FlinkKafkaConsumer010<>(
                    parameters.getRequired(KAFKA_TOPIC),
                    new AvroPingDeserializationSchema(),
                    kakfaProperties
                )
            ).name("Kafka - Processed pings")
            .map(new MapPingFunction())
            .filter(value ->
                value.f0 != null && !value.f0.equals("") && !value.f0.equals("_") &&
                value.f1 != null && !value.f1.equals("") &&
                value.f2 != 0)
            .keyBy(0, 1)
            .timeWindow(Time.of(5000, TimeUnit.MILLISECONDS))
            .sum(2)
            .addSink(
                    new IncrementVideoCountSink(
                        parameters.getRequired(REDIS_HOST),
                        Integer.parseInt(parameters.getRequired(REDIS_PORT))
                    )
            )
            .name("Redis_" + parameters.getRequired(REDIS_HOST) + ":" + parameters.getRequired(REDIS_PORT));

		// execute program
		env.execute(JOB_NAME);
    }

    static class MapPingFunction implements MapFunction<Ping, Tuple3<String, String, Integer>> {

        @Override
        public Tuple3<String, String, Integer> map(Ping value) throws Exception {
            try {
                String aid = null;
                for (Map.Entry<CharSequence, CharSequence> entry: value.getUrlParameters().entrySet()){
                    if (entry.getKey().toString().equals("aid")){
                        aid = entry.getValue().toString();
                        break;
                    }
                }

                return new Tuple3<>(
                        aid,
                        value.getMediaID().toString(),
                        1);
            } catch (NullPointerException e) {
                return new Tuple3<>("","",0);
            }

        }

    }

    public static class AvroPingDeserializationSchema implements DeserializationSchema<Ping>{

        @Override
        public Ping deserialize(byte[] message) throws IOException {
            DatumReader<Ping> reader = new SpecificDatumReader<>(Ping.class);
            try {
                return reader.read(null, DecoderFactory.get().binaryDecoder(message, null));
            } catch (IndexOutOfBoundsException | AvroRuntimeException | IOException e ) {
                System.err.println("skip");
                return new Ping();
            }
        }

        @Override
        public boolean isEndOfStream(Ping nextElement) {
            return false;
        }

        @Override
        public TypeInformation<Ping> getProducedType() {
            return TypeInformation.of(Ping.class);
        }

    }

    public static class IncrementVideoCountSink extends RichSinkFunction<Tuple3<String, String, Integer>> {

        private transient JedisPool jedisPool;
        private final String host;
        private final int port;

        IncrementVideoCountSink(String host, int port) {
            this.host = host;
            this.port = port;
        }

        private JedisPool getInstance() {
            if (this.jedisPool == null) {
                this.jedisPool = new JedisPool(host, port);
            }
            return this.jedisPool;
        }

        @Override
        public void invoke(Tuple3<String, String, Integer> value) throws Exception {
            Jedis jedis = null;
            try {
                jedis = getInstance().getResource();
                jedis.incrBy("flink:" + value.f0 + ":" + value.f1, value.f2);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
    }
}
