package storm.benchmark;

import avro.Ping;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.windowing.TupleWindow;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;

public class KafkaToRedis {

    private static final String CFG_DEBUG = "debug";
    private static final String CFG_SHUTDOWN_TIMER = "shutdown.timer";
    private static final String CFG_WORKERS = "workers";

    // Constants for configuring the kafka connector
    private static final String ZOOKEEPER_HOST = "zookeeper.host";
    private static final String KAFKA_TOPIC = "kafka.topic";

    private static final String REDIS_HOST = "redis.host";
    private static final String REDIS_PORT = "redis.port";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        String confPath = "config.properties";

        int noWorkers = 1;

        String zookeeperHost = null;
        String topic = null;

        String redisHost = null;
        int redisPort = -1;
        long shutdownTimer = -1;
        boolean debug = false;

        try {
            Properties props = new Properties();
            InputStream input;

//            File cfgFile = new File(confPath);
//            input = new FileInputStream(cfgFile);
            input = KafkaToRedis.class.getResourceAsStream(confPath);
            props.load(input);

            zookeeperHost = props.getProperty(ZOOKEEPER_HOST, "localhost:2181");
            topic = props.getProperty(KAFKA_TOPIC, "processed_pings");

            redisHost = props.getProperty(REDIS_HOST, "localhost");
            redisPort = Integer.parseInt(props.getProperty(REDIS_PORT, "6379"));

            shutdownTimer = Long.parseLong(props.getProperty(CFG_SHUTDOWN_TIMER, "-1"));
            debug = Boolean.parseBoolean(props.getProperty(CFG_DEBUG, "false"));

            noWorkers = Integer.parseInt(props.getProperty(CFG_WORKERS, "1"));

        } catch (IOException e) {
            System.err.println("Cannot load configuration from \"config.properties\", using default settings.");
        }

        System.out.println("Config: \n" +
                "\tZookeeper: " + zookeeperHost + "\n" +
                "\tTopics: " + topic  + "\n" +
                "\tRedis: " + redisHost + ":" + redisPort + "\n\t" +
                "NoWorkers: " + noWorkers);

        Config config = new Config();
        config.setDebug(debug);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        // Kafka Config
        BrokerHosts broker = new ZkHosts(zookeeperHost);
        SpoutConfig kafkaSpoutConfig = new SpoutConfig(broker, topic, "/" + topic, UUID.randomUUID().toString());
//        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
//        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new AvroPingScheme());

        // Build topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka_spout", new KafkaSpout(kafkaSpoutConfig));
        builder.setBolt("ping_mapper", new MapPingBolt()).shuffleGrouping("kafka_spout");
        builder.setBolt("windowed_count", new WindowedVideoCounter().withWindow(BaseWindowedBolt.Duration.seconds(10))).shuffleGrouping("ping_mapper");
        builder.setBolt("redis_store_bolt", new RedisStorePingCountBolt(redisHost, redisPort)).shuffleGrouping("windowed_count");


        if (args != null && args.length > 0) {
            config.setNumWorkers(noWorkers);
            StormSubmitter.submitTopologyWithProgressBar("kafka-storm-redis", config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka-storm-redis", config, builder.createTopology());

            // Shut down after x milliseconds
            if (shutdownTimer > 0){
                Utils.sleep(shutdownTimer);
                cluster.killTopology("kafka-storm-redis");
                cluster.shutdown();
            }
        }
    }

    static class AvroPingScheme implements Scheme {

        @Override
        public List<Object> deserialize(ByteBuffer byteBuffer) {
            return new Values(deserializePing(byteBuffer));
        }

        private Ping deserializePing(ByteBuffer message) {
            byte[] b = new byte[message.remaining()];
            message.get(b);

            DatumReader<Ping> reader = new SpecificDatumReader<>(Ping.class);
            try {
                Ping p = reader.read(null, DecoderFactory.get().binaryDecoder(b, null));
                return p;
            } catch (IndexOutOfBoundsException | AvroRuntimeException | IOException e ) {
                return new Ping();
            }
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("pings");
        }
    }

    static class MapPingBolt extends BaseBasicBolt {

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            Ping p = (Ping) input.getValue(0);
            if (p == null || p.getUrlParameters() == null) {
                return;
            }
            String aid = null;
            for (Map.Entry<CharSequence, CharSequence> entry: p.getUrlParameters().entrySet()){
                if (entry.getKey().toString().equals("aid")){
                    aid = entry.getValue().toString();
                    break;
                }
            }
            String media_id = p.getMediaID().toString();
            if (aid != null && !aid.equals("") && !aid.equals("_") &&  media_id != null && !media_id.equals("")) {
                System.err.println("Mapping: " + aid + ":" + media_id);
                collector.emit(new Values(aid + ":" + media_id, 1));
            }else {
                System.err.println("Could not get aid or mediaid");
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("aid:media_id", "count"));
        }

    }

    static class WindowedVideoCounter extends BaseWindowedBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(TupleWindow tupleWindow) {
            List<Tuple> tuples = tupleWindow.get();
            tuples.sort(Comparator.comparing(o -> o.getStringByField("aid:media_id")));

            HashMap<String, Integer> counts = new HashMap<>();

            for (Tuple tuple : tuples) {
                String key = tuple.getStringByField("aid:media_id");
                int existingCount = counts.getOrDefault(key, 0) ;
                int count = tuple.getIntegerByField("count");
                counts.put(key, existingCount + count);
                collector.ack(tuple);
            }

            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                System.err.println("Emitting: " + entry.getKey() + ":" +  entry.getValue());
                collector.emit(new Values(entry.getKey(), entry.getValue()));
            }


        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("aid:media_id", "count"));
        }
    }

    static class RedisStorePingCountBolt extends BaseBasicBolt {


        private final int port;
        private final String host;
        private JedisPool jedisPool;

        RedisStorePingCountBolt(String host, int port) {
            super();
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
        public void execute(Tuple input, BasicOutputCollector collector) {

            Jedis jedis = null;
            try {
                jedis = getInstance().getResource();
                jedis.incrBy("storm:" + input.getStringByField("aid:media_id"), input.getIntegerByField("count"));

            } finally {
                if (jedis != null) {
                    jedis.close();
                }

            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("aid:media_id", "count"));
        }

    }

}
