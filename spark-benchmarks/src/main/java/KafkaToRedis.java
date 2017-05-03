import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.*;
import java.util.*;


public class KafkaToRedis {

    private static final String KAFKA_TOPIC = "";

    public static void main(String[] args) {

        if (args == null || args.length == 0) {
            System.err.println("Specify the config file in the first argument please.");
            System.exit(1);
        }
        String confPath = args[0];

        try {
            Properties props = new Properties();
            InputStream input;

            File cfgFile = new File(confPath);
            input = new FileInputStream(cfgFile);
            props.load(input);

            String topic = props.getProperty("kafka.topic");
            int noThreads = Integer.parseInt(props.getProperty("numthreads"));
            Map<String, Integer> topicsMap = new HashMap<>();

            topicsMap.put(topic, noThreads);

            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", "localhost:9092");
//            kafkaParams.put("key.deserializer", StringDeserializer.class);
//            kafkaParams.put("value.deserializer", StringDeserializer.class);
            kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", false);


            SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
            JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
            JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(Collections.singletonList(topic), kafkaParams));

            kafkaStream.print();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}