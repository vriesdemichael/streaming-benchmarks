package streaming.generator;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import redis.clients.jedis.Jedis;
import streaming.generator.avro.Ping;

import java.io.File;
import java.util.List;
import java.util.UUID;

/**
 * Created by Bernardez on 5/2/2017.
 *
 * Generate a data stream from avro processed files.
 * Store into Redis with timestamp of insertion.
 * Output to Kafka.
 */
public class StreamingDataGenerator {

    public static void main(String[] args) throws Exception {
        System.out.println("It works arg[0] = " + args[0]);
        Long delayPerItem;
        try {
            System.out.println("Parselong: "+ Double.parseDouble(args[0]));
            delayPerItem = Math.round((1D / Double.parseDouble(args[0])) * 1000);
            System.out.println("After math round: " + delayPerItem);
        } catch (NumberFormatException e) {
            System.out.println("NumberFormatException: First argument should be throughput per s");
            delayPerItem = 10L;
        }

        // Open Avro file
        File file = new File("../../data/mini-2017-02-15-00-00-XCAH-m-00019.avro");

        // Open Redis connection
        Jedis jedis = new Jedis("localhost");

        // Deserialize main.java.streaming.generator.avro.Ping from disk
        DatumReader<Ping> pingDatumReader = new SpecificDatumReader<Ping>(Ping.class);
        DataFileReader<Ping> dataFileReader = new DataFileReader<Ping>(file, pingDatumReader);
        Ping ping = null;

        Long startTime = System.currentTimeMillis();
        Integer counter = 0;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            ping = dataFileReader.next(ping);
            counter++;
            Long now = System.currentTimeMillis();
            // store in Reddis

            String uuid = UUID.nameUUIDFromBytes(ping.toString().getBytes()).toString();



            jedis.lpush(uuid, now.toString());
            List<String> value = jedis.lrange(uuid, 0, 1);
            System.out.println(value.get(0));

            // Output to kafka

            // Sleep for time remainder
            System.out.println("delayPerItem: " + delayPerItem + "\tExecution time: " + (System.currentTimeMillis() - now)
            + "\tAverage per s" + (counter / (System.currentTimeMillis() - startTime)));
            Long delay = delayPerItem - (System.currentTimeMillis() - now);
            if (delay > 0L) {
                Thread.sleep(delay);
            }

        }

    }

//    private static void processAvroFiles(ArrayList<File> validFiles, Properties props){
//
//
//        Map<String, Object> config = new HashMap<>();
//        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(CFG_BOOTSTRAP_SERVER));
//        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, props.getProperty(CFG_AVRO_KEY_SERIALIZER, DEFAULT_SERIALIZER));
//        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, props.getProperty(CFG_AVRO_VALUE_SERIALIZER, DEFAULT_SERIALIZER));
//
//
//        long sleepMultiplier = 1;
//        try {
//            sleepMultiplier = Long.parseLong(props.getProperty(CFG_SLEEP_MULTIPLIER, Long.toString(DEFAULT_SLEEP_MULTIPLIER)));
//        } catch (NumberFormatException e) {
//            System.out.format("Cannot parse the sleep multiplier specified in the log (%s)," +
//                    " it has to be an integer or long.", props.getProperty(CFG_SLEEP_MULTIPLIER));
//        }
//
//        for (File binFile : validFiles) {
//            try {
//
//
//                DatumReader<main.java.streaming.generator.avro.Ping> reader = new SpecificDatumReader<>(main.java.streaming.generator.avro.Ping.class);
//                org.apache.avro.file.FileReader<main.java.streaming.generator.avro.Ping> fileReader = DataFileReader.openReader(binFile, reader);
//
//
//                main.java.streaming.generator.avro.Ping p = fileReader.next();
//                System.out.println(p.toString());
//                Injection<main.java.streaming.generator.avro.Ping, byte[]> pingInjection = GenericAvroCodecs.toBinary(main.java.streaming.generator.avro.Ping.getClassSchema());
//
//                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(config);
//
//
//                System.out.println("Processing " + binFile.getPath());
//
//
//                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
//
//                long start = System.currentTimeMillis();
//                long totalSleepTime = 0;
//                long previousTimestamp = start;
//
//                main.java.streaming.generator.avro.Ping datum = null;
//                while (fileReader.hasNext()) {
//                    try {
//                        datum = fileReader.next();
//                    } catch (Exception e) {
//                        System.err.println(e.getMessage());
//                        break;
//                    }
//
//                    Date d = formatter.parse(datum.getDateGMTISO().toString());
//                    long timeDiff = d.getTime() - previousTimestamp;
//
//                    if (sleepMultiplier != 0 && timeDiff > 0 && previousTimestamp != 0) {
//                        totalSleepTime += timeDiff;
//                        Thread.sleep(timeDiff * sleepMultiplier);
//                    }
//                    System.out.print(".");
//
//                    previousTimestamp = d.getTime();
//
//
//                    byte[] binPing = pingInjection.apply(datum);
//                    producer.send(new ProducerRecord<>(props.getProperty(CFG_AVRO_TOPIC, DEFAULT_AVRO_TOPIC), Long.toString(d.getTime()), binPing));
//                }
//
//                fileReader.close();
//                System.out.println("\nFinished feeding kafka with " + binFile.getPath() + "." +
//                        "\n\tTime span: " + totalSleepTime +
//                        "\n\tActual time slept:" + totalSleepTime * sleepMultiplier);
//
//
//            } catch (Exception e) {
//                System.err.println(e.getMessage());
//            }
//        }
//    }

}
