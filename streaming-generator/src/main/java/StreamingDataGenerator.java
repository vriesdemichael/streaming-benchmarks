package main.java;

import main.avro.Ping;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;

/**
 * Created by Bernardez on 5/2/2017.
 *
 * Generate a data stream from avro processed files.
 * Store into Redis with timestamp of insertion.
 * Output to Kafka.
 */
public class StreamingDataGenerator {

    public static void main(String[] args) throws Exception {
        System.out.println("It works");

        // Open Avro file
        File file = new File("../../data/mini-2017-02-15-00-00-XCAH-m-00019.avro");

        // Deserialize Ping from disk
        DatumReader<Ping> pingDatumReader = new SpecificDatumReader<Ping>(Ping.class);
        DataFileReader<Ping> dataFileReader = new DataFileReader<Ping>(file, pingDatumReader);
        Ping ping = null;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            ping = dataFileReader.next(ping);
            System.out.println(ping);
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
//                DatumReader<Ping> reader = new SpecificDatumReader<>(Ping.class);
//                org.apache.avro.file.FileReader<Ping> fileReader = DataFileReader.openReader(binFile, reader);
//
//
//                Ping p = fileReader.next();
//                System.out.println(p.toString());
//                Injection<Ping, byte[]> pingInjection = GenericAvroCodecs.toBinary(Ping.getClassSchema());
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
//                Ping datum = null;
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
