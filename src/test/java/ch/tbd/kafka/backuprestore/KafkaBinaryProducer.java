package ch.tbd.kafka.backuprestore;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class KafkaBinaryProducer {
    Config myConf;
    Producer<String, byte[]> producer;
    String topic, bootstrapServers, watchDir;
    Path path;
    ByteArrayOutputStream out;

    public KafkaBinaryProducer(String configPath) throws IOException {
        // Read initial configuration
        myConf = new Config(configPath);

        // setting the kafka producer stuff
        Properties props = new Properties();
        props.put("bootstrap.servers", myConf.get("bootstrap.servers"));
        props.put("acks", "1");
        props.put("compression.type", "snappy");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(props);

        topic = myConf.get("topic");
        watchDir = myConf.get("watchdir");
        path = FileSystems.getDefault().getPath(watchDir);

    }

    // Takes a whole binary file content and splits it into 10k chunks
    ArrayList splitFile(String name, byte[] datum) {
        int i, l = datum.length;
        int block = 10240;
        int numblocks = l / block;
        int counter = 0, totalSize = 0;
        int marker = 0;
        byte[] chunk;
        ArrayList<byte[]> data = new ArrayList();
        for (i = 0; i < numblocks; i++) {
            counter++;
            chunk = Arrays.copyOfRange(datum, marker, marker + block);
            data.add(chunk);
            totalSize += chunk.length;
            marker += block;
        }
        chunk = Arrays.copyOfRange(datum, marker, l);
        data.add(chunk);
        // the null value is a flag to the consumer, specifiying that it has
        // reached the end of the file
        data.add(null);
        return data;
    }

    void start() throws IOException, InterruptedException {
        System.out.println("Program started...");
        String fileName;
        byte[] fileData;
        ArrayList<byte[]> allChunks;
        // the watcher watches for filesystem changes
        WatchService watcher = FileSystems.getDefault().newWatchService();
        WatchKey key;
        path.register(watcher, ENTRY_CREATE);

        while (true) {
            System.out.println("while happen");
            key = watcher.take();
            // The code gets beyond this point only when a filesystem event
            // occurs
            System.out.println("Any event happen");
            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                // We act only if a new file was added
                if (kind == ENTRY_CREATE) {
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path filename = ev.context();
                    fileName = filename.toString();
                    // We need this little delay to make sure the file is closed
                    // before we read it
                    Thread.sleep(500);

                    fileData = Files
                            .readAllBytes(FileSystems.getDefault().getPath(watchDir + File.separator + fileName));
                    allChunks = splitFile(fileName, fileData);
                    for (int i = 0; i < allChunks.size(); i++) {
                        publishMessage(fileName, (allChunks.get(i)));
                    }
                    System.out.println("Published file " + fileName);

                }
            }
            key.reset();
        }
    }

    private void publishMessage(String key, byte[] bytes) {
        ProducerRecord<String, byte[]> data = new ProducerRecord<>(topic, key, bytes);
        producer.send(data);
        System.out.println("data transferred...");
    }

    public static void main(String args[]) {
        KafkaBinaryProducer abp;
        try {
            abp = new KafkaBinaryProducer(args[0]);
            try {
                abp.start();
            } catch (InterruptedException ex) {
                Logger.getLogger(KafkaBinaryProducer.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (IOException ex) {
            Logger.getLogger(KafkaBinaryProducer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}

class Config {

    // this class reads the configuration parameters from the config file and
    // serves them to the main program
    HashMap<String, String> conf;

    public Config(String filePath) throws FileNotFoundException, IOException {
        conf = new HashMap();
        File file = new File(filePath);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String[] vals;
        String line = br.readLine();
        while (line != null) {
            if (!line.startsWith("#")) {
                vals = line.toLowerCase().split("=");
                conf.put(vals[0], vals[1]);
            }
            line = br.readLine();
        }

    }

    public String get(String key) {
        return conf.get(key.toLowerCase());
    }

}
