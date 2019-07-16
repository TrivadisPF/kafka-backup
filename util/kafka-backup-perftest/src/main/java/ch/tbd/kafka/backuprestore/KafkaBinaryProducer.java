package ch.tbd.kafka.backuprestore;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.util.unit.DataSize;

import com.google.common.base.Strings;

public class KafkaBinaryProducer {
    Config myConf;
    Producer<String, byte[]> producer;
    String topic, bootstrapServers, watchDir;
    Integer partitions;
    Path path;
    ByteArrayOutputStream out;

    public KafkaBinaryProducer(CommandLine cmd)  {
        // Read initial configuration
    	String configPath = cmd.getOptionValue("conf");
    	if (configPath == null) {
            myConf = new Config(cmd);
    	} else {
            try {
				myConf = new Config(configPath);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}

        // setting the kafka producer stuff
        Properties props = new Properties();
        props.put("bootstrap.servers", myConf.getBootstrapServers());
        props.put("acks", "1");
        props.put("compression.type", "snappy");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(props);
        
        topic = myConf.getTopic();

        DescribeTopicsResult tr = KafkaAdminClient.create(props).describeTopics(Collections.singletonList(topic));
        
        TopicDescription td;
		try {
			td = tr.values().get(topic).get();
	        partitions = td.partitions().size();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
        
        watchDir = myConf.getWatchDir();
        path = FileSystems.getDefault().getPath(watchDir);
        

    }

    // Takes a whole binary file content and splits it into 10k chunks
    private ArrayList splitFile(String name, byte[] datum) {
        int i, l = datum.length;
        int block = myConf.getMessageSize().intValue();
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
        // the null value is a flag to the consumer, specifying that it has
        // reached the end of the file
        data.add(null);
        return data;
    }

    private void start() throws IOException, InterruptedException {
        System.out.println("Program started...");
        String fileName;
        byte[] fileData;
        ArrayList<byte[]> allChunks;
        // the watcher watches for filesystem changes
        WatchService watcher = FileSystems.getDefault().newWatchService();
        WatchKey key;
        path.register(watcher, ENTRY_CREATE);

        PartitionSelector partitionSelector = new PartitionSelector(partitions, myConf.getPartitionDistribution().stream().map(Integer::parseInt).collect(Collectors.toList()));
        
        
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
                        publishMessage(fileName, (allChunks.get(i)), partitionSelector.nextPartition());
                    }
                    System.out.println("Published file " + fileName);

                }
            }
            key.reset();
        }
    }

    private void publishMessage(String key, byte[] bytes, int partition) {
        ProducerRecord<String, byte[]> data = new ProducerRecord<>(topic, partition, key, bytes);
        Headers headers = data.headers();
        headers.add("message.number", "1".getBytes());
        producer.send(data);
        System.out.println("publishing message of size " + bytes.length + " to partition " + partition);
    }

    public static void main(String args[]) {
        KafkaBinaryProducer abp;
        
        final Options options = new Options();
        
        options.addOption(Option.builder("b")
        								.longOpt("bootstrapServers")
        								.hasArg()
        								.desc("boostrap Server")
//        								.required()
        								.build());
        options.addOption(Option.builder("t")
        								.longOpt("topic")
										.hasArg()
										.desc("the topic to write to")
//										.required()
										.build());
        options.addOption(Option.builder("w")
				.longOpt("watchDir")
				.hasArg()
				.desc("read the config from the file")
				.build());
        		
        options.addOption(Option.builder("f")
				.longOpt("confFile")
				.hasArg()
				.desc("read the config from the file")
				.build());
        
        options.addOption(Option.builder("h")
				.longOpt("help")
				.hasArg(false)
				.desc("show help")
				.build());

        options.addOption(Option.builder("s")
				.longOpt("messageSize")
				.hasArg()
				.desc("the message size to produce")
				.build());

        options.addOption(Option.builder("d")
				.longOpt("partitionDistribution")
				.hasArg()
				.desc("the distribution over partitions, if not set then it is equally distributed. Otherwise pass the number of messages per partition as a list of integers")
				.build());

        
        
        HelpFormatter formatter = new HelpFormatter();
        
        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine cmd = parser.parse(options, args);

            if (cmd.getOptions().length == 0 || cmd.hasOption("h")) {
            	formatter.printHelp( "gen", options );
            } else {
            	System.out.println(cmd.getArgList());
	            abp = new KafkaBinaryProducer(cmd);
	            try {
	                abp.start();
	            } catch (InterruptedException ex) {
	                Logger.getLogger(KafkaBinaryProducer.class.getName()).log(Level.SEVERE, null, ex);
				} catch (IOException ex) {
	                Logger.getLogger(KafkaBinaryProducer.class.getName()).log(Level.SEVERE, null, ex);
				}

            }
        }
        catch( ParseException exp ) {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }
        
    }
}

class Config {

    private String topic;
    private String bootstrapServers;
    private String watchDir;
    private Long messageSize;
    private List<String> partitionDistribution = new ArrayList<>();
    
    public Config(String filePath) throws FileNotFoundException, IOException {
        File file = new File(filePath);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String[] vals;
        String line = br.readLine();
        while (line != null) {
            if (!line.startsWith("#")) {
                vals = line.toLowerCase().split("=");
                if (vals[0].equals("bootstrap.servers")) {
                	this.bootstrapServers = vals[1];
                } else if (vals[0].equals("topic")) {
                	this.topic = vals[1];
                } else if (vals[0].equals("watchdir")) {
                	this.watchDir = vals[1];
                } else if (vals[0].equals("messageSize")) {
                	this.messageSize = new Long(vals[1]);
                }
            }
            line = br.readLine();
        }

    }
    
    public Config(CommandLine cmd) {
    	topic = cmd.getOptionValue("t");
    	System.out.println("topic" + topic);
    	bootstrapServers = cmd.getOptionValue("b");
    	if (cmd.hasOption("s")) {
    		String messageSizeStr = cmd.getOptionValue("s");
    		DataSize ds = DataSize.parse(messageSizeStr);
    		messageSize = ds.toBytes();
    		System.out.println(messageSize);
    	}
    	if (cmd.hasOption("w")) {
    		watchDir = cmd.getOptionValue("w");
    	} else {
    		watchDir = "/tmp";
    	}
    	if (cmd.hasOption("d")) {
    		String partitionDistributionStr = cmd.getOptionValue("d");
    		String[] partitonDistributionArr = partitionDistributionStr.split(",");
    		partitionDistribution = Arrays.asList(partitonDistributionArr);
    	}
    }

	public String getTopic() {
		return topic;
	}

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public String getWatchDir() {
		return watchDir;
	}

	public Long getMessageSize() {
		return messageSize;
	}

	public List<String> getPartitionDistribution() {
		return partitionDistribution;
	}

}
