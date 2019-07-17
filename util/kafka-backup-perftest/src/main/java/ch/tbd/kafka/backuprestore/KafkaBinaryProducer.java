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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.kafka.clients.admin.NewTopic;
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
    Map<Integer, Long> nofMessagesProduced = new HashMap<>();

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
        
        if (myConf.isCreateTopic()) {
        	NewTopic t = new NewTopic(myConf.getTopic(), myConf.getPartitions(), myConf.getReplicationFactor());
        	KafkaAdminClient.create(props).createTopics(Collections.singleton(t));
        	partitions = myConf.getPartitions();	
        } else { 
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
        }
        
        
        watchDir = myConf.getWatchDir();
        path = FileSystems.getDefault().getPath(watchDir);
        
        for (int i = 0; i < partitions; i++) {
        	nofMessagesProduced.put(i, 0L);
        }
    }

    // Takes a whole binary file content and splits it into message size chunks
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
                    int totalMessageCount = 0;
                    boolean repeat = true;
                    do {
                        for (int i = 0; i < allChunks.size()-1; i++) {
                        	if (allChunks.get(i).length == myConf.getMessageSize().intValue() ) {
                        		publishMessage(fileName, (allChunks.get(i)), partitionSelector.nextPartition());
                        		totalMessageCount++;
                        		if (totalMessageCount >= myConf.getNumberOfMessages() && myConf.getNumberOfMessages() != -1) {
                        			repeat = false;
                        			break;
                        		}
                        		if (totalMessageCount % 10000 == 0) {
                        			System.out.println("Published 10000 messages to topic " + topic + " ....");
                        		}
                        			
                        	}
                        }
                        if (myConf.getNumberOfMessages() == -1) {
                        	repeat = false;
                        }
                    } while (repeat);
                    
                    System.out.println("Finished publishing!");
                    System.out.println("A total of " + totalMessageCount + " messages have been sent to topic " + topic);
                    
                }
            }
            key.reset();
        }
    }

    private void publishMessage(String key, byte[] bytes, int partition) {
        ProducerRecord<String, byte[]> data = new ProducerRecord<>(topic, partition, key, bytes);
        Headers headers = data.headers();
        nofMessagesProduced.put(partition, nofMessagesProduced.get(partition)+1);
        headers.add("message.number", nofMessagesProduced.get(partition).toString().getBytes());
        producer.send(data);
        //System.out.println("publishing message of size " + bytes.length + " to partition " + partition);
    }

    public static void main(String args[]) {
        KafkaBinaryProducer abp;
        
        final Options options = new Options();
        
        options.addOption(Option.builder("b")
        								.longOpt("bootstrapServers")
        								.hasArg()
        								.desc("Bootstrap broker(s) (host[:port])")
//        								.required()
        								.build());
        options.addOption(Option.builder("t")
        								.longOpt("topic")
										.hasArg()
										.desc("Topic to write messages to")
//										.required()
										.build());
        
        options.addOption(Option.builder("c")
				.longOpt("create")
				.hasArg()
				.desc("Create Kafka topic before producing data")
//				.required()
				.build());

        options.addOption(Option.builder("c")
				.longOpt("create")
				.hasArg(false)
				.desc("Create Kafka topic before producing data")
				.build());
        
        options.addOption(Option.builder("r")
				.longOpt("replicationFactor")
				.hasArg(true)
				.desc("Replication Factor, only needed when topic should be created before running (-c). Defaults to 1.")
				.build());

        options.addOption(Option.builder("p")
				.longOpt("partitions")
				.hasArg(true)
				.desc("Number of Partitions, only needed when topic should be created before running (-c). Defaults to 1.")
				.build());    
        
        options.addOption(Option.builder("w")
				.longOpt("watchDir")
				.hasArg()
				.desc("Folder to watch on local filesystem for new files to be used as data. Defaults to /tmp")
				.build());
        		
        options.addOption(Option.builder("f")
				.longOpt("confFile")
				.hasArg()
				.desc("read the config from the file")
				.build());
        
        options.addOption(Option.builder("h")
				.longOpt("help")
				.hasArg(false)
				.desc("Print Usage help")
				.build());

        options.addOption(Option.builder("s")
				.longOpt("messageSize")
				.hasArg()
				.desc("The size of the message to produce, use KB or MB to specify the unit")
				.build());

        options.addOption(Option.builder("d")
				.longOpt("partitionDistribution")
				.hasArg()
				.desc("Distribution over partitions, if not set then it is equally distributed. Otherwise pass the number of messages per partition as a list of integers")
				.build());
        
        options.addOption(Option.builder("n")
				.longOpt("numberOfMessages")
				.hasArg()
				.desc("Number of messages to produce in total over all partitions")
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
    private Long numberOfMessages;
    private int partitions = 1;
    private short replicationFactor = 1;
    private boolean createTopic = true;
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
                } else if (vals[0].equals("watch.dir")) {
                	this.watchDir = vals[1];
                } else if (vals[0].equals("message.size")) {
                	this.messageSize = new Long(vals[1]);
                } else if (vals[0].equals("message.count")) {
                	this.numberOfMessages = new Long(vals[1]);
                } else if (vals[0].equals("create.topic")) {
                	this.createTopic = new Boolean(vals[1]);
                } else if (vals[0].equals("replication.factor")) {
                	this.replicationFactor = new Short(vals[1]);
                } else if (vals[0].equals("partitions")) {
                	this.partitions = new Integer(vals[1]);
                } else if (vals[0].equals("message.count")) {
                	this.numberOfMessages = new Long(vals[1]);
                } else if (vals[0].equals("partition.distribution")) {
            	    String[] partitonDistributionArr = vals[1].split(",");
            		partitionDistribution = Arrays.asList(partitonDistributionArr);
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
    	
    	// should a topic be created
    	createTopic = (cmd.hasOption("c"));
    	if (cmd.hasOption("r")) {
    		replicationFactor = Short.valueOf(cmd.getOptionValue("r"));
    	}    	
    	if (cmd.hasOption("p")) {
    		partitions = Integer.valueOf(cmd.getOptionValue("p"));
    	}    	
    	
    	if (cmd.hasOption("n")) {
    		numberOfMessages = Long.valueOf(cmd.getOptionValue("n"));
    	} else {
    		numberOfMessages = -1L;
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

	public Long getNumberOfMessages() {
		return numberOfMessages;
	}

	public List<String> getPartitionDistribution() {
		return partitionDistribution;
	}

	public int getPartitions() {
		return partitions;
	}

	public short getReplicationFactor() {
		return replicationFactor;
	}

	public boolean isCreateTopic() {
		return createTopic;
	}

}
