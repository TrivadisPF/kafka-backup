package ch.tbd.kafka.backuprestore.springshell;

import ch.tbd.kafka.backuprestore.restore.Restore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@ShellComponent
public class RestoreCommand {

    @Value("${s3.region}")
    private String region;

    @Value("${s3.bucket}")
    private String bucket;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @ShellMethod("Restore a topic")
    public void restore(@ShellOption(help = "Topic to restore") String topic) {
        Restore restore = new Restore(region, bucket, bootstrapServers);
        restore.restore(topic);
    }

    @ShellMethod("Restore a topic to a different topic")
    public void restoreTo(@ShellOption(help = "Topic to restore") String topic, @ShellOption(help = "Target topic ") String topicTo) {
        Restore restore = new Restore(region, bucket, bootstrapServers);
        restore.restore(topic, topicTo);
    }
}
