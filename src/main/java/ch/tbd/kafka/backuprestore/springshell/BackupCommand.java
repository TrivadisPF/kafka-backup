package ch.tbd.kafka.backuprestore.springshell;

import ch.tbd.kafka.backuprestore.backup.Backup;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@ShellComponent
public class BackupCommand {

    @Value("${s3.region}")
    private String region;

    @Value("${s3.bucket}")
    private String bucket;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @ShellMethod("Backup a topic")
    public void backup(@ShellOption(help = "Topic to restore") String topic) {
        Backup backup = new Backup(region, bucket, bootstrapServers);
        backup.backup(topic);
    }

}
