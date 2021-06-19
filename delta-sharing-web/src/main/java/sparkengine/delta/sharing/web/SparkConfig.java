package sparkengine.delta.sharing.web;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties("spark")
public class SparkConfig {

    String name;
    String master;
    String memory;

}
