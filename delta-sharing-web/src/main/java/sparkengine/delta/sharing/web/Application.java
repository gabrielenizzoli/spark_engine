package sparkengine.delta.sharing.web;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import sparkengine.delta.sharing.utils.TableLayoutStore;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }


    @Bean
    public SparkSession sparkSession(SparkConfig sparkConfig) {
        return SparkSession.builder()
                .appName(sparkConfig.getName())
                .master(sparkConfig.getMaster())
                .config("spark.driver.memory", sparkConfig.getMemory())
                .getOrCreate();
    }

    @Bean
    TableLayoutStore tableLayoutStore(SparkSession sparkSession) {
        return TableLayoutStore.builder().sparkSession(sparkSession).build();
    }

}

