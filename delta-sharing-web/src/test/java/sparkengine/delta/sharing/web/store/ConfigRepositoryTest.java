package sparkengine.delta.sharing.web.store;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;


@SpringBootTest
@ActiveProfiles("test")
public class ConfigRepositoryTest {

    @Autowired
    private ConfigRepository configRepository;

    @Test
    public void testBasicOperations() {

        var name = UUID.randomUUID().toString();

        Assertions.assertFalse(configRepository.isShare(name));
        Assertions.assertTrue(configRepository.addShare(name));
        Assertions.assertThrows(DuplicateKeyException.class, () -> configRepository.addShare(name));
        Assertions.assertTrue(configRepository.isShare(name));

    }

}
