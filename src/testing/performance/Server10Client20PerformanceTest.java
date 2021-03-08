package testing.performance;

import app_kvECS.ECSClient;
import client.KVStore;
import ecs.zk.ZooKeeperService;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Server10Client20PerformanceTest extends BasePerformanceTest {
    @Override
    protected List<KVStore> generateNewClients() {
        return IntStream.range(0, getNumClients())
                .mapToObj(i -> new KVStore("ug132", 50000))
                .collect(Collectors.toList());
    }

    @Override
    protected ECSClient generateNewServers() {
        ECSClient ecsClient;
        try {
            final String TEMP_FILE_NAME = "ecs.tmp.config";
            try (PrintWriter writer = new PrintWriter(new FileWriter(TEMP_FILE_NAME))) {
                Stream.of(
                        "server1 ug132 50000",
                        "server2 ug133 50000",
                        "server3 ug134 50000",
                        "server4 ug135 50000",
                        "server5 ug136 50000",
                        "server6 ug137 50000",
                        "server7 ug138 50000",
                        "server8 ug139 50000",
                        "server9 ug140 50000",
                        "server10 ug141 50000"
                ).forEach(writer::println);
            }
            ecsClient = new ECSClient(TEMP_FILE_NAME, ZooKeeperService.LOCALHOST_CONNSTR);
        } catch (Exception e) {
            throw new RuntimeException("Unable to create ECS", e);
        }

        return ecsClient;
    }

    @Override
    protected int getNumClients() {
        return 20;
    }

    @Override
    protected int getNumServers() {
        return 10;
    }
}
