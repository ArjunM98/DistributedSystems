package ecs.zkwatcher;

import app_kvECS.ECSClient;
import ecs.IECSNode;
import ecs.zk.ZooKeeperService;
import org.apache.log4j.Logger;
import shared.messages.KVAdminMessage;
import shared.messages.KVAdminMessageProto;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ECSTransferWatcher {

    private static final Logger logger = Logger.getRootLogger();
    private final IECSNode transferFrom, transferTo;
    private final ZooKeeperService zk;

    public ECSTransferWatcher(ZooKeeperService zk, IECSNode transferFrom, IECSNode transferTo) {
        this.zk = zk;
        this.transferFrom = transferFrom;
        this.transferTo = transferTo;
    }

    public boolean transferListener(long timeout, TimeUnit timeUnit) throws IOException {

        boolean transferFromComplete = false;
        boolean transferToComplete = false;

        ExecutorService threadPool = Executors.newFixedThreadPool(2);
        List<Callable<KVAdminMessageProto>> threads = Arrays.stream((new IECSNode[]{transferFrom, transferTo}))
                .map(node -> (Callable<KVAdminMessageProto>)
                        () -> new ECSMessageResponseWatcher(zk, node).sendMessage(new KVAdminMessageProto(ECSClient.ECS_NAME,
                                KVAdminMessage.AdminStatusType.TRANSFER_BEGIN), 5000, TimeUnit.MILLISECONDS))
                .collect(Collectors.toList());

        try {
            for (Future<KVAdminMessageProto> future : threadPool.invokeAll(threads)) {
                KVAdminMessageProto result = future.get();
                if (result.getSender().equals(transferFrom.getNodeName()) && result.getStatus() == KVAdminMessage.AdminStatusType.TRANSFER_COMPLETE) {
                    transferFromComplete = true;
                } else if (result.getSender().equals(transferTo.getNodeName()) && result.getStatus() == KVAdminMessage.AdminStatusType.TRANSFER_COMPLETE) {
                    transferToComplete = true;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to complete transfer", e);
        }

        return transferFromComplete && transferToComplete;
    }

}
