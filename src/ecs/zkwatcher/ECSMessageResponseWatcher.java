package ecs.zkwatcher;

import com.google.protobuf.InvalidProtocolBufferException;
import ecs.IECSNode;
import ecs.zk.ZooKeeperService;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import shared.messages.KVAdminMessageProto;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ECSMessageResponseWatcher {
    private static final Logger logger = Logger.getRootLogger();
    private final String zNode;
    private final ZooKeeperService zk;

    /**
     * Convenience method to generate a watcher
     *
     * @param zk         a {@link ZooKeeperService} instance to use for the getData call
     * @param zNode      the path to the zNode whose data we want to watch
     * @param onComplete a {@link Runnable} to run *synchronously* once we recieve the desired event
     * @return a {@link Watcher} for a {@link ZooKeeperService#getData(String, Watcher)}-like call
     */
    private static Watcher generateDataWatcher(ZooKeeperService zk, String zNode, Runnable onComplete) {
        return watchedEvent -> {
            if (watchedEvent.getType() == Watcher.Event.EventType.NodeDataChanged) {
                onComplete.run();
            } else try {
                // Start watching again; we don't care about this event at all, no callback required
                zk.getData(zNode, generateDataWatcher(zk, zNode, onComplete));
            } catch (Exception ignored) {
            }
        };
    }

    public ECSMessageResponseWatcher(ZooKeeperService zk, IECSNode node) {
        this.zk = zk;
        this.zNode = ZooKeeperService.ZK_SERVERS + "/" + node.getNodeName();
    }

    public synchronized KVAdminMessageProto sendMessage(KVAdminMessageProto request, long timeout, TimeUnit timeUnit) throws IOException {
        // 0. Prepare sync/async flow
        final CountDownLatch latch = new CountDownLatch(1);
        final Watcher responseWatcher = generateDataWatcher(zk, zNode, latch::countDown);

        // 1. Send the message
        try {
            zk.setData(zNode, request.getBytes());
            zk.getData(zNode, responseWatcher); // set the watch
        } catch (Exception e) {
            throw new IOException("Could not send message", e);
        }

        // 2. Wait for the response
        boolean resRecv = false;
        try {
            resRecv = !latch.await(timeout, timeUnit);
        } catch (InterruptedException e) {
            logger.warn("Unable to wait for latch to count down");
        }

        // 3. Extract response
        KVAdminMessageProto res = null;
        try {
            res = new KVAdminMessageProto(zk.getData(zNode));
        } catch (KeeperException | InterruptedException | InvalidProtocolBufferException e) {
            logger.warn("Unable to read response");
        }

        // 4. Return response
        if (res == null || !resRecv) throw new IOException("Did not receive a response");
        return res;
    }
}
