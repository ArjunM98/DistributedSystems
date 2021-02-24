package ecs.zk;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import shared.messages.KVAdminMessageProto;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class ZooKeeperService {
    public static final String ZK_SERVERS = "/workers", ZK_METADATA = "/data";
    private static final Logger logger = Logger.getRootLogger();
    private final ZooKeeper zooKeeper;

    /**
     * Establishes Connection to the Zookeeper Ensemble
     *
     * @param connStr - Zookeeper Ensemble Location (host:port)
     * @throws IOException if unable to connect
     */
    public ZooKeeperService(final String connStr) throws IOException {
        logger.info("Establishing Zookeeper Connection ...");
        CountDownLatch connectionLatch = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(connStr, 2000 /* sessionTimeout */, watchedEvent -> {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectionLatch.countDown();
            }
        });
        try {
            connectionLatch.await();
        } catch (InterruptedException e) {
            throw new IOException("Could not sync with ZooKeeper Ensemble", e);
        }
        logger.info("Connection Established to Zookeeper Ensemble");
    }

    /**
     * Creates a new znode
     *
     * @param node      - Node name
     * @param ephemeral - Boolean flag for persisting ZNode on connection loss
     * @throws IOException if unable to create node
     */
    public String createNode(final String node, KVAdminMessageProto msg, final boolean ephemeral) throws IOException {
        try {
            return zooKeeper.create(
                    node,
                    msg.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    (ephemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT)
            );
        } catch (Exception e) {
            throw new IOException(String.format("Unable to create node '%s'", node), e);
        }
    }

    /**
     * Checks whether the specified node with the described path exists
     *
     * @param node - Node path
     * @return boolean of node's existence
     * @throws IOException if indeterminate
     */
    public boolean nodeExists(final String node) throws IOException {
        try {
            return zooKeeper.exists(node, false /* watch does not need to be kept */) != null;
        } catch (Exception e) {
            throw new IOException(String.format("Unable to check existence of node '%s'", node), e);
        }
    }

    /**
     * getData sync associated with a specific node without setting a watch
     *
     * @param node - Node path
     * @return byte representation of data in zNode
     * @throws IOException on failure
     */
    public byte[] getData(final String node) throws IOException {
        try {
            return zooKeeper.getData(node, false, new Stat());
        } catch (Exception e) {
            throw new IOException(String.format("Unable to get data from node '%s'", node), e);
        }
    }

    /**
     * setData sync associated with a specific node without setting a watch
     *
     * @param node - Node path
     * @throws IOException on failure
     */
    public void setData(final String node, byte[] data) throws IOException {
        try {
            zooKeeper.setData(node, data, -1);
        } catch (Exception e) {
            throw new IOException(String.format("Unable to set data on node '%s'", node), e);
        }
    }

    /**
     * Set a watch on the data of a node. Persists only until the node's data is changed ONCE.
     *
     * @param node     - the path to the zNode who we want to watch
     * @param onChange - a {@link Runnable} to run *synchronously* once we receive the desired event
     */
    public void watchDataOnce(final String node, Runnable onChange) {
        final AsyncCallback.DataCallback noop = (i, s, o, bytes, stat) -> {
        };
        zooKeeper.getData(node, generateOneTimeDataWatcher(this, node, onChange), noop, null);
    }

    /**
     * Set a watch on the data of a node. Persists forever.
     *
     * @param node        - the path to the zNode who we want to watch
     * @param consumeData - a {@link Consumer} that takes in a byte array i.e. the contents of the node
     */
    public void watchDataForever(final String node, Consumer<byte[]> consumeData) {
        final AsyncCallback.DataCallback callback = (i, s, o, bytes, stat) -> consumeData.accept(bytes);
        zooKeeper.getData(node, generatePersistentDataWatcher(this, node, consumeData), callback, null);
    }

    /**
     * Set a persistent watch on all children of specified node
     *
     * @param node            - the path to the zNode whose children we want to watch
     * @param consumeChildren - a {@link Consumer} that takes in a list of strings i.e. the paths of the children
     */
    public void watchChildrenForever(final String node, Consumer<List<String>> consumeChildren) {
        final AsyncCallback.Children2Callback callback = (i, s, o, list, stat) -> consumeChildren.accept(list);
        zooKeeper.getChildren(node, generatePersistentChildWatcher(this, node, consumeChildren), callback, null);
    }

    /**
     * Set a watch on the deletion of a node. Persists until the node is deleted.
     *
     * @param node      the path to the zNode who we want to watch
     * @param onDeleted a {@link Runnable} to run *synchronously* once we receive the desired event
     */
    public void watchDeletion(final String node, Runnable onDeleted) {
        final AsyncCallback.DataCallback noop = (i, s, o, bytes, stat) -> {
        };
        zooKeeper.getData(node, generateDeletionWatcher(this, node, onDeleted), noop, null);
    }

    /**
     * Convenience method to generate a one-time data watcher
     *
     * @param zk         a {@link ZooKeeperService} instance to use for the getData call
     * @param zNode      the path to the zNode whose data we want to watch
     * @param onComplete a {@link Runnable} to run *synchronously* once we recieve the desired event
     */
    private static Watcher generateOneTimeDataWatcher(ZooKeeperService zk, String zNode, Runnable onComplete) {
        return watchedEvent -> {
            if (watchedEvent.getType() == Watcher.Event.EventType.NodeDataChanged) {
                onComplete.run();
            } else try {
                zk.watchDataOnce(zNode, onComplete);
            } catch (Exception ignored) {
            }
        };
    }

    /**
     * Convenience method to generate a persistent data watcher
     *
     * @param zk          a {@link ZooKeeperService} instance to use for the getData call
     * @param zNode       the path to the zNode whose data we want to watch
     * @param consumeData a {@link Consumer} that takes in a byte array i.e. the contents of the node
     */
    private static Watcher generatePersistentDataWatcher(ZooKeeperService zk, String zNode, Consumer<byte[]> consumeData) {
        return watchedEvent -> zk.watchDataForever(zNode, consumeData);
    }

    /**
     * Convenience method to persistently watch a node's children
     *
     * @param zk              a {@link ZooKeeperService} instance to use for the getChildren call
     * @param zNode           the path to the zNode whose children we want to watch
     * @param consumeChildren a {@link Consumer} that takes in a list of strings i.e. the paths of the children
     */
    private static Watcher generatePersistentChildWatcher(ZooKeeperService zk, String zNode, Consumer<List<String>> consumeChildren) {
        return watchedEvent -> zk.watchChildrenForever(zNode, consumeChildren);
    }

    /**
     * Convenience method to generate a deletion watcher. If the event isn't a deletion, it will reset the watch.
     * If it is a deletion, it will run the callback function and stop watching.
     *
     * @param zk         a {@link ZooKeeperService} instance to use for the getData call
     * @param zNode      the path to the zNode whose data we want to watch
     * @param onComplete a {@link Runnable} to run *synchronously* once we recieve the desired event
     */
    private static Watcher generateDeletionWatcher(ZooKeeperService zk, String zNode, Runnable onComplete) {
        return watchedEvent -> {
            if (watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted) {
                onComplete.run();
            } else try {
                zk.watchDeletion(zNode, onComplete);
            } catch (Exception ignored) {
            }
        };
    }
}