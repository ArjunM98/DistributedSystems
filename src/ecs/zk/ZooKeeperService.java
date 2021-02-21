package ecs.zk;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import shared.messages.KVAdminMessageProto;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperService {

    private static final Logger logger = Logger.getRootLogger();
    private ZooKeeper zooKeeper;

    /**
     * Establishes Connection to the Zookeeper Ensemble
     *
     * @param url - Zookeeper Ensemble Location (host:port)
     * @throws IOException
     * @throws InterruptedException
     */
    public ZooKeeperService(final String url) throws IOException, InterruptedException {
        logger.info("Establishing Zookeeper Connection ...");
        CountDownLatch connectionLatch = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(url, 2000 /* sessionTimeout */, watchedEvent -> {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectionLatch.countDown();
            }
        });
        connectionLatch.await();
        logger.info("Connection Established to Zookeeper Ensemble");
    }

    /**
     * Creates a new znode
     *
     * @param node      - Node name
     * @param ephemeral - Boolean flag for persisting ZNode on connection loss
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String createNode(final String node, KVAdminMessageProto msg, final boolean ephemeral) throws KeeperException, InterruptedException {
        return zooKeeper.create(
                node,
                msg.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                (ephemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT));
    }

    /**
     * Checks whether the specified node with the described path exists
     *
     * @param node - Node path
     * @return boolean
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean nodeExists(final String node) throws KeeperException, InterruptedException {
        return zooKeeper.exists(node, false /* watch does not need to be kept */) != null;
    }

    /**
     * getData async associated with a specific code
     *
     * @param node - Node path
     * @return boolean
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void getData(final String node, Watcher changedState, AsyncCallback.DataCallback handleChangedState) {
        zooKeeper.getData(node, changedState, handleChangedState, null);
    }

    /**
     * getData sync associated with a specific code
     *
     * @param node
     * @param changedState
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] getData(final String node, Watcher changedState) throws KeeperException, InterruptedException {
        return zooKeeper.getData(node, changedState, new Stat());
    }

    /**
     * getData sync associated with a specific node without setting a watch
     * @param node
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] getData(final String node) throws KeeperException, InterruptedException {
        return zooKeeper.getData(node, false, new Stat());
    }

    /**
     * setData sync associated with a specific code without setting a watch
     * @param node
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void setData(final String node, byte[] data) throws KeeperException, InterruptedException {
        zooKeeper.setData(node, data, -1);
    }

    /**
     * Gets all children of specified node, and sets a watch
     *
     * @param node
     * @param changedChildren
     * @return List<String>
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void getChildren(final String node, Watcher changedChildren, AsyncCallback.Children2Callback handleChange) {
        zooKeeper.getChildren(node, changedChildren, handleChange, null);
    }

    /**
     * delete specified node
     * @param node
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void deleteNode(final String node) throws KeeperException, InterruptedException {
        zooKeeper.delete(node, -1);
    }

}
