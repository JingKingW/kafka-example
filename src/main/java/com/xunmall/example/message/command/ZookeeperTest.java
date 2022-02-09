package com.xunmall.example.message.command;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/5/25 10:24
 */
public class ZookeeperTest {

    @Test
    public void testConnectionZkCluster() throws IOException, InterruptedException {
        ZooKeeper zookeeper = new ZooKeeper("10.100.31.41:2181,10.100.31.48:2181,10.100.31.49:2181", 30000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                String path = watchedEvent.getPath();
                System.out.println("path:" + path);

                Event.KeeperState state = watchedEvent.getState();
                System.out.println("keeperState:" + state);

                Event.EventType type = watchedEvent.getType();
                System.out.println("EventType" + type);
            }
        });

        zookeeper.close();
    }

    @Test
    public void manageData() throws IOException, KeeperException, InterruptedException {

        ZooKeeper zookeeper = new ZooKeeper("10.100.31.41:2181,10.100.31.48:2181,10.100.31.49:2181", 30000,null);

        zookeeper.create("/abc","123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Stat stat = new Stat();
        byte[] data = zookeeper.getData("/abc", false, stat);
        System.out.println(new String(data));
        System.out.println("node data length:" + stat.getDataLength());


        zookeeper.getData("/abc", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("path:" + watchedEvent.getPath());
                System.out.println("keeperState:" + watchedEvent.getState());
                System.out.println("EventType" + watchedEvent.getType());
            }
        },null);

        zookeeper.setData("/abc","456".getBytes(),-1);

        zookeeper.setData("/abc","789".getBytes(),-1);

        Thread.sleep(1000);

        zookeeper.close();

    }


    @Test
    public void testZkClient() throws InterruptedException {

        ZkClient zkClient = new ZkClient("10.100.31.41:2181,10.100.31.48:2181,10.100.31.49:2181",30000);

        zkClient.createPersistent("/efg","hello");
        zkClient.createEphemeral("/hij","world");
        zkClient.create("/opq","lulu", CreateMode.EPHEMERAL_SEQUENTIAL);

        String data = zkClient.readData("/efg");
        System.out.println(data);

        zkClient.subscribeStateChanges(new IZkStateListener(){

            @Override
            public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
                System.out.println("state:" + state);
            }

            @Override
            public void handleNewSession() throws Exception {
                System.out.println("new session");
            }

            @Override
            public void handleSessionEstablishmentError(Throwable error) throws Exception {
                error.printStackTrace();
            }
        });

        zkClient.subscribeChildChanges("/", new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                System.out.println("watch path:" + parentPath);
                currentChilds.forEach(str->{
                    System.out.println(str);
                });
            }
        });

        Thread.sleep(10000);
    }





}
