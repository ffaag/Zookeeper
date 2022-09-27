package com.it.case1;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author ZuYingFang
 * @time 2021-11-24 17:20
 * @description 真正的zookeeper是不能区分客户端和服务器端的，都认为是客户端
 * 只是咋们的服务器会在zookeeper上面创建节点存储自身服务器的信息，而客户端只会创建监听来监听这些节点的变化
 */
public class DistributeServer {

    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributeServer server = new DistributeServer();

        // 1 获取zk连接
        server.getConnection();

        // 2 注册服务器到zk集群
        server.register(args[0]);

        // 3 启动业务逻辑（睡觉）
        server.business();

    }

    private void business() throws InterruptedException {

        Thread.sleep(Long.MAX_VALUE);

    }

    private void register(String hostname) throws InterruptedException, KeeperException {

        String s = zooKeeper.create("/servers/" + hostname, hostname.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.printf(hostname + " is online");

    }

    private void getConnection() throws IOException {


        zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {


            }
        });


    }


}
