package com.it.case3;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author ZuYingFang
 * @time 2021-11-25 11:59
 * @description
 */
public class CuratorLockTest {

    public static void main(String[] args) {

        // 创建分布式锁1
        InterProcessMutex lock1 = new InterProcessMutex(getCuratorFramework(), "/locks");

        // 创建分布式锁2
        InterProcessMutex lock2 = new InterProcessMutex(getCuratorFramework(), "/locks");

        new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    lock1.acquire();  // 获取到锁，加锁
                    System.out.println("线程1获取到锁");
                    lock1.acquire();  // 获取到锁，加锁
                    System.out.println("线程1再次获取到锁");


                    Thread.sleep(5 * 1000);

                    lock1.release();   // 释放锁
                    System.out.println("线程1释放锁");
                    lock1.release();   // 释放锁
                    System.out.println("线程1再次释放锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();


        new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    lock2.acquire();  // 获取到锁，加锁
                    System.out.println("线程2获取到锁");
                    lock2.acquire();  // 获取到锁，加锁
                    System.out.println("线程2再次获取到锁");


                    Thread.sleep(5 * 1000);

                    lock2.release();   // 释放锁
                    System.out.println("线程2释放锁");
                    lock2.release();   // 释放锁
                    System.out.println("线程2再次释放锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }


    // 获取客户端
    private static CuratorFramework getCuratorFramework() {

        // 连接三秒后还没连上进行重试，重试3次
        ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(3000, 3);

        // 创建客户端
        String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(connectString)
                .connectionTimeoutMs(2000)
                .sessionTimeoutMs(2000)
                .retryPolicy(exponentialBackoffRetry).build();

        // 启动客户端
        client.start();
        System.out.println("启动成功");

        return client;
    }


}
