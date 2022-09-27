package com.it.case2;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-25 11:46
 * @description
 */
public class DistributeLockTest {

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        final DistributeLock lock1 = new DistributeLock();

        final DistributeLock lock2 = new DistributeLock();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.zkLock();
                    System.out.println("线程1已启动，获取到锁");
                    Thread.sleep(5000);

                    lock1.unZkLock();
                    System.out.println("线程1释放锁");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }).start();


        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock2.zkLock();
                    System.out.println("线程2已启动，获取到锁");
                    Thread.sleep(5000);

                    lock2.unZkLock();
                    System.out.println("线程2释放锁");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }).start();


    }


}
