package com.jeeframework.jeetask.startup;

import org.junit.Test;

/**
 * 描述
 *
 * @author lance
 * @version 1.0 2017-04-26 16:52
 */
public class JeeTaskServerTest {
    @Test
    public void start() throws Exception {

        JeeTaskServer jeeTaskServer = new JeeTaskServer(null);
        jeeTaskServer.start();

    }

    public static void main(String[] args) throws Exception {
        JeeTaskServer jeeTaskServer = new JeeTaskServer(null);
        jeeTaskServer.start();


        while (true) {
            try {
                Thread.sleep(1000);

                System.out.println("jeetask  in main  function   ");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}