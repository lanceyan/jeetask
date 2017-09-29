package com.jeeframework.jeetask.startup;

import com.jeeframework.jeetask.startup.server.JeeTaskServerChild;
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

    @Test
    public void startChild() throws Exception {

        JeeTaskServerChild jeeTaskServer = new JeeTaskServerChild();
        jeeTaskServer.start();

    }


    public static void main(String[] args) throws Exception {
        JeeTaskServerChild jeeTaskServer = new JeeTaskServerChild();
        jeeTaskServer.start();


        while (true) {
            int i = 0;
            i = i + 1;
            try {
                Thread.sleep(10);

                if (i % 1000 == 0) {
                    System.out.println("jeetask  in main  function   ");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}