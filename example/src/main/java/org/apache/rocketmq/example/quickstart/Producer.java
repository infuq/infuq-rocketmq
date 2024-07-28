package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

public class Producer {


    public static void main(String[] args) throws MQClientException, InterruptedException {

        DefaultMQProducer producer = new DefaultMQProducer("CID_PRODUCER_ORDER");

        String nameSrvAddr = "192.168.20.36:8876";
        producer.setNamesrvAddr(nameSrvAddr);
        producer.start();


        for (int i = 0; i < 3; i++) {
            try {
                String data = "This is " + (i + 1) + "message.";
                Message message = new Message("ORDER_FINISH", "foo", data.getBytes(RemotingHelper.DEFAULT_CHARSET));

                SendResult sendResult;
                if (i % 2 == 0)
                    sendResult = producer.send(message);
                else {
                    sendResult = producer.send(message, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            return mqs.get(0);
                        }
                    }, "12345");
                }

                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        producer.shutdown();
    }
}
