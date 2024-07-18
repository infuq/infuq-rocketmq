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

        DefaultMQProducer producer = new DefaultMQProducer("CID_PRODUCER_ORDER_PAY");

        producer.setNamesrvAddr("192.168.20.36:8876");
        producer.start();


        for (int i = 0; i < 1; i++) {
            try {
                Message msg = new Message("ORDER_FINISH", "foo", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult;
                if (1 == 1)
                    sendResult = producer.send(msg);
                else {
                    sendResult = producer.send(msg, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            return mqs.get(0);
                        }
                    }, "12345");
                }
                System.out.printf("%s%n", sendResult);
                Thread.sleep(2000);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }



        producer.shutdown();
    }
}
