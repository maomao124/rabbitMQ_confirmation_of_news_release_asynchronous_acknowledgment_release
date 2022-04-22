package mao;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import mao.tools.RabbitMQ;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Project name(项目名称)：rabbitMQ消息发布确认之异步确认发布
 * Package(包名): mao
 * Class(类名): Consumer
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/4/22
 * Time(创建时间)： 19:04
 * Version(版本): 1.0
 * Description(描述)： 无
 */

public class Consumer
{
    private static final String QUEUE_NAME = "work";

    public static void main(String[] args) throws IOException, TimeoutException
    {
        Channel channel = RabbitMQ.getChannel();

        channel.basicConsume(QUEUE_NAME, true, new DeliverCallback()
        {
            @Override
            public void handle(String consumerTag, Delivery message) throws IOException
            {
                System.out.println("消息接收");
            }
        }, new CancelCallback()
        {
            @Override
            public void handle(String consumerTag) throws IOException
            {

            }
        });
    }
}
