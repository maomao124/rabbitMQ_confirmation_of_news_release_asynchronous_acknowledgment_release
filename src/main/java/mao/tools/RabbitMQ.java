package mao.tools;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * Project name(项目名称)：rabbitMQ消息发布确认之异步确认发布
 * Package(包名): mao.tools
 * Class(类名): RabbitMQ
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/4/22
 * Time(创建时间)： 19:04
 * Version(版本): 1.0
 * Description(描述)： 无
 */

public class RabbitMQ
{
    @SuppressWarnings("all")
    private static String host = null;
    @SuppressWarnings("all")
    private static String username = null;
    @SuppressWarnings("all")
    private static String password = null;
    private static ConnectionFactory connectionFactory;

    private static final Logger log = LoggerFactory.getLogger(RabbitMQ.class);

    static
    {
        try
        {
            log.debug("开始加载RabbitMQ工具类");
            //从类路径里获得一个输入流
            InputStream inputStream = RabbitMQ.class.getClassLoader().getResourceAsStream("rabbitmq.properties");
            Properties properties = new Properties();
            //加载，读取配置文件
            properties.load(inputStream);
            //取得属性值
            //host
            host = properties.getProperty("rabbitmq.host");
            //用户名
            username = properties.getProperty("rabbitmq.username");
            //密码
            password = properties.getProperty("rabbitmq.password");
            //创建rabbitMQ连接工厂
            connectionFactory = new ConnectionFactory();
            //设置属性
            connectionFactory.setHost(host);
            connectionFactory.setUsername(username);
            connectionFactory.setPassword(password);
            log.info("RabbitMQ host：" + host);
            log.debug("RabbitMQ工具类静态代码块加载完成");
        }
        catch (Exception e)
        {
            e.printStackTrace();
            log.error("退出");
            System.exit(1);
        }
    }

    /**
     * 私有化构造方法
     */
    private RabbitMQ()
    {
    }

    /**
     * 获得rabbitMQ连接工厂
     *
     * @return com.rabbitmq.client.ConnectionFactory
     */
    public static ConnectionFactory getConnectionFactory()
    {
        return connectionFactory;
    }

    /**
     * 获得一个rabbitMQ连接
     *
     * @return com.rabbitmq.client.Connection
     * @throws IOException      IOException
     * @throws TimeoutException 超时
     */
    public static Connection getConnection() throws IOException, TimeoutException
    {
        return connectionFactory.newConnection();
    }

    /**
     * 获得一个rabbitMQ信道
     *
     * @return com.rabbitmq.client.Channel
     * @throws IOException      IOException
     * @throws TimeoutException 超时
     */
    public static Channel getChannel() throws IOException, TimeoutException
    {
        Connection connection = connectionFactory.newConnection();
        return connection.createChannel();
    }

    /**
     * 获得一个rabbitMQ信道
     *
     * @param channelNumber 要分配的频道号
     * @return com.rabbitmq.client.Channel
     * @throws IOException      IOException
     * @throws TimeoutException 超时
     */
    public static Channel getChannel(int channelNumber) throws IOException, TimeoutException
    {
        Connection connection = connectionFactory.newConnection();
        return connection.createChannel(channelNumber);
    }
}
