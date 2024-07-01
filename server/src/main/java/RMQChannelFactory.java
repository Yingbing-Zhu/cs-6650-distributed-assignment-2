
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.IOException;
public class RMQChannelFactory extends BasePooledObjectFactory<Channel> {

    // Valid RMQ connection
    private final Connection connection;

    public RMQChannelFactory(Connection connection) {
        this.connection = connection;
    }

    @Override
    synchronized public Channel create() throws IOException {
        return connection.createChannel();
    }

    @Override
    public PooledObject<Channel> wrap(Channel channel) {
        return new DefaultPooledObject<>(channel);
    }

    /*
        check whether a channel is still open before it is borrowed from the pool.
     */
    @Override
    public boolean validateObject(PooledObject<Channel> p) {
        return p.getObject().isOpen();
    }

    @Override
    public void destroyObject(PooledObject<Channel> p) throws Exception {
        if (p.getObject().isOpen()) {
            p.getObject().close();
        }
    }
}