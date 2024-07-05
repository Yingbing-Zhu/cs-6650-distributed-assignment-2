import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class Consumer {
    private final static String QUEUE_NAME = "skier_post_queue";
    private static final String SERVER = "c"; // broker url
    // private static final String SERVER = "localhost"; // local test
    private final static Integer NUM_THREADS = 50; // 50; 100; 150
    private static Gson gson = new Gson();
    private static final ConcurrentHashMap<String, JsonObject> skierLiftRides = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(SERVER);
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("rootroot");

        // want the process to stay alive while the consumer is listening asynchronously for messages to arrive
        Connection connection = factory.newConnection();
        // create N threads, each of which uses the channel pool to publish messages
        for (int i = 0; i < NUM_THREADS; i++) {
            Runnable thread = () -> {
                try {
                    // create one channel per thread
                    final Channel channel = connection.createChannel();
                    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                    // allow one message to be delivered at a time
                    channel.basicQos(1);
                    // callback
                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        try {
                            String message = new String(delivery.getBody(), "UTF-8");
                            JsonObject json = gson.fromJson(message, JsonObject.class);
                            int skierId = json.get("skierID").getAsInt();
                            // Remove the skierID from JSON before storing
                            json.remove("skierID");
                            // Store the remaining JSON object
                            skierLiftRides.put(String.valueOf(skierId), json);
                            // System.out.println("Stored data for Skier ID: " + skierId + ": " + json);

                        } catch (RuntimeException e) {
                            //  return  error message
                            System.out.println(" [.] " + e);
                        } finally {
                            // Send response back to the client
                            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        }
                    };
                    channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            new Thread(thread).start();
        }
    }


}
