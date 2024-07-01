import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.swagger.client.model.LiftRide;
import io.swagger.client.model.ResponseMsg;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class SkierServlet extends HttpServlet {
    private static final String QUEUE_NAME = "skier_post_queue";
    private static final String SERVER = "54.185.16.128"; // broker url
    // private static final String SERVER = "127.0.0.1";
    private GenericObjectPool<Channel> channelPool;
    private Gson gson = new Gson();
    // Create a response message
    private ResponseMsg responseMsg = new ResponseMsg();
    // number of concurrent threads that utilize the pool.
    private static final int ON_DEMAND = -1;
    // the durtaion in seconds a client waits for a channel to be available in the pool
    private static final int WAIT_TIME_SECS = 1;

    @Override
    public void init( ) {
        // long-lived connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(SERVER);
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("rootroot");

        final Connection conn;
        try {
            conn = factory.newConnection();
        } catch (TimeoutException | IOException e) {
            throw new RuntimeException(e);
        }

        // create a channel pool that shares a bunch of pre-created channels
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();

        config.setMaxTotal(ON_DEMAND);
        // clients will block when pool is exhausted, for a maximum duration of WAIT_TIME_SECS
        config.setBlockWhenExhausted(true);
        config.setMaxWait(Duration.ofSeconds(WAIT_TIME_SECS));

        // The channel factory generates new channels on demand, as needed by the GenericObjectPool
        RMQChannelFactory chanFactory = new RMQChannelFactory (conn);
        //create the pool
        channelPool = new GenericObjectPool<>(chanFactory, config);

    }

    // handle a POST request
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res)
            throws IOException {
        res.setContentType("application/json");

        String urlPath = req.getPathInfo();

        // check we have a URL!
        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            responseMsg.setMessage("no url");
            res.getWriter().write(gson.toJson(responseMsg));
            return;
        }

        String[] urlParts = urlPath.split("/");
        // and now validate url path and return the response status code
        if (!isUrlValid(urlParts)) {
            responseMsg.setMessage("invalid url");
            res.getWriter().write(gson.toJson(responseMsg));
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        // Parse JSON body
        LiftRide liftRide = gson.fromJson(req.getReader(), LiftRide.class);
        if (liftRide == null) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        // Parse path parameters
        int resortID = Integer.parseInt(urlParts[1]);
        String seasonID = urlParts[3];
        String dayID = urlParts[5];
        int skierID = Integer.parseInt(urlParts[7]);

        Map<String, Object> liftRideData = new HashMap<>();
        liftRideData.put("liftRide", liftRide);
        liftRideData.put("resortID", resortID);
        liftRideData.put("seasonID", seasonID);
        liftRideData.put("dayID", dayID);
        liftRideData.put("skierID", skierID);

        String jsonMessage = gson.toJson(liftRideData);
        Channel channel = null;
        try {
            // get a channel from the pool
            channel = channelPool.borrowObject();
            // publish a message
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            byte[] payLoad = jsonMessage.getBytes();
            channel.basicPublish("", QUEUE_NAME, null, payLoad);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (channel != null) {
                // return the channel to the pool
                channelPool.returnObject(channel);
            }
        }

        responseMsg.setMessage("Lift ride recorded successfully");
        res.getWriter().write(gson.toJson(responseMsg));
        res.setStatus(HttpServletResponse.SC_CREATED);
    }

    // handle a GET request
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
        res.setContentType("application/json");
        String urlPath = req.getPathInfo();

        // check we have a URL!
        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            responseMsg.setMessage("no url");
            res.getWriter().write(gson.toJson(responseMsg));
            return;
        }

        String[] urlParts = urlPath.split("/");
        // and now validate url path and return the response status code
        if (!isUrlValid(urlParts)) {
            responseMsg.setMessage("invalid url");
            res.getWriter().write(gson.toJson(responseMsg));
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        } else {
            // Parse path parameters
            int resortID = Integer.parseInt(urlParts[1]);
            String seasonID = urlParts[3];
            String dayID = urlParts[5];
            int skierID = Integer.parseInt(urlParts[7]);

            responseMsg.setMessage(String.format(
                    "Lift ride vertical value get successfully for: Resort ID - %d, Season ID - %s, Day ID - %s, Skier ID - %d",
                    resortID, seasonID, dayID, skierID
            ));
            res.getWriter().write(gson.toJson(responseMsg));
            res.setStatus(HttpServletResponse.SC_OK);
        }
    }

    private boolean isUrlValid(String[] urlPath) {
        // urlPath  = "/1/seasons/2019/day/1/skier/123"
        // urlParts = [, 1, seasons, 2019, day, 1, skier, 123]
        // Check if the URL path has the exact number of parts required
        if (urlPath.length != 8) {
            return false;
        }
        if (!"seasons".equals(urlPath[2]) || !"days".equals(urlPath[4]) || !"skiers".equals(urlPath[6])) {
            return false;
        }
        // validation for dayID and check for integer type
        return isValidDayID(urlPath[5]) && isInteger(urlPath[1]) && isInteger(urlPath[7]);
    }

    private boolean isInteger(String input) {
        try {
            Integer.parseInt(input);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isValidDayID(String dayID) {
        try {
            int day = Integer.parseInt(dayID);
            return day >= 1 && day <= 366;
        } catch (NumberFormatException e) {
            return false;
        }
    }


}