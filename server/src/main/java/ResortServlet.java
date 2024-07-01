import com.google.gson.Gson;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Objects;
import io.swagger.client.model.LiftRide;
import io.swagger.client.model.ResponseMsg;

public class ResortServlet extends HttpServlet {
    private Gson gson = new Gson();

    // handle a POST request
    public void doPost(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
        res.setContentType("application/json");
        String urlPath = req.getPathInfo();

        // check we have a URL!
        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("missing parameters");
            return;
        }

        String[] urlParts = urlPath.split("/");
        // and now validate url path and return the response status code
        // (and maybe also some value if input is valid)

        if (!isUrlValid(urlParts)) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else {
            res.setStatus(HttpServletResponse.SC_OK);
            // do any sophisticated processing with urlParts which contains all the url params
            // TODO: process url params in `urlParts`
            // Parse path parameters
            int resortID = Integer.parseInt(urlParts[1]);
            String seasonID = urlParts[3];
            String dayID = urlParts[5];
            int skierID = Integer.parseInt(urlParts[7]);
            // Parse JSON body
            LiftRide liftRide = gson.fromJson(req.getReader(), LiftRide.class);
            if (liftRide == null) {
                res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }


            res.getWriter().write("It works!");
            res.setStatus(HttpServletResponse.SC_CREATED);
        }



    }
    // handle a GET request
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
        res.setContentType("application/json");
        String urlPath = req.getPathInfo();
        if (urlPath == null) {
            res.setStatus(HttpServletResponse.SC_OK);
            return;
        }
        String[] urlParts = urlPath.split("/");
        // and now validate url path and return the response status code
        // (and maybe also some value if input is valid)

        if (!isUrlValid(urlParts)) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else {
            res.setStatus(HttpServletResponse.SC_OK);
            // do any sophisticated processing with urlParts which contains all the url params
            // TODO: process url params in `urlParts`
            res.getWriter().write("It works!");
        }
    }

    private boolean isUrlValid(String[] urlPath) {
        // check if url is null and the first part is not ""
        if (urlPath == null || !"".equals(urlPath[0])) return false;
        //  urlPath = "/"
        if (urlPath.length == 1) return true;
        // urlPath  ="/1/seasons"
        if (urlPath.length == 3 && "seasons".equals(urlPath[2]) && isInteger(urlPath[1])) return true;
        // urlPath  = "/1/seasons/2019/day/1/skier"
        return urlPath.length == 7 && "seasons".equals(urlPath[2]) && "day".equals(urlPath[4])
                && "skier".equals(urlPath[6]) && isInteger(urlPath[1]) && isInteger(urlPath[3]) && isInteger(urlPath[5]);
    }

    private boolean isInteger(String input) {
        try {
            Integer.parseInt(input);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }



}