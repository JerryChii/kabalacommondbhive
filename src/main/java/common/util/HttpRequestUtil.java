package common.util;

import org.apache.http.client.fluent.Form;
import org.apache.http.client.fluent.Request;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jialin5 on 2017/6/9.
 */
public class HttpRequestUtil {
    public static String postData(String url , Map<String, String> params, String defVal) {
        try {
            Form form = Form.form();
            for (String key : params.keySet()) {
                String val = params.get(key);
                form = form.add(key, val);
            }
            return Request.Post(url).bodyForm(form.build()).execute().returnContent().asString();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return defVal;
    }

    public static void main(String[] args) {
        String url = "http://i2.api.weibo.com/2/statuses/show.json?source=445670032/";
        final Map<String, String> params = new HashMap<String, String>();
        params.put("id","4116765581187151");

        System.out.println(postData(url, params, "{}"));

    }
}
