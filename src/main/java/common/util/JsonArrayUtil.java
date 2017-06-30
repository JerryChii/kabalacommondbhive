package common.util;

import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion;
import org.json.JSONArray;

/**
 * Created by jialin5 on 2017/6/9.
 */
public class JsonArrayUtil {
    public static String GetFirst(String jsonStr, String defaultVal) {

        try {
            JSONArray jsonArray = new JSONArray(jsonStr);
            if (jsonArray.length() > 0) {
                Object obj = jsonArray.get(0);
                return obj.toString();
            }
        } catch (Exception e) {
        }

        return defaultVal;
    }
}
