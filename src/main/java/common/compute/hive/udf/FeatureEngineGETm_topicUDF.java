package common.compute.hive.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.weibo.datasys.motan.client.feature.MidsFeatures;
import common.weibo.topic.Topic;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.http.client.fluent.Form;
import org.apache.http.client.fluent.Request;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jialin5 on 2017/5/12.
 */
public class FeatureEngineGETm_topicUDF extends UDF {

    public Text evaluate(String mcontent) {
        return new Text(dump(mcontent));
    }

    private String dump(String mcontent) {
        String ret = "";

        if (mcontent != null && mcontent.contains("#")) {
            List<String> ls = Topic.getTopics(mcontent);

            for (String l :ls) {
                String id = Topic.getObjectId(l);
                if(id == null) {
                    continue;
                } else {
                    ret = ret + id + "-";
                }

            }
            ;
        }
        if(ret.endsWith("-")) {
            ret  = ret.substring(0, ret.length()-1);
        }
        return ret;
    }


    public static void main(String[] args) throws IOException {

        FeatureEngineGETm_topicUDF obj = new FeatureEngineGETm_topicUDF();
        String mcontent = "看完帅照记得今晚节目有更新。//@何炅快乐店:#何炅# 啊啊啊终于出高清大图，太帅惹！！！";
        System.out.println(obj.evaluate(mcontent));
    }
}
