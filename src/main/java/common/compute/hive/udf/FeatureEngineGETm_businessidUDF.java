package common.compute.hive.udf;

import common.util.JsonArrayUtil;
import common.weibo.topic.Topic;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;

/**
 * Created by jialin5 on 2017/5/12.
 */
public class FeatureEngineGETm_businessidUDF extends UDF {

    public Text evaluate(String bizIds) {
        return new Text(JsonArrayUtil.GetFirst(bizIds, ""));
    }

    public static void main(String[] args) throws IOException {

        FeatureEngineGETm_businessidUDF obj = new FeatureEngineGETm_businessidUDF();
        String bizIds = "[230442]";
        System.out.println(obj.evaluate(bizIds));
    }
}
