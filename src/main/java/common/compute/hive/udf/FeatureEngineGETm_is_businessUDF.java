package common.compute.hive.udf;

import common.util.JsonArrayUtil;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by jialin5 on 2017/5/12.
 */
public class FeatureEngineGETm_is_businessUDF extends UDF {

    public Text evaluate(String bizId) {
        return new Text("230501".equals(bizId) ? "1" : "0");
    }

    public static void main(String[] args) throws IOException {

        FeatureEngineGETm_is_businessUDF obj = new FeatureEngineGETm_is_businessUDF();
        System.out.println(obj.evaluate("230442"));
        System.out.println(obj.evaluate("230501"));
    }
}
