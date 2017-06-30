package common.compute.hive.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.weibo.datasys.motan.client.feature.FeatureServiceMotan;
import com.weibo.datasys.motan.client.feature.MidsFeatures;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jialin5 on 2017/5/12.
 */
public class FeatureEngineBlogFeatureGetByRPCUDF extends UDF {
    private FeatureServiceMotan featureServiceMotan = null;
    private boolean _isInited = false;
    private static String DLIMITER = ",";
    private static String M_MID = "m_mid";

    public Text evaluate(String mids, String uids) {
        if (!this._isInited) {
            _init();
        }
        return new Text(dump(mids, uids, "{'101':'m_ctime','103':'m_sourceid'}", ";;abc;;"));
    }

    private boolean _init() {
        try {
            ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:motan-props.xml");
            featureServiceMotan = (FeatureServiceMotan) ctx.getBean("featureServiceRefer");
            this._isInited = true;
        } catch (BeansException e) {
            e.printStackTrace();
        }

        return true;
    }

    private String dump(String mids, String uids, String fidNamePairStr, String separator) {

        String ret = "{}";
        try {
            final Map<String, String> fidNameMap = JSONObject.parseObject(fidNamePairStr, new TypeReference<Map<String, String>>() {});
            final String []fidSet = fidNameMap.keySet().toArray(new String[2]);

            String []fids = new String[fidSet.length];

            for (int i = 0; i < fidSet.length; i++) {
                fids[i] = String.valueOf(fidSet[i]);
            }

            String []midArr = mids.split(DLIMITER);
            String []uidArr = uids.split(DLIMITER);

            final MidsFeatures midsFeatures = featureServiceMotan.getMidsFeatures(midArr, uidArr, fids);
            if (midsFeatures != null && midsFeatures.getCode() == 0 && midsFeatures.getData().size() > 0) {
                final List<String> featureList = replaceChildMapKeyAndReturnNotEmptyChildJsonStrList(midsFeatures.getData(), fidNameMap);
                ret = Joiner.on(separator).join(featureList);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    private List<String> replaceChildMapKeyAndReturnNotEmptyChildJsonStrList(Map<String, Map<String, Object>> midFeatureData, Map<String, String> fidNameMap) {
        List<String> retMapList = Lists.newArrayList();

        for (String mid : midFeatureData.keySet()) {
            final Map<String, Object> midFeatureVal = midFeatureData.get(mid);
            if (midFeatureVal != null && midFeatureVal.size() > 0) {
                final Map<String, Object> newVal = replaceChildMapKey(midFeatureVal, fidNameMap, mid);
                //没值的不要
                if (newVal.size() > 0) {
                    newVal.put(M_MID, mid);
                    retMapList.add(JSON.toJSONString(newVal));
                }
            }
        }

        return retMapList;
    }

    private Map<String, Object> replaceChildMapKey(Map<String, Object> data, Map<String,String> newKeyPair, String mid) {
        Map<String, Object> newValMap = new HashMap<String, Object>();
        for (String key : data.keySet()) {
            Object val = data.get(key);
            if (val != null) {
                newValMap.put(newKeyPair.get(key), data.get(key));
            }
        }

        return newValMap;
    }

    public static void main(String[] args) {
        FeatureEngineBlogFeatureGetByRPCUDF obj = new FeatureEngineBlogFeatureGetByRPCUDF();

        //String mids = "3109458390449637,4109458390449637,4109453466670252";
        //String uids = "1191965271,1191965271,1618051664";
        String mids = "4109458390449637";
        String uids = "1191965271";
        final String res = obj.evaluate(mids, uids).toString();
        System.out.println(res);
    }
}
