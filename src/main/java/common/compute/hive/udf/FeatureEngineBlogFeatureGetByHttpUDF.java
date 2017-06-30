package common.compute.hive.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.weibo.datasys.motan.client.feature.MidsFeatures;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.http.client.fluent.Form;
import org.apache.http.client.fluent.Request;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by jialin5 on 2017/5/12.
 */
public class FeatureEngineBlogFeatureGetByHttpUDF extends UDF {
    private static String DLIMITER = ",";
    private static String M_MID = "m_mid";
    private static String URL = "http://i.datastrategy.weibo.com/feature/get_mid.json?source=134289536";

    public Text evaluate(String mids, String uids, String fidNamePairStr, String separator) {
        return new Text(dump(mids, uids, fidNamePairStr, separator));
    }

    private String dump(String mids, String uids, String fidNamePairStr, String separator) {

        String ret = "{}";
        try {
            final Map<String, String> fidNameMap = JSONObject.parseObject(fidNamePairStr, new TypeReference<Map<String, String>>() {});
            final Set<String> keySet = fidNameMap.keySet();
            final String []fidSet = keySet.toArray(new String[keySet.size()]);
            String fids = Joiner.on(DLIMITER).join(fidSet);

            final MidsFeatures midsFeatures = postData(mids, uids, fids);
            if (midsFeatures != null && midsFeatures.getCode() == 0 && midsFeatures.getData().size() > 0) {
                final List<String> featureList = replaceChildMapKeyAndReturnNotEmptyChildJsonStrList(midsFeatures.getData(), fidNameMap);
                ret = Joiner.on(separator).join(featureList);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    private MidsFeatures postData(String mids, String uids, String fids) {
        MidsFeatures midsFeatures = null;

        try {
            final String retJson = Request.Post(URL)
                    .bodyForm(Form.form().add("mids", mids).add("uids", uids).add("fids", fids).build())
                    .execute().returnContent().asString();
            midsFeatures = JSONObject.parseObject(retJson, new TypeReference<MidsFeatures>() {});
        } catch (Exception e) {
            e.printStackTrace();
        }

        return midsFeatures;
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

    public static void main(String[] args) throws IOException {

        FeatureEngineBlogFeatureGetByHttpUDF obj = new FeatureEngineBlogFeatureGetByHttpUDF();
        //String mids = "3109458390449637,4109458390449637,4109453466670252";
        //String uids = "1191965271,1191965271,1618051664";
        String mids = "4118234343520601";
        String uids = "5869525717";
        final String res = obj.evaluate(mids, uids, "{\"101\":\"m_ctime\",\"1010\":\"m_has_long_blog_num\",\"1011\":\"m_has_face\",\"1012\":\"m_has_topic\",\"1013\":\"m_is_comment\",\"10147\":\"m_u_signin_frequency\",\"10148\":\"m_u_ia_num_month_fa\",\"10149\":\"m_u_expo_num_month_fa\",\"1015\":\"m_has_link\",\"10150\":\"m_u_ia_num_week\",\"10153\":\"m_u_expo_num_week\",\"1016\":\"m_has_card\",\"10166\":\"m_u_short_interest_tag_1st\",\"10167\":\"m_u_short_interest_tag_2nd\",\"10168\":\"m_u_short_interest_tag_3rd\",\"10169\":\"m_u_fwd_num_month_fa\",\"1017\":\"m_is_long_blog\",\"10170\":\"m_u_cmt_num_month_fa\",\"10171\":\"m_u_lk_num_month_fa\",\"10177\":\"m_u_filtered_attens_num\",\"10178\":\"m_u_filtered_fans_num\",\"1018\":\"m_is_toutiao\",\"1019\":\"m_is_business\",\"102\":\"m_chour\",\"1022\":\"m_topic\",\"1023\":\"m_ad\",\"1025\":\"m_has_pic_num\",\"1026\":\"m_has_long_pic_num\",\"1027\":\"m_has_video_num\",\"1028\":\"m_has_video_miaopai_num\",\"1029\":\"m_has_gif\",\"103\":\"m_sourceid\",\"1030\":\"m_has_pic\",\"1031\":\"m_has_long_pic\",\"1032\":\"m_has_video\",\"1033\":\"m_has_video_inner\",\"1034\":\"m_has_video_miaopai\",\"1035\":\"m_has_music\",\"104\":\"m_is_inner_service\",\"105\":\"m_is_first_tblog_day\",\"106\":\"m_root_mid\",\"107\":\"m_is_original\",\"108\":\"m_is_transmit\",\"1082\":\"m_u_uid\",\"1083\":\"m_u_gender\",\"1084\":\"m_u_age_level\",\"1085\":\"m_u_type\",\"1086\":\"m_u_vtype\",\"1087\":\"m_u_clevel\",\"1088\":\"m_u_tweets_num\",\"1089\":\"m_u_fans_num\",\"109\":\"m_has_card_num\",\"1090\":\"m_u_attens_num\"}", "&&abc&&").toString();
        System.out.println(res);



        /*String url = "http://i.datastrategy.weibo.com/feature/get_mid.json?source=134256784";

        final Content content = Request.Post(url)
                .bodyForm(Form.form().add("mids", "4109458390449637").add("uids", "1191965271").add("fids", "101,103").build())
                .execute().returnContent();
        System.out.println(content.asString());*/
    }
}
