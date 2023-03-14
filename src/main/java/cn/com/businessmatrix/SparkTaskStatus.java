package cn.com.businessmatrix;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SparkTaskStatus {

    private static String readAll(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }

    public static JSONArray readJsonFromUrl(String url) throws IOException, JSONException {
        InputStream is = new URL(url).openStream();
        try {
            BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
            String jsonText = readAll(rd);
            //System.out.println("==============================");
            //System.out.println(jsonText);
            //System.out.println("==============================");
            JSONArray jsonArray = new JSONArray(jsonText);
            return jsonArray;
        } finally {
            is.close();
        }
    }

    public static int getFailedTasksSum(JSONArray jsonArray) throws JSONException {
        int failedTasks = 0;
        int numTasks = 0;
        int numCompletedTasks = 0;
        JSONObject json = null;
        for (int i = 0; i < jsonArray.length(); i++) {
            json = (JSONObject) jsonArray.get(i);
            if (json.has("numTasks"))
                numTasks = json.getInt("numTasks");
            else if (json.has("numCompletedTasks"))
                numCompletedTasks = json.getInt("numCompletedTasks");
        }
        failedTasks = numTasks - numCompletedTasks;
        return failedTasks;
    }


    public static void main(String[] args) throws IOException, JSONException, ParseException {
        String urlPre = args[0];//"http://cdh03:18089";//args[0];//机器名和端口地址:http://cdh03:18089
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.000GMT'");

        String gmt = args[1];//"20200519";//args[1];//最小时间，北京时间，程序自动转成gmt时间
        //System.out.println(sdf2.format(sdf1.parse(gmt)));
        String name = args[2];//"Hive on Spark (sessionId = 60cc9480-2baa-4134-801d-33cbbe15b30a)";//"cn.com.businessmatrix.SparkAgencySplit";//args[2];
        //System.out.println("==============================");
        //System.out.println(urlPre+"/api/v1/applications?minDate="+sdf2.format(sdf1.parse(gmt)));
        //System.out.println("==============================");
        JSONArray jsonArray = readJsonFromUrl(urlPre
                + (urlPre.endsWith("/") ? "" : "/") + "api/v1/applications?minDate=" + sdf2.format(sdf1.parse(gmt)));
        JSONObject json = null;
        JSONArray failedArray = null;
        JSONArray failedArrayJobs = null;
        for (int i = 0; i < jsonArray.length(); i++) {
            json = (JSONObject) jsonArray.get(i);
            if (name.equals(json.get("name"))) {
                //这里没有判断多个的情况，如果name是唯一的，就不用判断存在多个的情况
                JSONArray attempts = json.getJSONArray("attempts");
                JSONObject jsonAtt = null;

                int attemptId = 0;
                for (int j = 0; j < attempts.length(); j++) {
                    jsonAtt = (JSONObject) attempts.get(j);
                    if (jsonAtt.has("attemptId")) {
                        //cluster模式,找出最大attemptId
                        if (jsonAtt.getInt("attemptId") > attemptId)
                            attemptId = jsonAtt.getInt("attemptId");
                    } else {
                        //cliient模式
                        //http://cdh03:18089/api/v1/applications/application_1589260110161_0056/executors
                        failedArray = readJsonFromUrl(urlPre + "/api/v1/applications/" + json.get("id") + "/executors");
                    }
                }

                if (attemptId > 0) {
                    // failedArray=readJsonFromUrl(urlPre+"/api/v1/applications/"+json.get("id")+"/"+attemptId+"/executors");
                    failedArrayJobs = readJsonFromUrl(urlPre + "/api/v1/applications/" + json.get("id") + "/" + attemptId + "/jobs");
                }

                break;
            }// 找到唯一name
        }//end for i

        int failedTasks =//getFailedTasksSum(failedArray)+
                getFailedTasksSum(failedArrayJobs);
        System.out.println(failedTasks);
        if (failedTasks > 0)
            System.exit(1);
    }

}

