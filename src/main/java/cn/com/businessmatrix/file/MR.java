package cn.com.businessmatrix.file;

import cn.com.businessmatrix.domain.DataItem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

public class MR {
    static final char COL_DELIMITER = '\u0001';
    static final char ROW_DELIMITER = '\n';
    static final String FILE_ENCODING = "UTF-8";

    public MR() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> argsMap = new HashMap();

        for (int i = 0; i < args.length; ++i) {
            String[] arr = args[i].replaceAll("--", "").split("=");
            argsMap.put(arr[0], arr[1]);
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream((String) argsMap.get("infile")), (String) argsMap.get("infile_encoding")));
        DataItem[] items = null;
        Map<String, DataItem> map = DataItem.fromJsonDataDict((String) argsMap.get("json_file"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream((String) argsMap.get("outfile")), FILE_ENCODING));
        String line = null;
        String outLine = null;
        int i = 0;
        int cols = 0;
        int rows = 0;

        while ((line = br.readLine()) != null) {
            ++i;
            if ("OFDCFEND".equals(line)) {
                break;
            }

            if (i >= 10) {
                if (i == 10) {
                    cols = Integer.parseInt(line);
                    items = new DataItem[cols];
                } else if (i <= 10 + cols) {
                    items[i - 11] = (DataItem) map.get(line);
                } else if (i == 11 + cols) {
                    rows = Integer.parseInt(line);
                } else {
                    outLine = "";
                    int len = 0;
                    byte[] bs = line.getBytes((String) argsMap.get("infile_encoding"));
                    System.out.println("filedLength:" + bs.length);

                    for (int m = 0; m < cols; ++m) {
                        outLine = outLine + items[m].parse(new String(bs, len, items[m].scale, (String) argsMap.get("infile_encoding"))) + (m < items.length ? COL_DELIMITER : "");
                        len += items[m].scale;
                    }

                    bw.write(outLine + ROW_DELIMITER);
                }
            }
        }

        br.close();
        bw.close();
        if (rows != 0 && rows != i - cols - 11 - 1) {
            throw new Exception("文件中的定义数据量和数据行数不一致");
        }
    }
}

