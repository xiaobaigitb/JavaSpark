package cn.com.tool;

import com.spire.xls.FileFormat;
import com.spire.xls.Workbook;

public class XmlToExcel {
    public static void main(String[] args) {
        //加载xml文件或 Office OpenXml 类型的xsl文件
        Workbook wb = new Workbook();
        wb.loadFromXml("F:\\test.xml");

        //转为2013版xlsx格式的Excel
        wb.saveToFile("F:\\newExcel.xlsx", FileFormat.Version2013);
    }
}
