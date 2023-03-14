package cn.com.test;

public class SetHadoopDev {
    private static String devPath = "F:\\ideaProducts\\JavaSpark\\src\\main\\resources\\hadoop-common-2.2.0-bin-master\\bin";

    public static void init() {
        System.setProperty("hadoop.home.dir", devPath);
    }

//    public static void main(String[] args) {
//        init();
//    }
}
