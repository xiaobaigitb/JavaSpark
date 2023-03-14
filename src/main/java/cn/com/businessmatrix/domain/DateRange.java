package cn.com.businessmatrix.domain;

public class DateRange {
    public int start_dt;
    public int end_dt;
    public double remain_ratio;

    public DateRange() {
        this.start_dt = 0;
        this.end_dt = 0;
        this.remain_ratio = 0;
    }

    public DateRange(int x, int y, double z) {
        this.start_dt = x;
        this.end_dt = y;
        this.remain_ratio = z;
    }

}
