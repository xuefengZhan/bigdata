package No07_案例.Bean;

public class PageViewCount {
    private String pv;
    private String time;
    private int count;

    public PageViewCount(String pv, String time, int count) {
        this.pv = pv;
        this.time = time;
        this.count = count;
    }

    public PageViewCount() {
    }

    public String getPv() {
        return pv;
    }

    public void setPv(String pv) {
        this.pv = pv;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "PageViewCount{" +
                "pv='" + pv + '\'' +
                ", time='" + time + '\'' +
                ", count=" + count +
                '}';
    }
}
