package No07_案例.Bean;

public class AdWindowCnt {
    private String province;
    private String time;
    private int count;

    public AdWindowCnt(String province, String time, int count) {
        this.province = province;
        this.time = time;
        this.count = count;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
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
        return "AdWindowCnt{" +
                "province='" + province + '\'' +
                ", time='" + time + '\'' +
                ", count=" + count +
                '}';
    }
}
