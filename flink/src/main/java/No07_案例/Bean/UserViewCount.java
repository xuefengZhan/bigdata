package No07_案例.Bean;

public class UserViewCount {
    private String uv;
    private String time;
    private Integer count;

    public UserViewCount(String uv, String time, Integer count) {
        this.uv = uv;
        this.time = time;
        this.count = count;
    }

    public String getUv() {
        return uv;
    }

    public void setUv(String uv) {
        this.uv = uv;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "UserViewCount{" +
                "uv='" + uv + '\'' +
                ", time='" + time + '\'' +
                ", count=" + count +
                '}';
    }
}
