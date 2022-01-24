package No07_案例.Bean;

public class UrlWindowCnt {
    private String  Url;
    private String timeStamp;
    private int count;

    public UrlWindowCnt(String url, String timeStamp, int count) {
        Url = url;
        this.timeStamp = timeStamp;
        this.count = count;
    }

    public String getUrl() {
        return Url;
    }

    public void setUrl(String url) {
        Url = url;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "UrlWindowCnt{" +
                "Url='" + Url + '\'' +
                ", timeStamp='" + timeStamp + '\'' +
                ", count=" + count +
                '}';
    }
}
