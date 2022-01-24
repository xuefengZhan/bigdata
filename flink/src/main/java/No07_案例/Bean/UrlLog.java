package No07_案例.Bean;

public class UrlLog {
    private String ip;
    private String userId;
    private Long ts;
    private String method;
    private String url;


    public UrlLog(String ip, String userId, Long ts, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.ts = ts;
        this.method = method;
        this.url = url;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "UrlLog{" +
                "ip='" + ip + '\'' +
                ", userId='" + userId + '\'' +
                ", ts=" + ts +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
