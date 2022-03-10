package No12_Join.Bean;

public class User {
    private String username;
    private int cityid;
    private String timestamp;

    public User(String username, int cityid, String timestamp) {
        this.username = username;
        this.cityid = cityid;
        this.timestamp = timestamp;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getCityid() {
        return cityid;
    }

    public void setCityid(int cityid) {
        this.cityid = cityid;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
