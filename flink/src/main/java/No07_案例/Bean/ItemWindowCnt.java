package No07_案例.Bean;

public class ItemWindowCnt {

    private long itemId;
    private String timeStamp;
    private int count;

    public ItemWindowCnt(long itemId, String timeStamp, int count) {
        this.itemId = itemId;
        this.timeStamp = timeStamp;
        this.count = count;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
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
        return "ItemWindowCnt{" +
                "itemId=" + itemId +
                ", timeStamp='" + timeStamp + '\'' +
                ", count=" + count +
                '}';
    }
}
