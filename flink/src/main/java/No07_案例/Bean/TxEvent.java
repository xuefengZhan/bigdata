package No07_案例.Bean;

public class TxEvent {
    private String txID;
    private String payChannel;
    private Long eventTime;

    public TxEvent(String txID, String payChannel, Long eventTime) {
        this.txID = txID;
        this.payChannel = payChannel;
        this.eventTime = eventTime;
    }

    public String getTxID() {
        return txID;
    }

    public void setTxID(String txID) {
        this.txID = txID;
    }

    public String getPayChannel() {
        return payChannel;
    }

    public void setPayChannel(String payChannel) {
        this.payChannel = payChannel;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "TxEvent{" +
                "txID='" + txID + '\'' +
                ", payChannel='" + payChannel + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
