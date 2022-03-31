package No12_Join.Bean;

public class JoinBean {
    String name;
    String age;
    Long ts;

    public JoinBean(String name, String age, Long ts) {
        this.name = name;
        this.age = age;
        this.ts = ts;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "JoinBean{" +
                "name='" + name + '\'' +
                ", age='" + age + '\'' +
                ", ts=" + ts +
                '}';
    }
}
