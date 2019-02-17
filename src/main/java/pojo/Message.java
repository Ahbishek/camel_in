package pojo;

import java.io.Serializable;

public class Message implements Serializable {

    private String answer;

    public Message(String answer) {
        this.answer = answer;
    }


    public String getAnswer() {
        return answer;
    }

    public void setAnswer(String answer) {
        this.answer = answer;
    }

    @Override
    public String toString() {
        return "Message{" +
                "Answer='" + answer + '\'' +
                '}';
    }
}
