package ru.alexeyva;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Task {

    String name;
    Status status;
    String result;


    public enum Status {
        NEW, IN_PROGRESS, DONE
    }

}
