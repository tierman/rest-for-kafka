package pl.icreatesoftware.entrypoint.rest.dto;

import lombok.Getter;
import lombok.Value;

@Value
public class Employee {

    String name;
    int age;

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }
}
