package com.zakgof.sintadyn.model;

import com.zakgof.sintadyn.Key;
import lombok.Data;

@Data
public class Actor {
    @Key
    private final String id;
    private final String fullName;
    private final int birthYear;
}
