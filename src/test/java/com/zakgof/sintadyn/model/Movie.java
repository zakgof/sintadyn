package com.zakgof.sintadyn.model;

import com.zakgof.sintadyn.Key;
import lombok.Data;

@Data
public class Movie {
    @Key
    private final String id;
    private final String title;
    private final int year;
}
