package com.zakgof.sintadyn.model;

import com.zakgof.sintadyn.Key;
import lombok.Data;

@Data
public class Book {
    @Key
    private final String isbn;
    private final String name;
}
