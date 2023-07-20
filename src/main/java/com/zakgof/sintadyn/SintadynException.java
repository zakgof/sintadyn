package com.zakgof.sintadyn;

public class SintadynException extends RuntimeException {
    public SintadynException(Exception e) {
        super(e);
    }

    public SintadynException(String message) {
        super(message);
    }
}
