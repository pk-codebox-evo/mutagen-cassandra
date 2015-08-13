package com.toddfast.mutagen.cassandra;

/**
 * @author Manuel Boillod
 */
public enum MutationStatus {
    SUCCESS("Success"),
    FAILED("Failed"),
    BASELINE("Baseline"),
    BEFORE_BASELINE("<Baseline");

    private String value;

    MutationStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
