package com.ksayers.twoPhaseCommit.utils;

public class Pair<First, Second> {
    public First first;
    public Second second;

    public Pair(First first, Second second) {
        this.first = first;
        this.second = second;
    }

    public String toString() {
        return String.format("Pair(%s, %s)", first, second);
    }
}
