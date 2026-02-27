package org.example.sirianalyzer.models;

/**
 * Represents a via point in a journey, typically used to indicate intermediate stops
 * or routing information between origin and destination.
 */
public record Via(
    /** Name of the place that the journey passes through */
    String placeName
) {}
