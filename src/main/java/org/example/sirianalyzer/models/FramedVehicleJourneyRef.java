package org.example.sirianalyzer.models;

/**
 * Reference to a specific dated vehicle journey within a data frame.
 * Links the real-time data to the planned timetable.
 */
public record FramedVehicleJourneyRef(
    /** Operating date of the journey (e.g., "2019-08-16") */
    String dataFrameRef,

    /** Unique identifier for the specific service journey (e.g., "NSB:ServiceJourney:1-2492-2343") */
    String datedVehicleJourneyRef
) {}
