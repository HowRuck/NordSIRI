package org.example.sirianalyzer.proto;

import com.google.protobuf.ByteString;

public record LmdbEntry(ByteString key, long hash) {}
