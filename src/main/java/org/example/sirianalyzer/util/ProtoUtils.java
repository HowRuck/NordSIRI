package org.example.sirianalyzer.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import java.io.IOException;
import java.util.function.Consumer;

/**
 * Utility class for working with Protocol Buffers (protobuf) data.
 * <br/>
 * Provides helper methods for parsing and manipulating protobuf messages.
 */
public class ProtoUtils {

    /**
     * Checks if a protobuf field tag matches the specified target field number
     * and has the length-delimited wire type.
     *
     * @param tag The protobuf field tag to check
     * @param targetField The target field number to compare against
     * @return true if the tag matches the target field number and is length-delimited, false otherwise
     */
    public static boolean isLengthDelimitedTagEquals(int tag, int targetField) {
        return (
            WireFormat.getTagFieldNumber(tag) == targetField &&
            WireFormat.getTagWireType(tag) ==
            WireFormat.WIRETYPE_LENGTH_DELIMITED
        );
    }

    /**
     * Searches through a protobuf input stream to find the first occurrence
     * of a length-delimited field with the specified target field number.
     *
     * @param input The CodedInputStream containing the protobuf data
     * @param targetField The field number to search for
     * @return The ByteString containing the field data if found, null otherwise
     * @throws IOException If there's an error reading from the input stream
     */
    public static ByteString findFirstLengthDelimitedField(
        CodedInputStream input,
        int targetField
    ) throws IOException {
        while (!input.isAtEnd()) {
            int tag = input.readTag();
            if (tag == 0) {
                break;
            }

            if (isLengthDelimitedTagEquals(tag, targetField)) {
                return input.readBytes();
            } else {
                input.skipField(tag);
            }
        }
        return null;
    }

    /**
     * Applies {@code consumer} to every occurrence of the given {@code targetField}
     * where the wire type is LENGTH_DELIMITED.
     *
     * @param input The CodedInputStream containing the protobuf data
     * @param targetField The field number to search for
     * @param consumer The consumer to apply to each matching field
     * @throws IOException If there's an error reading from the input stream
     *
     * @return number of matching fields processed
     */
    public static int forEachLengthDelimitedField(
        CodedInputStream input,
        int targetField,
        Consumer<CodedInputStream> consumer
    ) throws IOException {
        int count = 0;

        while (!input.isAtEnd()) {
            var tag = input.readTag();

            // Skip non-length-delimited fields
            if (
                WireFormat.getTagWireType(tag) !=
                WireFormat.WIRETYPE_LENGTH_DELIMITED
            ) {
                input.skipField(tag);
                continue;
            }

            if (WireFormat.getTagFieldNumber(tag) == targetField) {
                // Read the length of the field and limit the input stream
                var len = input.readRawVarint32();
                var limitedInput = input.pushLimit(len);

                // Apply the consumer to the limited input stream
                consumer.accept(input);
                input.popLimit(limitedInput);

                count++;
            } else {
                input.skipField(tag);
            }
        }

        return count;
    }
}
