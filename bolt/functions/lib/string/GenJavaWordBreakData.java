/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Generates the C++ tables used to reproduce ByteOpenJDK 17.0.9+9
 * root-locale word boundaries.
 *
 * <p>Run this source file with the exact compatibility JDK from its directory:
 *
 * <pre>{@code
 * "$JAVA_HOME/bin/java" GenJavaWordBreakData.java \
 *     --output OpenJdkWordBreakData.inc
 * "$JAVA_HOME/bin/java" GenJavaWordBreakData.java \
 *     --check OpenJdkWordBreakData.inc
 * }</pre>
 */
public final class GenJavaWordBreakData {
  private static final String COMPATIBILITY_JAVA_VERSION = "17.0.9";
  private static final String COMPATIBILITY_JAVA_RUNTIME_VERSION = "17.0.9+9";
  private static final String COMPATIBILITY_JAVA_VENDOR = "ByteOpenJDK";
  private static final String COMPATIBILITY_WORD_BREAK_DATA_SHA256 =
      "2bb4e3482bd6fe639acf5ba14af071acf397dc3a1a6f181ad35e18e5ff98fe1c";
  private static final String COMPATIBILITY_CASED_RANGES_SHA256 =
      "0553ff2821178186973a522f5d05687dbcf7d166be6c8882771eba0ce1f36e59";
  private static final byte[] MAGIC = {'B', 'I', 'd', 'a', 't', 'a', 0};
  private static final int SUPPORTED_VERSION = 1;
  private static final int BMP_INDEX_LENGTH = 512;
  private static final int MIN_SUPPLEMENTARY_CODE_POINT = 0x10000;
  private static final int MAX_CODE_POINT = 0x10ffff;

  private GenJavaWordBreakData() {}

  public static void main(String[] args) throws Exception {
    check(
        args.length == 0
            || (args.length == 2
                && (args[0].equals("--output") || args[0].equals("--check"))),
        "Usage: java GenJavaWordBreakData.java "
            + "[--output <file> | --check <file>]");

    String javaVersion = System.getProperty("java.version");
    String javaRuntimeVersion = System.getProperty("java.runtime.version");
    String javaVendor = System.getProperty("java.vendor");
    if (!COMPATIBILITY_JAVA_VERSION.equals(javaVersion)
        || !COMPATIBILITY_JAVA_RUNTIME_VERSION.equals(javaRuntimeVersion)
        || !COMPATIBILITY_JAVA_VENDOR.equals(javaVendor)) {
      throw new IllegalStateException(
          "Run this generator with the ByteOpenJDK "
              + COMPATIBILITY_JAVA_VERSION
              + " compatibility baseline; got java.version="
              + javaVersion
              + ", java.runtime.version="
              + javaRuntimeVersion
              + ", java.vendor="
              + javaVendor);
    }

    byte[] bytes = readWordBreakData();
    String sourceSha256 = sha256(bytes);
    check(
        COMPATIBILITY_WORD_BREAK_DATA_SHA256.equals(sourceSha256),
        "WordBreakIteratorData SHA-256 mismatch: " + sourceSha256);
    WordBreakData data = parse(bytes);
    validate(data);
    List<CodePointRange> casedRanges = makeCasedRanges();
    String casedRangesSha256 = casedRangesSha256(casedRanges);
    check(
        COMPATIBILITY_CASED_RANGES_SHA256.equals(casedRangesSha256),
        "ConditionalSpecialCasing cased ranges SHA-256 mismatch: "
            + casedRangesSha256);

    StringWriter generated = new StringWriter();
    PrintWriter writer = new PrintWriter(generated);
    emit(writer, data, sourceSha256, casedRangesSha256, casedRanges);
    writer.flush();

    if (args.length == 0) {
      System.out.print(generated);
      return;
    }

    Path output = Path.of(args[1]);
    if (args[0].equals("--output")) {
      // Do not open the destination until every compatibility and data check
      // has passed, so a wrong JDK cannot truncate the checked-in file.
      Files.writeString(output, generated.toString(), StandardCharsets.UTF_8);
      return;
    }

    String checkedIn = Files.readString(output, StandardCharsets.UTF_8);
    check(
        generated.toString().equals(checkedIn),
        output
            + " is not the exact output of this generator and compatibility JDK");
    System.err.println(output + " matches ByteOpenJDK 17.0.9+9 exactly");
  }

  private static byte[] readWordBreakData() throws IOException {
    URI uri = URI.create("jrt:/");
    FileSystem fileSystem = FileSystems.getFileSystem(uri);
    Path path = fileSystem.getPath(
        "/modules/java.base/sun/text/resources/WordBreakIteratorData");
    return Files.readAllBytes(path);
  }

  private static WordBreakData parse(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
    byte[] magic = new byte[MAGIC.length];
    buffer.get(magic);
    check(Arrays.equals(magic, MAGIC), "wrong magic");
    check(Byte.toUnsignedInt(buffer.get()) == SUPPORTED_VERSION, "wrong version");
    int dataSize = buffer.getInt();
    check(dataSize == buffer.remaining(), "wrong total data size");

    int forwardLength = checkedLength(buffer.getInt(), "forward table");
    int backwardLength = checkedLength(buffer.getInt(), "backward table");
    int endStatesLength = checkedLength(buffer.getInt(), "end states");
    int lookaheadStatesLength = checkedLength(buffer.getInt(), "lookahead states");
    int bmpDataLength = checkedLength(buffer.getInt(), "BMP data");
    int supplementaryLength = checkedLength(buffer.getInt(), "supplementary data");
    int additionalLength = checkedLength(buffer.getInt(), "additional data");
    long checksum = buffer.getLong();

    int[] forward = readUnsignedShorts(buffer, forwardLength);
    int[] backward = readUnsignedShorts(buffer, backwardLength);
    byte[] endStates = readBytes(buffer, endStatesLength);
    byte[] lookaheadStates = readBytes(buffer, lookaheadStatesLength);
    int[] bmpIndices = readUnsignedShorts(buffer, BMP_INDEX_LENGTH);
    byte[] bmpCategories = readBytes(buffer, bmpDataLength);
    int[] supplementary = readInts(buffer, supplementaryLength);
    byte[] additional = readBytes(buffer, additionalLength);
    check(!buffer.hasRemaining(), "trailing data");

    return new WordBreakData(
        forward,
        backward,
        endStates,
        lookaheadStates,
        bmpIndices,
        bmpCategories,
        supplementary,
        additional,
        checksum);
  }

  private static int checkedLength(int length, String name) {
    check(length >= 0, "negative " + name + " length");
    return length;
  }

  private static int[] readUnsignedShorts(ByteBuffer buffer, int length) {
    int[] result = new int[length];
    for (int i = 0; i < length; ++i) {
      result[i] = Short.toUnsignedInt(buffer.getShort());
    }
    return result;
  }

  private static int[] readInts(ByteBuffer buffer, int length) {
    int[] result = new int[length];
    for (int i = 0; i < length; ++i) {
      result[i] = buffer.getInt();
    }
    return result;
  }

  private static byte[] readBytes(ByteBuffer buffer, int length) {
    byte[] result = new byte[length];
    buffer.get(result);
    return result;
  }

  private static void validate(WordBreakData data) {
    check(data.endStates.length > 1, "missing forward states");
    check(
        data.lookaheadStates.length == data.endStates.length,
        "end/lookahead state size mismatch");
    check(
        data.forward.length % data.endStates.length == 0,
        "invalid forward table dimensions");

    int categories = data.forward.length / data.endStates.length;
    check(categories > 0 && categories < 0xff, "invalid category count");
    check(
        data.backward.length % categories == 0,
        "invalid backward table dimensions");
    int backwardStates = data.backward.length / categories;

    for (int state : data.forward) {
      check(state < data.endStates.length, "forward state out of bounds");
    }
    for (int state : data.backward) {
      check(state < backwardStates, "backward state out of bounds");
    }
    for (byte flag : data.endStates) {
      check(flag == 0 || flag == 1, "invalid end-state flag");
    }
    for (byte flag : data.lookaheadStates) {
      check(flag == 0, "word data unexpectedly uses lookahead states");
    }
    check(data.additional.length == 0, "unexpected additional data");

    for (int index : data.bmpIndices) {
      check(index + 127 < data.bmpCategories.length, "BMP index out of bounds");
    }
    for (byte category : data.bmpCategories) {
      validateCategory(category, categories);
    }
    check(data.supplementary.length >= 2, "missing supplementary ranges");
    int previousStart = -1;
    for (int entry : data.supplementary) {
      int start = entry >>> 8;
      // OpenJDK data can contain an empty range represented by two adjacent
      // entries with the same start code point.
      check(start >= previousStart, "supplementary ranges are not ordered");
      validateCategory((byte) entry, categories);
      previousStart = start;
    }
    check(
        (data.supplementary[0] >>> 8) == MIN_SUPPLEMENTARY_CODE_POINT,
        "wrong first supplementary range");
    check(
        (data.supplementary[data.supplementary.length - 1] >>> 8)
            == MAX_CODE_POINT + 1,
        "missing supplementary sentinel");

    check(
        lookupCategory(data, 0x03a3) == lookupCategory(data, 0x03c3)
            && lookupCategory(data, 0x03a3) == lookupCategory(data, 0x03c2),
        "Greek sigma variants have different word categories");
    check(computeOpenJdkChecksum(data) == data.checksum, "checksum mismatch");
  }

  private static void validateCategory(byte category, int categories) {
    int value = category;
    check(value == -1 || (value >= 0 && value < categories), "category out of bounds");
  }

  private static int lookupCategory(WordBreakData data, int codePoint) {
    if (codePoint < MIN_SUPPLEMENTARY_CODE_POINT) {
      int index = data.bmpIndices[codePoint >>> 7] + (codePoint & 0x7f);
      return data.bmpCategories[index];
    }
    int low = 0;
    int high = data.supplementary.length - 1;
    while (true) {
      int middle = (low + high) / 2;
      int start = data.supplementary[middle] >>> 8;
      int limit = data.supplementary[middle + 1] >>> 8;
      if (codePoint < start) {
        high = middle;
      } else if (codePoint >= limit) {
        low = middle;
      } else {
        int category = data.supplementary[middle] & 0xff;
        return category == 0xff ? -1 : category;
      }
    }
  }

  // OpenJDK's builder feeds the low byte of short/int table entries to CRC32.
  private static long computeOpenJdkChecksum(WordBreakData data) {
    CRC32 crc = new CRC32();
    updateLowBytes(crc, data.forward);
    updateLowBytes(crc, data.backward);
    crc.update(data.endStates);
    crc.update(data.lookaheadStates);
    updateLowBytes(crc, data.bmpIndices);
    crc.update(data.bmpCategories);
    updateLowBytes(crc, data.supplementary);
    crc.update(data.additional);
    return crc.getValue();
  }

  private static void updateLowBytes(CRC32 crc, int[] values) {
    for (int value : values) {
      crc.update(value);
    }
  }

  private static List<CodePointRange> makeCasedRanges() {
    List<CodePointRange> result = new ArrayList<>();
    int rangeStart = -1;
    for (int codePoint = 0; codePoint <= MAX_CODE_POINT + 1; ++codePoint) {
      boolean cased = codePoint <= MAX_CODE_POINT && isOpenJdkCased(codePoint);
      if (cased && rangeStart < 0) {
        rangeStart = codePoint;
      } else if (!cased && rangeStart >= 0) {
        result.add(new CodePointRange(rangeStart, codePoint - 1));
        rangeStart = -1;
      }
    }
    return result;
  }

  // Mirrors java.lang.ConditionalSpecialCasing.isCased in OpenJDK 17.0.9.
  private static boolean isOpenJdkCased(int codePoint) {
    int type = Character.getType(codePoint);
    if (type == Character.LOWERCASE_LETTER
        || type == Character.UPPERCASE_LETTER
        || type == Character.TITLECASE_LETTER) {
      return true;
    }
    return (codePoint >= 0x02b0 && codePoint <= 0x02b8)
        || (codePoint >= 0x02c0 && codePoint <= 0x02c1)
        || (codePoint >= 0x02e0 && codePoint <= 0x02e4)
        || codePoint == 0x0345
        || codePoint == 0x037a
        || (codePoint >= 0x1d2c && codePoint <= 0x1d61)
        || (codePoint >= 0x2160 && codePoint <= 0x217f)
        || (codePoint >= 0x24b6 && codePoint <= 0x24e9);
  }

  private static void emit(
      PrintWriter out,
      WordBreakData data,
      String sourceSha256,
      String casedRangesSha256,
      List<CodePointRange> casedRanges) {
    int categories = data.forward.length / data.endStates.length;
    int backwardStates = data.backward.length / categories;

    out.println("/*");
    out.println(" * Copyright (c) ByteDance Ltd. and/or its affiliates.");
    out.println(" *");
    out.println(
        " * Licensed under the Apache License, Version 2.0 (the \"License\");");
    out.println(" * you may not use this file except in compliance with the License.");
    out.println(" * You may obtain a copy of the License at");
    out.println(" *");
    out.println(" *     http://www.apache.org/licenses/LICENSE-2.0");
    out.println(" *");
    out.println(" * Unless required by applicable law or agreed to in writing, software");
    out.println(" * distributed under the License is distributed on an \"AS IS\" BASIS,");
    out.println(
        " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
    out.println(" * See the License for the specific language governing permissions and");
    out.println(" * limitations under the License.");
    out.println(" */");
    out.println();
    out.println("// Generated by GenJavaWordBreakData.java. Do not edit.");
    out.println("// java.version=" + System.getProperty("java.version"));
    out.println(
        "// java.runtime.version=" + System.getProperty("java.runtime.version"));
    out.println("// java.vendor=" + System.getProperty("java.vendor"));
    out.println("// WordBreakIteratorData.sha256=" + sourceSha256);
    out.println(
        "// ConditionalSpecialCasing.casedRanges.sha256=" + casedRangesSha256);
    out.println("// clang-format off");
    out.println();
    emitScalar(out, "uint16_t", "kJavaWordCategoryCount", categories);
    emitScalar(
        out, "uint16_t", "kJavaWordForwardStateCount", data.endStates.length);
    emitScalar(out, "uint16_t", "kJavaWordBackwardStateCount", backwardStates);
    emitScalar(out, "uint64_t", "kJavaWordDataChecksum", data.checksum, "ULL");
    emitArray(
        out, "uint16_t", "kJavaWordForwardTransitions", data.forward, 16, false);
    emitArray(
        out,
        "uint16_t",
        "kJavaWordBackwardTransitions",
        data.backward,
        16,
        false);
    emitArray(out, "uint8_t", "kJavaWordEndStates", data.endStates, 32, false);
    emitArray(out, "uint16_t", "kJavaWordBmpIndices", data.bmpIndices, 16, false);
    emitArray(
        out, "int8_t", "kJavaWordBmpCategories", data.bmpCategories, 32, false);
    emitArray(
        out,
        "uint32_t",
        "kJavaWordSupplementaryCategories",
        data.supplementary,
        8,
        true);

    out.println(
        "constexpr std::array<JavaCasedRange, " + casedRanges.size()
            + "> kJavaCasedRanges = {{");
    for (CodePointRange range : casedRanges) {
      out.printf("    {0x%06x, 0x%06x},%n", range.first, range.last);
    }
    out.println("}};");
    out.println();
    out.println("// clang-format on");
  }

  private static void emitScalar(
      PrintWriter out, String type, String name, long value) {
    emitScalar(out, type, name, value, "");
  }

  private static void emitScalar(
      PrintWriter out, String type, String name, long value, String suffix) {
    out.println(
        "constexpr " + type + " " + name + " = " + value + suffix + ";");
  }

  private static void emitArray(
      PrintWriter out,
      String type,
      String name,
      int[] values,
      int valuesPerLine,
      boolean hex) {
    out.println();
    out.println(
        "constexpr std::array<" + type + ", " + values.length + "> " + name
            + " = {{");
    for (int i = 0; i < values.length; ++i) {
      if (i % valuesPerLine == 0) {
        out.print("    ");
      }
      out.print(hex ? String.format("0x%08xU", values[i]) : values[i]);
      boolean endOfLine =
          i % valuesPerLine == valuesPerLine - 1 || i + 1 == values.length;
      if (i + 1 != values.length) {
        out.print(endOfLine ? "," : ", ");
      }
      if (endOfLine) {
        out.println();
      }
    }
    out.println("}};");
  }

  private static void emitArray(
      PrintWriter out,
      String type,
      String name,
      byte[] values,
      int valuesPerLine,
      boolean hex) {
    int[] converted = new int[values.length];
    for (int i = 0; i < values.length; ++i) {
      converted[i] =
          type.equals("int8_t") ? values[i] : Byte.toUnsignedInt(values[i]);
    }
    emitArray(out, type, name, converted, valuesPerLine, hex);
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  private static String toHex(byte[] bytes) {
    StringBuilder result = new StringBuilder(bytes.length * 2);
    for (byte value : bytes) {
      result.append(String.format("%02x", Byte.toUnsignedInt(value)));
    }
    return result.toString();
  }

  private static String sha256(byte[] bytes) throws NoSuchAlgorithmException {
    return toHex(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static String casedRangesSha256(List<CodePointRange> ranges)
      throws NoSuchAlgorithmException {
    ByteBuffer buffer =
        ByteBuffer.allocate(ranges.size() * 2 * Integer.BYTES)
            .order(ByteOrder.BIG_ENDIAN);
    for (CodePointRange range : ranges) {
      buffer.putInt(range.first);
      buffer.putInt(range.last);
    }
    return sha256(buffer.array());
  }

  private static final class WordBreakData {
    final int[] forward;
    final int[] backward;
    final byte[] endStates;
    final byte[] lookaheadStates;
    final int[] bmpIndices;
    final byte[] bmpCategories;
    final int[] supplementary;
    final byte[] additional;
    final long checksum;

    WordBreakData(
        int[] forward,
        int[] backward,
        byte[] endStates,
        byte[] lookaheadStates,
        int[] bmpIndices,
        byte[] bmpCategories,
        int[] supplementary,
        byte[] additional,
        long checksum) {
      this.forward = forward;
      this.backward = backward;
      this.endStates = endStates;
      this.lookaheadStates = lookaheadStates;
      this.bmpIndices = bmpIndices;
      this.bmpCategories = bmpCategories;
      this.supplementary = supplementary;
      this.additional = additional;
      this.checksum = checksum;
    }
  }

  private static final class CodePointRange {
    final int first;
    final int last;

    CodePointRange(int first, int last) {
      this.first = first;
      this.last = last;
    }
  }
}
