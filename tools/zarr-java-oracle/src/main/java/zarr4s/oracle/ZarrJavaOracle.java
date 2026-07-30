package zarr4s.oracle;

import dev.zarr.zarrjava.core.Array;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import ucar.ma2.IndexIterator;

/** Independent, tool-scoped zarr-java interoperability oracle. */
public final class ZarrJavaOracle {
    private static final long[] PYTHON_SHARD_SHAPE = new long[]{4, 4};

    private ZarrJavaOracle() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException(
                "expected <verify-python-shard|write-fixture> <path>"
            );
        }
        Path target = Paths.get(arguments[1]);
        switch (arguments[0]) {
            case "verify-python-shard":
                verifyPythonShard(target);
                break;
            case "write-fixture":
                writeFixture(target);
                break;
            default:
                throw new IllegalArgumentException("unknown oracle mode: " + arguments[0]);
        }
    }

    private static void verifyPythonShard(Path target) throws Exception {
        verify(
            "Zarr-Python start-indexed shard",
            target,
            PYTHON_SHARD_SHAPE,
            DataType.INT16,
            null,
            index -> index + 1.0
        );
        System.out.println("zarr-java 0.1.3 Python shard oracle: ok");
    }

    private static void writeFixture(Path target) throws Exception {
        dev.zarr.zarrjava.v3.Array array = dev.zarr.zarrjava.v3.Array.create(
            target,
            builder -> builder
                .withShape(2, 3)
                .withDataType(DataType.INT16)
                .withChunkShape(2, 3)
                .withDefaultChunkKeyEncoding()
                .withFillValue(0)
                .withCodecs(codecs -> codecs.withBytes(BytesCodec.Endian.LITTLE))
                .withDimensionNames("y", "x"),
            false
        );
        short[] source = new short[]{1, -2, 300, 4, 5, -6};
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.SHORT, new int[]{2, 3}, source));
        verify(
            "zarr-java fixture",
            target,
            new long[]{2, 3},
            DataType.INT16,
            new String[]{"y", "x"},
            index -> source[index]
        );
        System.out.println("zarr-java 0.1.3 fixture writer: ok");
    }

    private static void verify(
        String label,
        Path target,
        long[] expectedShape,
        DataType expectedDataType,
        String[] expectedAxes,
        ExpectedValue expectedValue
    ) throws Exception {
        Array array = Array.open(target);
        if (!Arrays.equals(array.metadata().shape, expectedShape)) {
            throw new AssertionError(
                label + ": expected shape " + Arrays.toString(expectedShape)
                    + ", found " + Arrays.toString(array.metadata().shape)
            );
        }
        if (array.metadata().dataType() != expectedDataType) {
            throw new AssertionError(
                label + ": expected data type " + expectedDataType
                    + ", found " + array.metadata().dataType()
            );
        }
        if (expectedAxes != null) {
            if (!(array.metadata() instanceof ArrayMetadata)) {
                throw new AssertionError(label + ": expected Zarr v3 metadata");
            }
            String[] actualAxes = ((ArrayMetadata) array.metadata()).dimensionNames;
            if (!Arrays.equals(actualAxes, expectedAxes)) {
                throw new AssertionError(
                    label + ": expected axes " + Arrays.toString(expectedAxes)
                        + ", found " + Arrays.toString(actualAxes)
                );
            }
        }

        IndexIterator values = array.read().getIndexIterator();
        int index = 0;
        while (values.hasNext()) {
            if (index >= expectedElementCount(expectedShape)) {
                throw new AssertionError(label + ": returned too many values");
            }
            double actual = values.getDoubleNext();
            double expected = expectedValue.at(index);
            if (Double.doubleToLongBits(actual) != Double.doubleToLongBits(expected)) {
                throw new AssertionError(
                    label + ": value " + index + " expected " + expected + ", found " + actual
                );
            }
            index += 1;
        }
        int expectedCount = expectedElementCount(expectedShape);
        if (index != expectedCount) {
            throw new AssertionError(
                label + ": expected " + expectedCount + " values, found " + index
            );
        }
    }

    private static int expectedElementCount(long[] shape) {
        long count = 1L;
        for (long size : shape) {
            count = Math.multiplyExact(count, size);
        }
        return Math.toIntExact(count);
    }

    @FunctionalInterface
    private interface ExpectedValue {
        double at(int index);
    }
}
