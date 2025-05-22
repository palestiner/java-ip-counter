package dev.palestiner;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class IpCounter {

    private static final int CHUNK_SIZE = 256 * 1024 * 1024; // 256MB
    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors();
    private static final BitSet[] ipSets = new BitSet[NUM_THREADS];

    public static void main(String[] args) throws Exception {
        Path ipFile = Paths.get("/Users/palestiner/Downloads/ip_addresses");
        long start = System.currentTimeMillis();

        // Инициализация BitSet для каждого потока
        for (int i = 0; i < NUM_THREADS; i++) {
            ipSets[i] = new BitSet();
        }

        try (FileChannel channel = FileChannel.open(ipFile, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            try (ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS)) {

                // Разделяем файл на части для параллельной обработки
                for (long pos = 0; pos < fileSize; pos += CHUNK_SIZE) {
                    final long chunkStart = pos;
                    final long chunkEnd = Math.min(pos + CHUNK_SIZE, fileSize);

                    executor.submit(() -> processChunk(ipFile, chunkStart, chunkEnd));
                }

                executor.shutdown();
                executor.awaitTermination(1, TimeUnit.DAYS);

                // Объединяем результаты
                BitSet finalSet = new BitSet();
                for (BitSet set : ipSets) {
                    finalSet.or(set);
                }
                System.out.println("Unique IPs: " + finalSet.cardinality());
                System.out.println("Time: " + (System.currentTimeMillis() - start) / 1000 + "s");
            }

        }
    }

    private static void processChunk(Path file, long start, long end) {
        try (Arena arena = Arena.ofConfined();
             FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {

            MemorySegment segment = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    start,
                    end - start,
                    arena
            );

            BitSet localSet = ipSets[ThreadLocalRandom.current().nextInt(NUM_THREADS)];
            byte[] ipBuffer = new byte[15];
            int bufferPos = 0;

            for (long i = 0; i < segment.byteSize(); i++) {
                byte b = segment.get(ValueLayout.JAVA_BYTE, i);

                if (b == '\n') {
                    if (bufferPos > 0) {
                        int ip = shortIp(ipBuffer, bufferPos);
                        localSet.set(ip & 0x7FFFFFFF);
                        bufferPos = 0;
                    }
                } else if (bufferPos < ipBuffer.length) {
                    ipBuffer[bufferPos++] = b;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int shortIp(byte[] bytes, int length) {
        int ip = 0, octet = 0, shift = 24;
        for (int i = 0; i < length; i++) {
            byte b = bytes[i];
            if (b == '.') {
                ip |= octet << shift;
                shift -= 8;
                octet = 0;
            } else {
                octet = octet * 10 + (b - '0');
            }
        }
        return ip | octet;
    }

}
