package ecs;

import java.math.BigInteger;

import shared.MD5;

public class ECSHashRange {
    private BigInteger start;
    private BigInteger end;

    public static final BigInteger RING_START = BigInteger.ZERO;
    public static final BigInteger RING_END = new BigInteger(String.valueOf('F').repeat(32), 16);

    public ECSHashRange(BigInteger start, BigInteger end) {
        this.start = start;
        this.end = end;
    }

    public static boolean isKeyInRange(String key, BigInteger start, BigInteger end) {
        BigInteger keyHash = MD5.getHash(key);

        // if start > end, then the range wraps around the hashring
        if (start.compareTo(end) >= 0) {
            // if the key is in the range of start to RING_END or RING_START to end
            if ((keyHash.compareTo(RING_START) >= 0) && (keyHash.compareTo(end) <= 0)
                    || (keyHash.compareTo(start) >= 0) && (keyHash.compareTo(RING_END) <= 0)) {
                return true;
            } else {
                return false;
            }

        } else {
            return keyHash.compareTo(start) >= 0 && keyHash.compareTo(end) <= 0;
        }
    }

    public boolean isKeyInRange(String key) {
        return isKeyInRange(key, this.start, this.end);
    }
}
