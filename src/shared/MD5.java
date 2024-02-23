package shared;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5 {
    public static BigInteger getHash(String s) {
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            return new BigInteger(1, m.digest(s.getBytes()));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }
}
