import java.io.Serializable;
import java.math.BigInteger;

public class MetaData implements Serializable {
    private String host;
    private int port;
    private BigInteger hashStartRange;
    private BigInteger hashEndRange;

    public MetaData(String host, int port, String hashStartRange, String hashEndRange) {
        this.host = host;
        this.port = port;
        this.hashStartRange = new BigInteger(hashStartRange);
        this.hashEndRange = new BigInteger(hashEndRange);
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public String[] getNodeHashRange() {
        String[] hashRange = new String[2];
        hashRange[0] = hashStartRange.toString();
        hashRange[1] = hashEndRange.toString();

        return hashRange;
    }
}
