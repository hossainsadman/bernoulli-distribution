package shared.messages;

import java.io.ObjectInputFilter.Status;
import java.nio.ByteBuffer;

public class BasicKVMessage implements KVMessage {
  private static final char LINE_FEED = 0x0A;
  private static final char RETURN = 0x0D;

  private StatusType status;
  private String key;
  private String value;

  String msg;
  byte[] msgBytes;

  public BasicKVMessage(StatusType status, String key, String value) {
    this.status = status;
    this.key = key;
    this.value = value;

    this.msg = this.status.toString();
    if (this.key != null)
      this.msg += " " + this.key;
    if (this.value != null)
      this.msg += " " + this.value;
    this.msg.trim();

    this.msgBytes = toByteArray(status, key, value);
  }

  public BasicKVMessage(byte[] bytes) {
    this.msgBytes = addCtrChars(bytes);
    this.parseBytes(msgBytes);
  }

  public byte[] getMsgBytes() {
    return this.msgBytes;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
      return value;
  }

  public void changeKey(String key) {
    this.key = key;
  }

  public StatusType getStatus() {
    return status;
  }

  private void parseBytes(byte[] bytes) {
    try {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        // Extract StatusType (20 bytes)
        byte[] statusBytes = new byte[20];
        buffer.get(statusBytes);
        String statusStr = new String(statusBytes).trim();
        this.status = StatusType.valueOf(statusStr);

        // Extract key length and key
        int keyLength = buffer.getInt();
        if (keyLength < 0 || keyLength > buffer.remaining()) {
            throw new IllegalArgumentException("Invalid key length");
        }
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        this.key = new String(keyBytes);

        int valueLength = buffer.getInt();
        value = null;
        if (valueLength > 0) {
            byte[] valueBytes = new byte[valueLength];
            buffer.get(valueBytes);
            value = new String(valueBytes);
        }

    } catch (Exception e) {
      // Handle the error as needed
      this.status = StatusType.FAILED;
      this.key = "Message format is unknown";
      this.value = null;
    }
  }

  
  private byte[] addCtrChars(byte[] bytes) {
    byte[] ctrBytes = new byte[] { LINE_FEED, RETURN };
    byte[] tmp = new byte[bytes.length + ctrBytes.length];

    System.arraycopy(bytes, 0, tmp, 0, bytes.length);
    System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

    return tmp;
  }

  private byte[] toByteArray(StatusType status, String key, String value) {
    byte[] statusBytes = String.format("%-20s", status.name()).getBytes();

    byte[] keyBytes = key.getBytes();
    int keyLength = keyBytes.length;

    // Convert value to bytes
    byte[] valueBytes = (value != null) ? value.getBytes() : new byte[0];
    int valueLength = valueBytes.length;

    int totalLength = statusBytes.length + 4 + keyLength + 4 + valueLength;

    ByteBuffer buffer = ByteBuffer.allocate(totalLength);

    // Append StatusType bytes
    buffer.put(statusBytes);

    // Append key length and key bytes
    buffer.putInt(keyLength);
    buffer.put(keyBytes);

    // Append value length and value bytes
    if (value != null) {
      buffer.putInt(valueLength);
      buffer.put(valueBytes);
    } else {
      // If value is null, append 0 for length
      buffer.putInt(0);
    }

    return addCtrChars(buffer.array());
  }
}
