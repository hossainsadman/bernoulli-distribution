package shared.messages;

public class BasicKVMessage implements KVMessage{
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

    this.msgBytes = toByteArray(this.msg);
  }
  
  public BasicKVMessage(byte[] bytes) {
    this.msgBytes = addCtrChars(bytes);
    this.parseBytes(bytes);
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

  public StatusType getStatus() {
    return status;
  }

  private void parseBytes(byte[] bytes) {
    // TODO: parse bytes to status, key and value
    this.status = null;
    this.key = null;
    this.value = null;
  }

  private byte[] addCtrChars(byte[] bytes) {
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;
	}

  private byte[] toByteArray(String s){
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}
}
