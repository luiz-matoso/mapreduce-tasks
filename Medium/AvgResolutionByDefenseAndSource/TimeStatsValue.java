package intermediate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class TimeStatsValue implements Writable {
  private int time;
  private int count;

  public TimeStatsValue() {
  }

  public TimeStatsValue(int time, int count) {
    this.time = time;
    this.count = count;
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(time);
    out.writeInt(count);
  }

  public void readFields(DataInput in) throws IOException {
    time = in.readInt();
    count = in.readInt();
  }

  public int getTime() {
    return time;
  }

  public int getCount() {
    return count;
  }
}
