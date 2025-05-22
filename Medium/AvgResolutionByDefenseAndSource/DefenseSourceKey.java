package intermediate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DefenseSourceKey implements WritableComparable<DefenseSourceKey> {
  private String defense;
  private String source;

  public DefenseSourceKey() {
  }

  public DefenseSourceKey(String defense, String source) {
    this.defense = defense;
    this.source = source;
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(defense);
    out.writeUTF(source);
  }

  public void readFields(DataInput in) throws IOException {
    defense = in.readUTF();
    source = in.readUTF();
  }

  public int compareTo(DefenseSourceKey o) {
    int cmp = defense.compareTo(o.defense);
    if (cmp != 0)
      return cmp;
    return source.compareTo(o.source);
  }

  public String toString() {
    return defense + " - " + source;
  }

  @Override
  public int hashCode() {
    return defense.hashCode() * 163 + source.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof DefenseSourceKey) {
      DefenseSourceKey other = (DefenseSourceKey) o;
      return defense.equals(other.defense) && source.equals(other.source);
    }
    return false;
  }
}
