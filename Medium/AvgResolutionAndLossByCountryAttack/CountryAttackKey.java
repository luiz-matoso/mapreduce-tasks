package intermediate;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CountryAttackKey implements WritableComparable<CountryAttackKey> {
    private String country;
    private String attackType;

    public CountryAttackKey() {}

    public CountryAttackKey(String country, String attackType) {
        this.country = country;
        this.attackType = attackType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, country);
        WritableUtils.writeString(out, attackType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        country = WritableUtils.readString(in);
        attackType = WritableUtils.readString(in);
    }

    @Override
    public int compareTo(CountryAttackKey other) {
        int cmp = country.compareTo(other.country);
        if (cmp != 0) {
            return cmp;
        }
        return attackType.compareTo(other.attackType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CountryAttackKey)) return false;
        CountryAttackKey that = (CountryAttackKey) o;
        return Objects.equals(country, that.country) && Objects.equals(attackType, that.attackType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(country, attackType);
    }

    @Override
    public String toString() {
        return country + " - " + attackType;
    }
}
