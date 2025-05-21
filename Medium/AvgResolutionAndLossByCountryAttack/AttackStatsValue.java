package intermediate;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AttackStatsValue implements Writable {
    private float financialLoss;
    private int resolutionTime;
    private int count;

    public AttackStatsValue() {}

    public AttackStatsValue(float financialLoss, int resolutionTime, int count) {
        this.financialLoss = financialLoss;
        this.resolutionTime = resolutionTime;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(financialLoss);
        out.writeInt(resolutionTime);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        financialLoss = in.readFloat();
        resolutionTime = in.readInt();
        count = in.readInt();
    }

    public float getFinancialLoss() { return financialLoss; }
    public int getResolutionTime() { return resolutionTime; }
    public int getCount() { return count; }
}
