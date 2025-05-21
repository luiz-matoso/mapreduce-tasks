package intermediate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class IndustryStatsValue implements Writable {
    private int affectedUsers;
    private float financialLoss;
    private int count;

    public IndustryStatsValue() {}

    public IndustryStatsValue(int affectedUsers, float financialLoss, int count) {
        this.affectedUsers = affectedUsers;
        this.financialLoss = financialLoss;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(affectedUsers);
        out.writeFloat(financialLoss);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        affectedUsers = in.readInt();
        financialLoss = in.readFloat();
        count = in.readInt();
    }

    public int getAffectedUsers() { return affectedUsers; }
    public float getFinancialLoss() { return financialLoss; }
    public int getCount() { return count; }
}