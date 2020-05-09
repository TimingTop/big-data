package com.natural.data.analyze.spark.user.visit.session;

import com.natural.data.analyze.spark.user.visit.constant.Constants;
import com.natural.data.analyze.spark.user.visit.util.StringUtils;
import org.apache.spark.util.AccumulatorV2;

import java.util.Objects;

public class SessionAggrStatAccumulator extends AccumulatorV2<String, String> {

    String zeroValue = Constants.SESSION_COUNT + "=0|"
            + Constants.TIME_PERIOD_1s_3s + "=0|"
            + Constants.TIME_PERIOD_4s_6s + "=0|"
            + Constants.TIME_PERIOD_7s_9s + "=0|"
            + Constants.TIME_PERIOD_10s_30s + "=0|"
            + Constants.TIME_PERIOD_30s_60s + "=0|"
            + Constants.TIME_PERIOD_1m_3m + "=0|"
            + Constants.TIME_PERIOD_3m_10m + "=0|"
            + Constants.TIME_PERIOD_10m_30m + "=0|"
            + Constants.TIME_PERIOD_30m + "=0|"
            + Constants.STEP_PERIOD_1_3 + "=0|"
            + Constants.STEP_PERIOD_4_6 + "=0|"
            + Constants.STEP_PERIOD_7_9 + "=0|"
            + Constants.STEP_PERIOD_10_30 + "=0|"
            + Constants.STEP_PERIOD_30_60 + "=0|"
            + Constants.STEP_PERIOD_60 + "=0";

    String value = Constants.SESSION_COUNT + "=0|"
            + Constants.TIME_PERIOD_1s_3s + "=0|"
            + Constants.TIME_PERIOD_4s_6s + "=0|"
            + Constants.TIME_PERIOD_7s_9s + "=0|"
            + Constants.TIME_PERIOD_10s_30s + "=0|"
            + Constants.TIME_PERIOD_30s_60s + "=0|"
            + Constants.TIME_PERIOD_1m_3m + "=0|"
            + Constants.TIME_PERIOD_3m_10m + "=0|"
            + Constants.TIME_PERIOD_10m_30m + "=0|"
            + Constants.TIME_PERIOD_30m + "=0|"
            + Constants.STEP_PERIOD_1_3 + "=0|"
            + Constants.STEP_PERIOD_4_6 + "=0|"
            + Constants.STEP_PERIOD_7_9 + "=0|"
            + Constants.STEP_PERIOD_10_30 + "=0|"
            + Constants.STEP_PERIOD_30_60 + "=0|"
            + Constants.STEP_PERIOD_60 + "=0";

    public SessionAggrStatAccumulator() {
        value = Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    @Override
    public void reset() {
        value = Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    @Override
    public void add(String v) {
        if (Objects.isNull(v) || v.equals("")) {
            return;
        }
        String oldValue = StringUtils.getFieldFromConcatString(value, "\\|", v);
        if (oldValue != null) {
            int newValue = Integer.valueOf(oldValue) + 1;

            value = StringUtils.setFieldInConcatString(value, "\\|", v, String.valueOf(newValue));
        }
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public boolean isZero() {
        return zeroValue.equals(value);
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        SessionAggrStatAccumulator newAcc = new SessionAggrStatAccumulator();
        newAcc.value = this.value;
        return newAcc;
    }

    @Override
    public void merge(AccumulatorV2<String, String> other) {
        if (Objects.isNull(value)) {
            this.value = other.value();
        } else if (Objects.isNull(other)) {
            return;
        } else {
            merge(Constants.SESSION_COUNT, other.value());
            merge(Constants.TIME_PERIOD_1s_3s, other.value());
            merge(Constants.TIME_PERIOD_4s_6s, other.value());
            merge(Constants.TIME_PERIOD_7s_9s, other.value());
            merge(Constants.TIME_PERIOD_10s_30s, other.value());
            merge(Constants.TIME_PERIOD_1m_3m, other.value());
            merge(Constants.TIME_PERIOD_3m_10m, other.value());
            merge(Constants.TIME_PERIOD_10m_30m, other.value());
            merge(Constants.TIME_PERIOD_30m, other.value());
            merge(Constants.STEP_PERIOD_1_3, other.value());
            merge(Constants.STEP_PERIOD_4_6, other.value());
            merge(Constants.STEP_PERIOD_7_9, other.value());
            merge(Constants.STEP_PERIOD_10_30, other.value());
            merge(Constants.STEP_PERIOD_30_60, other.value());
            merge(Constants.STEP_PERIOD_60, other.value());
        }
    }

    private void merge(String index, String otherValue) {
        String num = StringUtils.getFieldFromConcatString(otherValue, "\\|", index);
        if (!Objects.isNull(num)) {
            int number = Integer.valueOf(num);

            merge(index, number);
        }
    }

    private void merge(String index, int num) {
        if (Objects.isNull(index)) {
            return;
        }
        String oldValue = StringUtils.getFieldFromConcatString(value, "\\|", index);
        if (oldValue != null) {
            int newValue = Integer.valueOf(oldValue) + num;

            this.value = StringUtils.setFieldInConcatString(value, "\\|", index, String.valueOf(newValue));
        }
    }
}
