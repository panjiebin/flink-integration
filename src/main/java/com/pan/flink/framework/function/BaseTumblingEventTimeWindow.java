package com.pan.flink.framework.function;

import com.pan.flink.framework.Constants;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * base tumbling event time window
 *
 * @param <K> key
 * @param <I> input
 * @param <O> output
 * @param <S> state type
 * @author panjb
 * @since 0.0.1
 */
public abstract class BaseTumblingEventTimeWindow<K, I, O, S> extends KeyedProcessFunction<K, I, O> {

    private static final long serialVersionUID = 197289599494145905L;
    private transient MapState<Long, S> allWindowStates;
    protected final long durationMsec;

    public BaseTumblingEventTimeWindow(Time duration) {
        this.durationMsec = duration.toMilliseconds();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<Long, S> mapStateDescriptor = new MapStateDescriptor<>("state",
                TypeInformation.of(Long.class), getValueTypeInfo());
        if (this.enableTtl()) {
            ParameterTool globalParams = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            int ttl = globalParams.getInt(Constants.CONF_STATE_TTL, 300);
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(org.apache.flink.api.common.time.Time.minutes(ttl))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .cleanupFullSnapshot()
                    .build();
            mapStateDescriptor.enableTimeToLive(ttlConfig);
        }
        this.allWindowStates = getRuntimeContext().getMapState(mapStateDescriptor);
        this.init(parameters);
    }

    @Override
    public final void processElement(I in, Context context, Collector<O> out) throws Exception {
        Long eventTime = getEventTime(in);
        TimerService timerService = context.timerService();
        if (eventTime <= timerService.currentWatermark()) {
            // This event is late; its window has already been triggered.
            this.doSideOutput(in, context);
        } else {
            long endOfWindow = calEndOfWindow(eventTime, this.durationMsec);
            timerService.registerEventTimeTimer(endOfWindow);
            S state = allWindowStates.get(endOfWindow);
            if (state == null) {
                state = this.createState(in);
                allWindowStates.put(endOfWindow, state);
            }
            this.doProcessElement(in, state);
        }
    }

    @Override
    public final void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out) throws Exception {
        doProcessOnTimer(timestamp, ctx.getCurrentKey(), getWindow(timestamp), out);
        this.clearWindowState(allWindowStates, timestamp);
        postProcessOnTimer(timestamp, ctx, out);
    }

    protected long calEndOfWindow(long eventTime, long durationMsec) {
        return (eventTime - (eventTime % durationMsec) + durationMsec - 1);
    }

    protected S getWindow(long timestamp) throws Exception {
        return this.allWindowStates.get(timestamp);
    }

    /**
     * Called when the window fires.
     * <p>Gets the timestamp of the window to remove.
     * It can be used to control the number of windows reserved, the default is not reserved,
     * that is, when the window triggered immediately delete.
     *
     * @param allWindowStates all window state
     * @param currTimestamp current window timestamp
     */
    protected void clearWindowState(MapState<Long, S> allWindowStates, long currTimestamp) throws Exception {
        allWindowStates.remove(currTimestamp);
    }

    /**
     * get map state value TypeInformation
     * @return TypeInformation of map state value
     */
    protected abstract TypeInformation<S> getValueTypeInfo();

    /**
     * get event time from input value
     * @param in input
     * @return the event time of input value
     */
    protected abstract Long getEventTime(I in);

    /**
     * create instance of special state
     * @return state instance
     */
    protected abstract S createState(I in);

    /**
     * process input element
     * @param in input
     * @param state intermediate state
     */
    protected abstract void doProcessElement(I in, S state);

    /**
     * process when the timer triggered
     * @param currentKey current key
     * @param timestamp trigger timestamp
     * @param state intermediate state of each window
     * @param out output
     * @throws Exception exception
     */
    protected abstract void doProcessOnTimer(long timestamp, K currentKey, S state, Collector<O> out) throws Exception;

    /**
     * process later event
     * @param in input
     * @param context context
     */
    protected void doSideOutput(I in, Context context) throws Exception {
    }

    protected void init(Configuration parameters) {
    }

    protected boolean enableTtl() {
        return true;
    }

    protected void postProcessOnTimer(long timestamp, OnTimerContext ctx, Collector<O> out) throws Exception {
    }
}
