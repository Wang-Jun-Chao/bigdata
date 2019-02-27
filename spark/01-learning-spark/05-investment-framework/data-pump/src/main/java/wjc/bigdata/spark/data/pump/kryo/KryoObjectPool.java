package wjc.bigdata.spark.data.pump.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 22:40
 **/
public class KryoObjectPool implements Serializable {
    private final static KryoPool KRYO_POOL = new KryoPool
            .Builder(new KryoFactoryImpl())
            .queue(new LinkedBlockingQueue<>(200))
            .softReferences()
            .build();

    /**
     * Takes a {@link Kryo} instance from the pool or creates a new one (using the factory) if the pool is empty.
     */
    public static Kryo borrow() {
        return KRYO_POOL.borrow();
    }

    /**
     * TODO
     *
     * @param time
     * @param unit
     * @return
     */
    public static Kryo borrow(long time, TimeUnit unit) {
        return KRYO_POOL.borrow();
    }

    /**
     * Returns the given {@link Kryo} instance to the pool.
     */
    public static void release(Kryo kryo) {
        KRYO_POOL.release(kryo);
    }


    private static class KryoFactoryImpl implements KryoFactory, Serializable {
        @Override
        public Kryo create() {
            return new Kryo();
        }
    }
}
