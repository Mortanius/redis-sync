package mutex

import (
	"context"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	"github.com/stretchr/testify/require"
)

func TestNonBlockingMutex_MockedClient(t *testing.T) {
	{
		cl, mock := redismock.NewClientMock()
		mock = mock.Regexp()

		timeout := time.Second * 5
		mux := NewNonBlockingMutex(context.Background(), cl, "dummy").WithLockTimeout(timeout)

		mock.ExpectWatch(NonBlockingMutexPrefix + "dummy")
		mock.ExpectGet(NonBlockingMutexPrefix + "dummy").RedisNil()
		mock.ExpectTxPipeline()
		mock.ExpectSet(NonBlockingMutexPrefix+"dummy", ".*", timeout).SetVal("OK")
		mock.ExpectTxPipelineExec()

		has, err := mux.TryLock()
		require.NoError(t, err)
		require.True(t, has)

		mock.ExpectWatch(NonBlockingMutexPrefix + "dummy")
		mock.ExpectGet(NonBlockingMutexPrefix + "dummy").SetVal(time.Now().Format(time.RFC3339Nano))

		has, err = mux.TryLock()
		require.NoError(t, err)
		require.False(t, has)

		mock.ExpectDel(NonBlockingMutexPrefix + "dummy").SetVal(1)

		require.NoError(t, mux.Unlock())

		mock.ExpectWatch(NonBlockingMutexPrefix + "dummy")
		mock.ExpectGet(NonBlockingMutexPrefix + "dummy").RedisNil()
		mock.ExpectTxPipeline()
		mock.ExpectSet(NonBlockingMutexPrefix+"dummy", ".*", timeout).SetVal("OK")
		mock.ExpectTxPipelineExec()

		has, err = mux.TryLock()
		require.NoError(t, err)
		require.True(t, has)

		require.NoError(t, mock.ExpectationsWereMet())
	}
}

func TestNonBlockingMutex_RealClient(t *testing.T) {
	cl := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_HOST") + ":" + os.Getenv("REDIS_PORT"),
		Password: os.Getenv("REDIS_PW"),
	})
	mux := NewNonBlockingMutex(context.Background(), cl, "dummy").WithLockTimeout(time.Second * 5)

	var count = 0

	var n = 64
	var wg sync.WaitGroup
	wg.Add(n)

	incrFn := func(i int) {
		defer wg.Done()
		ok, err := mux.TryLock()
		if err != nil {
			panic(err)
		}
		t.Log(i, ok)
		if !ok {
			return
		}
		time.Sleep(time.Millisecond * time.Duration(200+rand.Int31n(800)))
		count++
	}

	for i := 0; i < n; i++ {
		go incrFn(i)
	}

	wg.Wait()

	t.Log("unlocking mux")

	require.NoError(t, mux.Unlock())

	require.EqualValues(t, 1, count)
}

func TestMutex_MockedClient(t *testing.T) {
	{
		cl, mock := redismock.NewClientMock()
		mock = mock.Regexp()

		lockTimeout := time.Second * 10
		waitTimeout := time.Second * 3
		mux := NewMutex(context.Background(), cl, "dummy").WithLockTimeout(lockTimeout).WithWaitTimeout(waitTimeout)

		// Successful lock
		mock.ExpectWatch(MutexPrefix+"dummy", mux.getAuxQueueKey())
		mock.ExpectLLen(MutexPrefix + "dummy").SetVal(0)
		mock.ExpectLLen(mux.getAuxQueueKey()).SetVal(0)
		mock.ExpectTxPipeline()
		mock.ExpectLPush(MutexPrefix+"dummy", ".*").SetVal(1)
		mock.ExpectExpire(MutexPrefix+"dummy", lockTimeout).SetVal(true)
		mock.ExpectExpire(mux.getAuxQueueKey(), lockTimeout).SetVal(true)
		mock.ExpectTxPipelineExec()
		require.NoError(t, mux.Lock())

		// Failed Tx
		mock.ExpectWatch(MutexPrefix+"dummy", mux.getAuxQueueKey())
		mock.ExpectLLen(MutexPrefix + "dummy").SetVal(0)
		mock.ExpectLLen(mux.getAuxQueueKey()).SetVal(0)
		mock.ExpectTxPipeline()
		mock.ExpectLPush(MutexPrefix+"dummy", ".*").SetVal(1)
		mock.ExpectExpire(MutexPrefix+"dummy", lockTimeout).SetVal(true)
		mock.ExpectExpire(mux.getAuxQueueKey(), lockTimeout).SetVal(true)
		mock.ExpectTxPipelineExec().SetErr(redis.TxFailedErr)
		mock.ExpectWatch(MutexPrefix+"dummy", mux.getAuxQueueKey())
		mock.ExpectBLPop(waitTimeout, mux.getAuxQueueKey()).SetVal([]string{""})
		mock.ExpectLLen(MutexPrefix + "dummy").SetVal(0)
		mock.ExpectLLen(mux.getAuxQueueKey()).SetVal(0)
		mock.ExpectTxPipeline()
		mock.ExpectLPush(MutexPrefix+"dummy", ".*").SetVal(1)
		mock.ExpectExpire(MutexPrefix+"dummy", lockTimeout).SetVal(true)
		mock.ExpectExpire(mux.getAuxQueueKey(), lockTimeout).SetVal(true)
		mock.ExpectTxPipelineExec()
		require.NoError(t, mux.Lock())

		// read mux queue is 1
		mock.ExpectWatch(MutexPrefix+"dummy", mux.getAuxQueueKey())
		mock.ExpectLLen(MutexPrefix + "dummy").SetVal(1)
		mock.ExpectWatch(MutexPrefix+"dummy", mux.getAuxQueueKey())
		mock.ExpectBLPop(waitTimeout, mux.getAuxQueueKey()).SetVal([]string{""})
		mock.ExpectLLen(MutexPrefix + "dummy").SetVal(0)
		mock.ExpectLLen(mux.getAuxQueueKey()).SetVal(0)
		mock.ExpectTxPipeline()
		mock.ExpectLPush(MutexPrefix+"dummy", ".*").SetVal(1)
		mock.ExpectExpire(MutexPrefix+"dummy", lockTimeout).SetVal(true)
		mock.ExpectExpire(mux.getAuxQueueKey(), lockTimeout).SetVal(true)
		mock.ExpectTxPipelineExec()
		require.NoError(t, mux.Lock())

		// read aux queue 1
		mock.ExpectWatch(MutexPrefix+"dummy", mux.getAuxQueueKey())
		mock.ExpectLLen(MutexPrefix + "dummy").SetVal(0)
		mock.ExpectLLen(mux.getAuxQueueKey()).SetVal(1)
		mock.ExpectWatch(MutexPrefix+"dummy", mux.getAuxQueueKey())
		mock.ExpectBLPop(waitTimeout, mux.getAuxQueueKey()).SetVal([]string{""})
		mock.ExpectLLen(MutexPrefix + "dummy").SetVal(0)
		mock.ExpectLLen(mux.getAuxQueueKey()).SetVal(0)
		mock.ExpectTxPipeline()
		mock.ExpectLPush(MutexPrefix+"dummy", ".*").SetVal(1)
		mock.ExpectExpire(MutexPrefix+"dummy", lockTimeout).SetVal(true)
		mock.ExpectExpire(mux.getAuxQueueKey(), lockTimeout).SetVal(true)
		mock.ExpectTxPipelineExec()
		require.NoError(t, mux.Lock())

		// successful unlock
		mock.ExpectRPopLPush(MutexPrefix+"dummy", mux.getAuxQueueKey()).SetVal("")
		require.NoError(t, mux.Unlock())
	}
}

func TestMutex_RealClient(t *testing.T) {
	cl := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_HOST") + ":" + os.Getenv("REDIS_PORT"),
		Password: os.Getenv("REDIS_PW"),
	})

	incrFn := func(wg *sync.WaitGroup, i int, count *int) {
		defer wg.Done()

		mux := NewMutex(context.Background(), cl, "dummy").WithLockTimeout(time.Minute).WithWaitTimeout(time.Second * 5)
		defer func() {
			t.Log(i, "releasing lock")
			if err := mux.Unlock(); err != nil {
				t.Fatal("unlock failed: " + err.Error())
			}
		}()

		lockingAt := time.Now()
		err := mux.Lock()
		if err != nil {
			panic(err)
		}
		t.Log(i, "acquired lock after", time.Since(lockingAt).Microseconds(), "microseconds")
		if *count != 0 {
			time.Sleep(time.Millisecond * time.Duration(20+rand.Int31n(480)))
			return
		}
		*count++
	}

	testFn := func(n, runs int) {
		for r := 0; r < runs; r++ {
			var count = 0
			var wg sync.WaitGroup
			wg.Add(n)
			for i := 0; i < n; i++ {
				go incrFn(&wg, i, &count)
			}
			wg.Wait()
			require.EqualValues(t, 1, count)
		}
	}

	testFn(1, 2)
	testFn(2, 2)
	testFn(4, 2)
	testFn(8, 2)
	testFn(64, 2)
}
