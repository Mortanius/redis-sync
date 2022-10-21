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

		mock.ExpectWatch(MutexPrefix+"dummy", mux.getAuxQueueKey())
		mock.ExpectLLen(MutexPrefix + "dummy").SetVal(0)
		mock.ExpectLLen(mux.getAuxQueueKey()).SetVal(0)
		mock.ExpectLPush(mux.getAuxQueueKey(), ".*").SetVal(1)
		mock.ExpectExpire(mux.getAuxQueueKey(), lockTimeout).SetVal(true)
		require.NoError(t, mux.Lock())

		mock.ExpectWatch(MutexPrefix+"dummy", mux.getAuxQueueKey())
		mock.ExpectLLen(MutexPrefix + "dummy").SetVal(0)
		mock.ExpectLLen(mux.getAuxQueueKey()).SetVal(1)
		mock.ExpectBRPopLPush(MutexPrefix+"dummy", mux.getAuxQueueKey(), waitTimeout).RedisNil()
		require.Equal(t, redis.Nil, mux.Lock())

		mock.ExpectRPopLPush(mux.getAuxQueueKey(), MutexPrefix+"dummy").SetVal("")
		require.NoError(t, mux.Unlock())

		mock.ExpectWatch(MutexPrefix+"dummy", mux.getAuxQueueKey())
		mock.ExpectLLen(MutexPrefix + "dummy").SetVal(0)
		mock.ExpectLLen(mux.getAuxQueueKey()).SetVal(0)
		mock.ExpectLPush(mux.getAuxQueueKey(), ".*").SetVal(1)
		mock.ExpectExpire(MutexPrefix+"dummy", lockTimeout).SetVal(true)
		require.NoError(t, mux.Lock())
	}
}

func TestMutex_RealClient(t *testing.T) {
	cl := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_HOST") + ":" + os.Getenv("REDIS_PORT"),
		Password: os.Getenv("REDIS_PW"),
	})
	mux := NewMutex(context.Background(), cl, "dummy").WithLockTimeout(time.Second * 5)

	var count = 0

	var n = 64
	var wg sync.WaitGroup
	wg.Add(n)

	incrFn := func(i int) {
		defer wg.Done()
		defer mux.Unlock()

		lockingAt := time.Now()
		err := mux.Lock()
		if err != nil {
			panic(err)
		}
		t.Log(i, "acquired lock after", time.Since(lockingAt).Microseconds(), "microseconds")
		if count != 0 {
			time.Sleep(time.Millisecond * time.Duration(200+rand.Int31n(800)))
			return
		}
		count++
	}

	for i := 0; i < n; i++ {
		go incrFn(i)
	}
	wg.Wait()

	require.EqualValues(t, 1, count)
}
