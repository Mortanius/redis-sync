package mutex

import (
	"context"
	"errors"
	"time"

	. "github.com/Mortanius/redis-sync/errors"
	"github.com/go-redis/redis/v8"
)

type Locker interface {
	Lock() error
	Unlock() error
}

var MutexPrefix = "mutex/"

type Mutex struct {
	ctx         context.Context
	cl          *redis.Client
	QueueKey    string
	lockTimeout time.Duration
	waitTimeout time.Duration
}

func NewMutex(ctx context.Context, client *redis.Client, keySuffix string) *Mutex {
	return &Mutex{
		ctx:      ctx,
		cl:       client,
		QueueKey: MutexPrefix + keySuffix,
	}
}

func (m *Mutex) WithLockTimeout(d time.Duration) *Mutex {
	m.lockTimeout = d
	return m
}

func (m *Mutex) WithWaitTimeout(d time.Duration) *Mutex {
	m.waitTimeout = d
	return m
}

func (m *Mutex) getAuxQueueKey() string {
	return m.QueueKey + "-aux"
}

func (m *Mutex) tryLock(tx *redis.Tx) (hasLock bool, rerr error) {
	qLen, rerr := tx.LLen(m.ctx, m.QueueKey).Uint64()
	if rerr != nil {
		return
	}
	if qLen > 1 {
		rerr = errors.New("mutex queue length should not be greater than 1")
		return
	}
	if qLen != 0 {
		return
	}

	auxQLen, rerr := tx.LLen(m.ctx, m.getAuxQueueKey()).Uint64()
	if rerr != nil {
		return
	}
	if auxQLen > 1 {
		rerr = errors.New("mutex aux queue length should not be greater than 1")
		return
	}
	if auxQLen != 0 {
		return
	}

	hasLock = true

	if _, err := tx.TxPipelined(m.ctx, func(pipe redis.Pipeliner) error {
		if err := pipe.LPush(m.ctx, m.QueueKey, time.Now()).Err(); err != nil {
			return err
		}
		if m.lockTimeout != 0 {
			if err := pipe.Expire(m.ctx, m.QueueKey, m.lockTimeout).Err(); err != nil {
				return err
			}
			return pipe.Expire(m.ctx, m.getAuxQueueKey(), m.lockTimeout).Err()
		}
		return nil
	}); err != nil {
		hasLock = false
		if err == RedisTxFailedErr {
			return
		}
		rerr = err
		return
	}
	return
}

func (m *Mutex) Lock() (rerr error) {
	auxQueueKey := m.getAuxQueueKey()
	var hasLock bool
	for !hasLock && rerr == nil {
		if err := m.cl.Watch(m.ctx, func(tx *redis.Tx) (err error) {
			hasLock, err = m.tryLock(tx)
			return
		}, m.QueueKey, auxQueueKey); err != nil {
			return err
		}
		if hasLock {
			return nil
		}
		rerr = m.cl.Watch(m.ctx, func(tx *redis.Tx) (rerr error) {
			err := tx.BLPop(m.ctx, m.waitTimeout, auxQueueKey).Err()
			if err != nil && err != redis.Nil {
				rerr = err
				return
			}
			hasLock, rerr = m.tryLock(tx)
			return
		}, m.QueueKey, auxQueueKey)
	}
	return
}

func (m *Mutex) Unlock() error {
	auxQueueKey := m.getAuxQueueKey()
	res := m.cl.RPopLPush(m.ctx, m.QueueKey, auxQueueKey)
	if err := res.Err(); err != nil {
		return err
	}
	return m.cl.Expire(m.ctx, auxQueueKey, m.waitTimeout).Err()
}

type NonBlockingLocker interface {
	TryLock() (bool, error)
	Unlock() error
}

var NonBlockingMutexPrefix = "non-blocking-mutex/"

type NonBlockingMutex struct {
	ctx         context.Context
	cl          *redis.Client
	Key         string
	lockTimeout time.Duration
}

func NewNonBlockingMutex(ctx context.Context, client *redis.Client, keySuffix string) *NonBlockingMutex {
	return &NonBlockingMutex{
		ctx: ctx,
		cl:  client,
		Key: NonBlockingMutexPrefix + keySuffix,
	}
}

func (m *NonBlockingMutex) WithLockTimeout(d time.Duration) *NonBlockingMutex {
	m.lockTimeout = d
	return m
}

func (m *NonBlockingMutex) TryLock() (bool, error) {
	var hasLock bool
	err := m.cl.Watch(m.ctx, func(tx *redis.Tx) error {
		if err := tx.Get(m.ctx, m.Key).Err(); err != redis.Nil {
			return err
		}
		// lock available
		if _, err := tx.TxPipelined(m.ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(m.ctx, m.Key, time.Now(), m.lockTimeout)
			return nil
		}); err != nil {
			return err
		}
		hasLock = true
		return nil
	}, m.Key)
	if err == RedisTxFailedErr {
		hasLock = false
		err = nil
	}
	return hasLock && err == nil, err
}

func (m *NonBlockingMutex) Unlock() error {
	return m.cl.Del(m.ctx, m.Key).Err()
}
