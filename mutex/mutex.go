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

func (m *Mutex) Lock() (rerr error) {
	auxQueueKey := m.getAuxQueueKey()
	var hasLock bool
	if err := m.cl.Watch(m.ctx, func(tx *redis.Tx) error {
		mLen, err := tx.LLen(m.ctx, m.QueueKey).Uint64()
		if err != nil {
			return err
		}
		if mLen > 1 {
			return errors.New("mutex queue length should not be greater than 1")
		}
		if mLen == 0 {
			if _, err := tx.TxPipelined(m.ctx, func(pipe redis.Pipeliner) error {
				auxLen, err := pipe.LLen(m.ctx, auxQueueKey).Uint64()
				if err != nil {
					return err
				}
				if auxLen > 1 {
					return errors.New("mutex aux queue length should not be greater than 1")
				}

				if auxLen == 0 {
					hasLock = true
					if err := pipe.LPush(m.ctx, auxQueueKey, time.Now()).Err(); err != nil {
						return err
					}
					if m.lockTimeout != 0 {
						return pipe.Expire(m.ctx, auxQueueKey, m.lockTimeout).Err()
					}
					return nil
				}
				return nil
			}); err != nil {
				hasLock = false
				if err == RedisTxFailedErr {
					return nil
				}
				return err
			}
		}
		return nil
	}, m.QueueKey, auxQueueKey); err != nil {
		return err
	}
	if !hasLock {
		return m.cl.BRPopLPush(m.ctx, m.QueueKey, auxQueueKey, m.waitTimeout).Err()
	}
	return nil
}

func (m *Mutex) Unlock() error {
	return m.cl.RPopLPush(m.ctx, m.getAuxQueueKey(), m.QueueKey).Err()
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
