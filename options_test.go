package dynalock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewWriteOptions(t *testing.T) {
	assert := require.New(t)

	opts := NewWriteOptions()

	assert.Equal(&WriteOptions{ttl: DefaultLockTTL}, opts)
}

func TestNewWriteOptionsWithPreviousKV(t *testing.T) {
	assert := require.New(t)

	dt := time.Now().Add(60 * time.Second)

	kv := &KVPair{Expires: dt.Unix()}

	opts := NewWriteOptions(WriteWithPreviousKV(kv))

	expected := &WriteOptions{ttl: time.Until(dt), previous: kv}

	assert.Equal(expected.previous, opts.previous)
	assert.WithinDuration(time.Now().Add(expected.ttl), time.Now().Add(opts.ttl), 1*time.Second)
}

func TestLockOptions(t *testing.T) {
	assert := require.New(t)

	lockOptions := NewLockOptions(LockWithNoRenew(), LockWithNoTryPolling())

	assert.False(lockOptions.renewEnable)
	assert.False(lockOptions.tryLockPollingEnable)
}
