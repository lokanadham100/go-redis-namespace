package go_redis_namespace

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

const namespaceSeperator = ":"

var _ redis.Cmdable = (*redisNamespace)(nil)
var _ redis.Pipeliner = (*redisNamespace)(nil)

type redisNamespace struct {
	namespace string
	redis.Cmdable
}

func NewRedisNamespace(namespace string, client redis.Cmdable) redis.Cmdable {
	return &redisNamespace{
		namespace: namespace,
		Cmdable:   client,
	}
}

func (rn *redisNamespace) Pipeline() redis.Pipeliner {
	pipeline := rn.Cmdable.Pipeline()
	if len(rn.namespace) == 0 {
		return pipeline
	}
	return &redisNamespace{
		rn.namespace,
		pipeline,
	}
}

func (rn *redisNamespace) Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	if len(rn.namespace) == 0 {
		return rn.Cmdable.Pipelined(ctx, fn)
	}
	newfn := func(pipeline redis.Pipeliner) error {
		return fn(&redisNamespace{
			rn.namespace,
			pipeline,
		})
	}
	return rn.Cmdable.Pipelined(ctx, newfn)
}

func (rn *redisNamespace) TxPipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	if len(rn.namespace) == 0 {
		return rn.Cmdable.TxPipelined(ctx, fn)
	}
	newfn := func(pipeline redis.Pipeliner) error {
		return fn(&redisNamespace{
			rn.namespace,
			pipeline,
		})
	}
	return rn.Cmdable.TxPipelined(ctx, newfn)
}

func (rn *redisNamespace) TxPipeline() redis.Pipeliner {
	pipeline := rn.Cmdable.TxPipeline()
	if len(rn.namespace) == 0 {
		return pipeline
	}
	return &redisNamespace{
		rn.namespace,
		pipeline,
	}
}

func (rn *redisNamespace) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return rn.Del(ctx, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) Unlink(ctx context.Context, keys ...string) *redis.IntCmd {
	return rn.Unlink(ctx, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) Dump(ctx context.Context, key string) *redis.StringCmd {
	return rn.Dump(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	return rn.Exists(ctx, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return rn.Expire(ctx, rn.appendNamespaceToKey(key), expiration)
}

func (rn *redisNamespace) ExpireAt(ctx context.Context, key string, tm time.Time) *redis.BoolCmd {
	return rn.ExpireAt(ctx, rn.appendNamespaceToKey(key), tm)
}

func (rn *redisNamespace) Keys(ctx context.Context, pattern string) *redis.StringSliceCmd {
	return rn.Keys(ctx, rn.appendNamespaceToKey(pattern))
}

func (rn *redisNamespace) Migrate(ctx context.Context, host, port, key string, db int, timeout time.Duration) *redis.StatusCmd {
	return rn.Migrate(ctx, host, port, rn.appendNamespaceToKey(key), db, timeout)
}

func (rn *redisNamespace) Move(ctx context.Context, key string, db int) *redis.BoolCmd {
	return rn.Move(ctx, rn.appendNamespaceToKey(key), db)
}

func (rn *redisNamespace) ObjectRefCount(ctx context.Context, key string) *redis.IntCmd {
	return rn.ObjectRefCount(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) ObjectEncoding(ctx context.Context, key string) *redis.StringCmd {
	return rn.ObjectEncoding(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) ObjectIdleTime(ctx context.Context, key string) *redis.DurationCmd {
	return rn.ObjectIdleTime(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) Persist(ctx context.Context, key string) *redis.BoolCmd {
	return rn.Persist(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) PExpire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return rn.PExpire(ctx, rn.appendNamespaceToKey(key), expiration)
}

func (rn *redisNamespace) PExpireAt(ctx context.Context, key string, tm time.Time) *redis.BoolCmd {
	return rn.PExpireAt(ctx, rn.appendNamespaceToKey(key), tm)
}

func (rn *redisNamespace) PTTL(ctx context.Context, key string) *redis.DurationCmd {
	return rn.PTTL(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) Rename(ctx context.Context, key, newkey string) *redis.StatusCmd {
	return rn.Rename(ctx, rn.appendNamespaceToKey(key), rn.appendNamespaceToKey(newkey))
}

func (rn *redisNamespace) RenameNX(ctx context.Context, key, newkey string) *redis.BoolCmd {
	return rn.RenameNX(ctx, rn.appendNamespaceToKey(key), rn.appendNamespaceToKey(newkey))
}

func (rn *redisNamespace) Restore(ctx context.Context, key string, ttl time.Duration, value string) *redis.StatusCmd {
	return rn.Restore(ctx, rn.appendNamespaceToKey(key), ttl, value)
}

func (rn *redisNamespace) RestoreReplace(ctx context.Context, key string, ttl time.Duration, value string) *redis.StatusCmd {
	return rn.RestoreReplace(ctx, rn.appendNamespaceToKey(key), ttl, value)
}

func (rn *redisNamespace) Sort(ctx context.Context, key string, sort *redis.Sort) *redis.StringSliceCmd {
	return rn.Sort(ctx, rn.appendNamespaceToKey(key), sort)
}

func (rn *redisNamespace) SortStore(ctx context.Context, key, store string, sort *redis.Sort) *redis.IntCmd {
	return rn.SortStore(ctx, rn.appendNamespaceToKey(key), store, sort)
}

func (rn *redisNamespace) SortInterfaces(ctx context.Context, key string, sort *redis.Sort) *redis.SliceCmd {
	return rn.SortInterfaces(ctx, rn.appendNamespaceToKey(key), sort)
}

func (rn *redisNamespace) Touch(ctx context.Context, keys ...string) *redis.IntCmd {
	return rn.Touch(ctx, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) TTL(ctx context.Context, key string) *redis.DurationCmd {
	return rn.TTL(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) Type(ctx context.Context, key string) *redis.StatusCmd {
	return rn.Type(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	return rn.Scan(ctx, cursor, rn.appendNamespaceToKey(match), count)
}

func (rn *redisNamespace) SScan(ctx context.Context, key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	return rn.SScan(ctx, rn.appendNamespaceToKey(key), cursor, match, count)
}

func (rn *redisNamespace) Append(ctx context.Context, key, value string) *redis.IntCmd {
	return rn.Append(ctx, rn.appendNamespaceToKey(key), value)
}

func (rn *redisNamespace) BitCount(ctx context.Context, key string, bitCount *redis.BitCount) *redis.IntCmd {
	return rn.BitCount(ctx, rn.appendNamespaceToKey(key), bitCount)
}

func (rn *redisNamespace) BitOpAnd(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	return rn.BitOpAnd(ctx, rn.appendNamespaceToKey(destKey), rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) BitOpOr(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	return rn.BitOpOr(ctx, rn.appendNamespaceToKey(destKey), rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) BitOpXor(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	return rn.BitOpXor(ctx, rn.appendNamespaceToKey(destKey), rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) BitOpNot(ctx context.Context, destKey string, key string) *redis.IntCmd {
	return rn.BitOpNot(ctx, rn.appendNamespaceToKey(destKey), rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) BitPos(ctx context.Context, key string, bit int64, pos ...int64) *redis.IntCmd {
	return rn.BitPos(ctx, rn.appendNamespaceToKey(key), bit, pos...)
}

func (rn *redisNamespace) BitField(ctx context.Context, key string, args ...interface{}) *redis.IntSliceCmd {
	return rn.BitField(ctx, rn.appendNamespaceToKey(key), args...)
}

func (rn *redisNamespace) Decr(ctx context.Context, key string) *redis.IntCmd {
	return rn.Decr(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) DecrBy(ctx context.Context, key string, decrement int64) *redis.IntCmd {
	return rn.DecrBy(ctx, rn.appendNamespaceToKey(key), decrement)
}

func (rn *redisNamespace) Get(ctx context.Context, key string) *redis.StringCmd {
	return rn.Get(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) GetBit(ctx context.Context, key string, offset int64) *redis.IntCmd {
	return rn.GetBit(ctx, rn.appendNamespaceToKey(key), offset)
}

func (rn *redisNamespace) GetRange(ctx context.Context, key string, start, end int64) *redis.StringCmd {
	return rn.GetRange(ctx, rn.appendNamespaceToKey(key), start, end)
}

func (rn *redisNamespace) GetSet(ctx context.Context, key string, value interface{}) *redis.StringCmd {
	return rn.GetSet(ctx, rn.appendNamespaceToKey(key), value)
}

func (rn *redisNamespace) Incr(ctx context.Context, key string) *redis.IntCmd {
	return rn.Incr(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	return rn.IncrBy(ctx, rn.appendNamespaceToKey(key), value)
}

func (rn *redisNamespace) IncrByFloat(ctx context.Context, key string, value float64) *redis.FloatCmd {
	return rn.IncrByFloat(ctx, rn.appendNamespaceToKey(key), value)
}

func (rn *redisNamespace) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	return rn.MGet(ctx, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) MSet(ctx context.Context, pairs ...interface{}) *redis.StatusCmd {
	return rn.MSet(ctx, rn.appendNamespaceToPairs(pairs)...)
}

func (rn *redisNamespace) MSetNX(ctx context.Context, pairs ...interface{}) *redis.BoolCmd {
	return rn.MSetNX(ctx, rn.appendNamespaceToPairs(pairs)...)
}

func (rn *redisNamespace) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return rn.Set(ctx, rn.appendNamespaceToKey(key), value, expiration)
}

func (rn *redisNamespace) SetEX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return rn.SetEX(ctx, rn.appendNamespaceToKey(key), value, expiration)
}

func (rn *redisNamespace) SetBit(ctx context.Context, key string, offset int64, value int) *redis.IntCmd {
	return rn.SetBit(ctx, rn.appendNamespaceToKey(key), offset, value)
}

func (rn *redisNamespace) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return rn.SetNX(ctx, rn.appendNamespaceToKey(key), value, expiration)
}

func (rn *redisNamespace) SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return rn.SetXX(ctx, rn.appendNamespaceToKey(key), value, expiration)
}

func (rn *redisNamespace) SetRange(ctx context.Context, key string, offset int64, value string) *redis.IntCmd {
	return rn.SetRange(ctx, rn.appendNamespaceToKey(key), offset, value)
}

func (rn *redisNamespace) StrLen(ctx context.Context, key string) *redis.IntCmd {
	return rn.StrLen(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) BLPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	return rn.BLPop(ctx, timeout, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) BRPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	return rn.BRPop(ctx, timeout, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) BRPopLPush(ctx context.Context, source, destination string, timeout time.Duration) *redis.StringCmd {
	return rn.BRPopLPush(ctx, rn.appendNamespaceToKey(source), rn.appendNamespaceToKey(destination), timeout)
}

func (rn *redisNamespace) LIndex(ctx context.Context, key string, index int64) *redis.StringCmd {
	return rn.LIndex(ctx, rn.appendNamespaceToKey(key), index)
}

func (rn *redisNamespace) LInsert(ctx context.Context, key, op string, pivot, value interface{}) *redis.IntCmd {
	return rn.LInsert(ctx, rn.appendNamespaceToKey(key), op, pivot, value)
}

func (rn *redisNamespace) LInsertBefore(ctx context.Context, key string, pivot, value interface{}) *redis.IntCmd {
	return rn.LInsertBefore(ctx, rn.appendNamespaceToKey(key), pivot, value)
}

func (rn *redisNamespace) LInsertAfter(ctx context.Context, key string, pivot, value interface{}) *redis.IntCmd {
	return rn.LInsertAfter(ctx, rn.appendNamespaceToKey(key), pivot, value)
}

func (rn *redisNamespace) LLen(ctx context.Context, key string) *redis.IntCmd {
	return rn.LLen(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) LPos(ctx context.Context, key string, value string, args redis.LPosArgs) *redis.IntCmd {
	return rn.LPos(ctx, rn.appendNamespaceToKey(key), value, args)
}

func (rn *redisNamespace) LPosCount(ctx context.Context, key string, value string, count int64, args redis.LPosArgs) *redis.IntSliceCmd {
	return rn.LPosCount(ctx, rn.appendNamespaceToKey(key), value, count, args)
}

func (rn *redisNamespace) LPop(ctx context.Context, key string) *redis.StringCmd {
	return rn.LPop(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return rn.LPush(ctx, rn.appendNamespaceToKey(key), values...)
}

func (rn *redisNamespace) LPushX(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return rn.LPushX(ctx, rn.appendNamespaceToKey(key), values...)
}

func (rn *redisNamespace) LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return rn.LRange(ctx, rn.appendNamespaceToKey(key), start, stop)
}

func (rn *redisNamespace) LRem(ctx context.Context, key string, count int64, value interface{}) *redis.IntCmd {
	return rn.LRem(ctx, rn.appendNamespaceToKey(key), count, value)
}

func (rn *redisNamespace) LSet(ctx context.Context, key string, index int64, value interface{}) *redis.StatusCmd {
	return rn.LSet(ctx, rn.appendNamespaceToKey(key), index, value)
}

func (rn *redisNamespace) LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd {
	return rn.LTrim(ctx, rn.appendNamespaceToKey(key), start, stop)
}

func (rn *redisNamespace) RPop(ctx context.Context, key string) *redis.StringCmd {
	return rn.RPop(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) RPopLPush(ctx context.Context, source, destination string) *redis.StringCmd {
	return rn.RPopLPush(ctx, rn.appendNamespaceToKey(source), rn.appendNamespaceToKey(destination))
}

func (rn *redisNamespace) RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return rn.RPush(ctx, rn.appendNamespaceToKey(key), values...)
}

func (rn *redisNamespace) RPushX(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return rn.RPushX(ctx, rn.appendNamespaceToKey(key), values...)
}

func (rn *redisNamespace) PFAdd(ctx context.Context, key string, els ...interface{}) *redis.IntCmd {
	return rn.PFAdd(ctx, rn.appendNamespaceToKey(key), els...)
}

func (rn *redisNamespace) PFCount(ctx context.Context, keys ...string) *redis.IntCmd {
	return rn.PFCount(ctx, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) PFMerge(ctx context.Context, dest string, keys ...string) *redis.StatusCmd {
	return rn.PFMerge(ctx, rn.appendNamespaceToKey(dest), rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return rn.Eval(ctx, script, rn.appendNamespaceToKeys(keys), args...)
}

func (rn *redisNamespace) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return rn.EvalSha(ctx, sha1, rn.appendNamespaceToKeys(keys), args...)
}

func (rn *redisNamespace) DebugObject(ctx context.Context, key string) *redis.StringCmd {
	return rn.DebugObject(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	return rn.Publish(ctx, rn.appendNamespaceToKey(channel), message)
}

func (rn *redisNamespace) PubSubChannels(ctx context.Context, pattern string) *redis.StringSliceCmd {
	return rn.PubSubChannels(ctx, rn.appendNamespaceToKey(pattern))
}

func (rn *redisNamespace) PubSubNumSub(ctx context.Context, channels ...string) *redis.StringIntMapCmd {
	return rn.PubSubNumSub(ctx, rn.appendNamespaceToKeys(channels)...)
}

func (rn *redisNamespace) ClusterKeySlot(ctx context.Context, key string) *redis.IntCmd {
	return rn.ClusterKeySlot(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) GeoAdd(ctx context.Context, key string, geoLocation ...*redis.GeoLocation) *redis.IntCmd {
	return rn.GeoAdd(ctx, rn.appendNamespaceToKey(key), geoLocation...)
}

func (rn *redisNamespace) GeoPos(ctx context.Context, key string, members ...string) *redis.GeoPosCmd {
	return rn.GeoPos(ctx, rn.appendNamespaceToKey(key), members...)
}

func (rn *redisNamespace) GeoRadius(ctx context.Context, key string, longitude, latitude float64, query *redis.GeoRadiusQuery) *redis.GeoLocationCmd {
	return rn.GeoRadius(ctx, rn.appendNamespaceToKey(key), longitude, latitude, query)
}

func (rn *redisNamespace) GeoRadiusStore(ctx context.Context, key string, longitude, latitude float64, query *redis.GeoRadiusQuery) *redis.IntCmd {
	return rn.GeoRadiusStore(ctx, rn.appendNamespaceToKey(key), longitude, latitude, query)
}

func (rn *redisNamespace) GeoRadiusByMember(ctx context.Context, key, member string, query *redis.GeoRadiusQuery) *redis.GeoLocationCmd {
	return rn.GeoRadiusByMember(ctx, rn.appendNamespaceToKey(key), member, query)
}

func (rn *redisNamespace) GeoRadiusByMemberStore(ctx context.Context, key, member string, query *redis.GeoRadiusQuery) *redis.IntCmd {
	return rn.GeoRadiusByMemberStore(ctx, rn.appendNamespaceToKey(key), member, query)
}

func (rn *redisNamespace) GeoDist(ctx context.Context, key string, member1, member2, unit string) *redis.FloatCmd {
	return rn.GeoDist(ctx, rn.appendNamespaceToKey(key), member1, member2, unit)
}

func (rn *redisNamespace) GeoHash(ctx context.Context, key string, members ...string) *redis.StringSliceCmd {
	return rn.GeoHash(ctx, rn.appendNamespaceToKey(key), members...)
}

func (rn *redisNamespace) ClientKillByFilter(ctx context.Context, keys ...string) *redis.IntCmd {
	return rn.ClientKillByFilter(ctx, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) MemoryUsage(ctx context.Context, key string, samples ...int) *redis.IntCmd {
	return rn.MemoryUsage(ctx, rn.appendNamespaceToKey(key), samples...)
}

func (rn *redisNamespace) ScanType(ctx context.Context, cursor uint64, match string, count int64, keyType string) *redis.ScanCmd {
	return rn.ScanType(ctx, cursor, rn.appendNamespaceToKey(match), count, keyType)
}

func (rn *redisNamespace) HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd {
	return rn.HDel(ctx, rn.appendNamespaceToKey(key), fields...)
}

func (rn *redisNamespace) HExists(ctx context.Context, key, field string) *redis.BoolCmd {
	return rn.HExists(ctx, rn.appendNamespaceToKey(key), field)
}

func (rn *redisNamespace) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	return rn.HGet(ctx, rn.appendNamespaceToKey(key), field)
}

func (rn *redisNamespace) HGetAll(ctx context.Context, key string) *redis.StringStringMapCmd {
	return rn.HGetAll(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) HIncrBy(ctx context.Context, key, field string, incr int64) *redis.IntCmd {
	return rn.HIncrBy(ctx, rn.appendNamespaceToKey(key), field, incr)
}

func (rn *redisNamespace) HIncrByFloat(ctx context.Context, key, field string, incr float64) *redis.FloatCmd {
	return rn.HIncrByFloat(ctx, rn.appendNamespaceToKey(key), field, incr)
}

func (rn *redisNamespace) HKeys(ctx context.Context, key string) *redis.StringSliceCmd {
	return rn.HKeys(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) HLen(ctx context.Context, key string) *redis.IntCmd {
	return rn.HLen(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) HMGet(ctx context.Context, key string, fields ...string) *redis.SliceCmd {
	return rn.HMGet(ctx, rn.appendNamespaceToKey(key), fields...)
}

func (rn *redisNamespace) HMSet(ctx context.Context, key string, fields ...interface{}) *redis.BoolCmd {
	return rn.HMSet(ctx, rn.appendNamespaceToKey(key), fields...)
}

func (rn *redisNamespace) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return rn.HSet(ctx, rn.appendNamespaceToKey(key), values...)
}

func (rn *redisNamespace) HSetNX(ctx context.Context, key, field string, value interface{}) *redis.BoolCmd {
	return rn.HSetNX(ctx, rn.appendNamespaceToKey(key), field, value)
}

func (rn *redisNamespace) HVals(ctx context.Context, key string) *redis.StringSliceCmd {
	return rn.HVals(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) HScan(ctx context.Context, key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	return rn.HScan(ctx, rn.appendNamespaceToKey(key), cursor, match, count)
}

func (rn *redisNamespace) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return rn.SAdd(ctx, rn.appendNamespaceToKey(key), members...)
}

func (rn *redisNamespace) SCard(ctx context.Context, key string) *redis.IntCmd {
	return rn.SCard(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) SDiff(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	return rn.SDiff(ctx, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) SDiffStore(ctx context.Context, destination string, keys ...string) *redis.IntCmd {
	return rn.SDiffStore(ctx, rn.appendNamespaceToKey(destination), rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) SInter(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	return rn.SInter(ctx, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) SInterStore(ctx context.Context, destination string, keys ...string) *redis.IntCmd {
	return rn.SInterStore(ctx, rn.appendNamespaceToKey(destination), rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd {
	return rn.SIsMember(ctx, rn.appendNamespaceToKey(key), member)
}

func (rn *redisNamespace) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	return rn.SMembers(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) SMembersMap(ctx context.Context, key string) *redis.StringStructMapCmd {
	return rn.SMembersMap(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) SMove(ctx context.Context, source, destination string, member interface{}) *redis.BoolCmd {
	return rn.SMove(ctx, rn.appendNamespaceToKey(source), rn.appendNamespaceToKey(destination), member)
}

func (rn *redisNamespace) SPop(ctx context.Context, key string) *redis.StringCmd {
	return rn.SPop(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) SPopN(ctx context.Context, key string, count int64) *redis.StringSliceCmd {
	return rn.SPopN(ctx, rn.appendNamespaceToKey(key), count)
}

func (rn *redisNamespace) SRandMember(ctx context.Context, key string) *redis.StringCmd {
	return rn.SRandMember(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) SRandMemberN(ctx context.Context, key string, count int64) *redis.StringSliceCmd {
	return rn.SRandMemberN(ctx, rn.appendNamespaceToKey(key), count)
}

func (rn *redisNamespace) SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return rn.SRem(ctx, rn.appendNamespaceToKey(key), members...)
}

func (rn *redisNamespace) SUnion(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	return rn.SUnion(ctx, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) SUnionStore(ctx context.Context, destination string, keys ...string) *redis.IntCmd {
	return rn.SUnionStore(ctx, rn.appendNamespaceToKey(destination), rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error {
	return rn.Watch(ctx, fn, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) XDel(ctx context.Context, stream string, ids ...string) *redis.IntCmd {
	return rn.XDel(ctx, rn.appendNamespaceToKey(stream), ids...)
}

func (rn *redisNamespace) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	return rn.XAdd(ctx, a)
}

func (rn *redisNamespace) XLen(ctx context.Context, stream string) *redis.IntCmd {
	return rn.XLen(ctx, rn.appendNamespaceToKey(stream))
}

func (rn *redisNamespace) XRange(ctx context.Context, stream, start, stop string) *redis.XMessageSliceCmd {
	return rn.XRange(ctx, rn.appendNamespaceToKey(stream), start, stop)
}

func (rn *redisNamespace) XRangeN(ctx context.Context, stream, start, stop string, count int64) *redis.XMessageSliceCmd {
	return rn.XRangeN(ctx, rn.appendNamespaceToKey(stream), start, stop, count)
}

func (rn *redisNamespace) XRead(ctx context.Context, a *redis.XReadArgs) *redis.XStreamSliceCmd {
	return rn.XRead(ctx, a)
}

func (rn *redisNamespace) XRevRange(ctx context.Context, stream string, start, stop string) *redis.XMessageSliceCmd {
	return rn.XRevRange(ctx, rn.appendNamespaceToKey(stream), start, stop)
}

func (rn *redisNamespace) XRevRangeN(ctx context.Context, stream string, start, stop string, count int64) *redis.XMessageSliceCmd {
	return rn.XRevRangeN(ctx, rn.appendNamespaceToKey(stream), start, stop, count)
}

func (rn *redisNamespace) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	return rn.XAck(ctx, rn.appendNamespaceToKey(stream), group, ids...)
}

func (rn *redisNamespace) XClaim(ctx context.Context, a *redis.XClaimArgs) *redis.XMessageSliceCmd {
	return rn.XClaim(ctx, a)
}

func (rn *redisNamespace) XClaimJustID(ctx context.Context, a *redis.XClaimArgs) *redis.StringSliceCmd {
	return rn.XClaimJustID(ctx, a)
}

func (rn *redisNamespace) XGroupCreate(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	return rn.XGroupCreate(ctx, rn.appendNamespaceToKey(stream), group, start)
}

func (rn *redisNamespace) XGroupDelConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd {
	return rn.XGroupDelConsumer(ctx, rn.appendNamespaceToKey(stream), group, consumer)
}

func (rn *redisNamespace) XGroupDestroy(ctx context.Context, stream, group string) *redis.IntCmd {
	return rn.XGroupDestroy(ctx, rn.appendNamespaceToKey(stream), group)
}

func (rn *redisNamespace) XGroupSetID(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	return rn.XGroupSetID(ctx, rn.appendNamespaceToKey(stream), group, start)
}

func (rn *redisNamespace) XPending(ctx context.Context, stream, group string) *redis.XPendingCmd {
	return rn.XPending(ctx, rn.appendNamespaceToKey(stream), group)
}

func (rn *redisNamespace) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	return rn.XPendingExt(ctx, a)
}

func (rn *redisNamespace) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	return rn.XReadGroup(ctx, a)
}

func (rn *redisNamespace) XReadStreams(ctx context.Context, streams ...string) *redis.XStreamSliceCmd {
	return rn.XReadStreams(ctx, rn.appendNamespaceToKeys(streams)...)
}

func (rn *redisNamespace) XTrim(ctx context.Context, key string, maxLen int64) *redis.IntCmd {
	return rn.XTrim(ctx, rn.appendNamespaceToKey(key), maxLen)
}

func (rn *redisNamespace) XTrimApprox(ctx context.Context, key string, maxLen int64) *redis.IntCmd {
	return rn.XTrimApprox(ctx, rn.appendNamespaceToKey(key), maxLen)
}

func (rn *redisNamespace) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	return rn.XGroupCreateMkStream(ctx, rn.appendNamespaceToKey(stream), group, start)
}

func (rn *redisNamespace) XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd {
	return rn.XInfoGroups(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) XInfoStream(ctx context.Context, key string) *redis.XInfoStreamCmd {
	return rn.XInfoStream(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) BZPopMax(ctx context.Context, timeout time.Duration, keys ...string) *redis.ZWithKeyCmd {
	return rn.BZPopMax(ctx, timeout, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) BZPopMin(ctx context.Context, timeout time.Duration, keys ...string) *redis.ZWithKeyCmd {
	return rn.BZPopMin(ctx, timeout, rn.appendNamespaceToKeys(keys)...)
}

func (rn *redisNamespace) ZPopMax(ctx context.Context, key string, count ...int64) *redis.ZSliceCmd {
	return rn.ZPopMax(ctx, rn.appendNamespaceToKey(key), count...)
}

func (rn *redisNamespace) ZPopMin(ctx context.Context, key string, count ...int64) *redis.ZSliceCmd {
	return rn.ZPopMin(ctx, rn.appendNamespaceToKey(key), count...)
}

func (rn *redisNamespace) ZScan(ctx context.Context, key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	return rn.ZScan(ctx, rn.appendNamespaceToKey(key), cursor, match, count)
}

func (rn *redisNamespace) ZAdd(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return rn.ZAdd(ctx, rn.appendNamespaceToKey(key), members...)
}

func (rn *redisNamespace) ZAddNX(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return rn.ZAddNX(ctx, rn.appendNamespaceToKey(key), members...)
}

func (rn *redisNamespace) ZAddXX(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return rn.ZAddXX(ctx, rn.appendNamespaceToKey(key), members...)
}

func (rn *redisNamespace) ZAddCh(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return rn.ZAddCh(ctx, rn.appendNamespaceToKey(key), members...)
}

func (rn *redisNamespace) ZAddNXCh(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return rn.ZAddNXCh(ctx, rn.appendNamespaceToKey(key), members...)
}

func (rn *redisNamespace) ZAddXXCh(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return rn.ZAddXXCh(ctx, rn.appendNamespaceToKey(key), members...)
}

func (rn *redisNamespace) ZIncr(ctx context.Context, key string, member *redis.Z) *redis.FloatCmd {
	return rn.ZIncr(ctx, rn.appendNamespaceToKey(key), member)
}

func (rn *redisNamespace) ZIncrNX(ctx context.Context, key string, member *redis.Z) *redis.FloatCmd {
	return rn.ZIncrNX(ctx, rn.appendNamespaceToKey(key), member)
}

func (rn *redisNamespace) ZIncrXX(ctx context.Context, key string, member *redis.Z) *redis.FloatCmd {
	return rn.ZIncrXX(ctx, rn.appendNamespaceToKey(key), member)
}

func (rn *redisNamespace) ZCard(ctx context.Context, key string) *redis.IntCmd {
	return rn.ZCard(ctx, rn.appendNamespaceToKey(key))
}

func (rn *redisNamespace) ZCount(ctx context.Context, key, min, max string) *redis.IntCmd {
	return rn.ZCount(ctx, rn.appendNamespaceToKey(key), min, max)
}

func (rn *redisNamespace) ZLexCount(ctx context.Context, key, min, max string) *redis.IntCmd {
	return rn.ZLexCount(ctx, rn.appendNamespaceToKey(key), min, max)
}

func (rn *redisNamespace) ZIncrBy(ctx context.Context, key string, increment float64, member string) *redis.FloatCmd {
	return rn.ZIncrBy(ctx, rn.appendNamespaceToKey(key), increment, member)
}

func (rn *redisNamespace) ZInterStore(ctx context.Context, destination string, store *redis.ZStore) *redis.IntCmd {
	store.keys = rn.appendNamespaceToKeys(store.keys)
	return rn.ZInterStore(ctx, rn.appendNamespaceToKey(destination), store)
}

func (rn *redisNamespace) ZRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return rn.ZRange(ctx, rn.appendNamespaceToKey(key), start, stop)
}

func (rn *redisNamespace) ZRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd {
	return rn.ZRangeWithScores(ctx, rn.appendNamespaceToKey(key), start, stop)
}

func (rn *redisNamespace) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return rn.ZRangeByScore(ctx, rn.appendNamespaceToKey(key), opt)
}

func (rn *redisNamespace) ZRangeByLex(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return rn.ZRangeByLex(ctx, rn.appendNamespaceToKey(key), opt)
}

func (rn *redisNamespace) ZRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	return rn.ZRangeByScoreWithScores(ctx, rn.appendNamespaceToKey(key), opt)
}

func (rn *redisNamespace) ZRank(ctx context.Context, key, member string) *redis.IntCmd {
	return rn.ZRank(ctx, rn.appendNamespaceToKey(key), member)
}

func (rn *redisNamespace) ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return rn.ZRem(ctx, rn.appendNamespaceToKey(key), members...)
}

func (rn *redisNamespace) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) *redis.IntCmd {
	return rn.ZRemRangeByRank(ctx, rn.appendNamespaceToKey(key), start, stop)
}

func (rn *redisNamespace) ZRemRangeByScore(ctx context.Context, key, min, max string) *redis.IntCmd {
	return rn.ZRemRangeByScore(ctx, rn.appendNamespaceToKey(key), min, max)
}

func (rn *redisNamespace) ZRemRangeByLex(ctx context.Context, key, min, max string) *redis.IntCmd {
	return rn.ZRemRangeByLex(ctx, rn.appendNamespaceToKey(key), min, max)
}

func (rn *redisNamespace) ZRevRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return rn.ZRevRange(ctx, rn.appendNamespaceToKey(key), start, stop)
}

func (rn *redisNamespace) ZRevRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd {
	return rn.ZRevRangeWithScores(ctx, rn.appendNamespaceToKey(key), start, stop)
}

func (rn *redisNamespace) ZRevRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return rn.ZRevRangeByScore(ctx, rn.appendNamespaceToKey(key), opt)
}

func (rn *redisNamespace) ZRevRangeByLex(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return rn.ZRevRangeByLex(ctx, rn.appendNamespaceToKey(key), opt)
}

func (rn *redisNamespace) ZRevRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	return rn.ZRevRangeByScoreWithScores(ctx, rn.appendNamespaceToKey(key), opt)
}

func (rn *redisNamespace) ZRevRank(ctx context.Context, key, member string) *redis.IntCmd {
	return rn.ZRevRank(ctx, rn.appendNamespaceToKey(key), member)
}

func (rn *redisNamespace) ZScore(ctx context.Context, key, member string) *redis.FloatCmd {
	return rn.ZScore(ctx, rn.appendNamespaceToKey(key), member)
}

func (rn *redisNamespace) ZUnionStore(ctx context.Context, dest string, store *redis.ZStore) *redis.IntCmd {
	store.Keys = rn.appendNamespaceToKeys(store.Keys)
	return rn.ZUnionStore(ctx, rn.appendNamespaceToKey(dest), store)
}

func (rn *redisNamespace) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	return rn.Do(ctx, args...)
}
func (rn *redisNamespace) Process(ctx context.Context, cmd redis.Cmder) error {
	return rn.Process(ctx, cmd)
}
func (rn *redisNamespace) Close() error {
	return rn.Close()
}
func (rn *redisNamespace) Discard() error {
	return rn.Discard()
}
func (rn *redisNamespace) Exec(ctx context.Context) ([]redis.Cmder, error) {
	return rn.Exec(ctx)
}
func (rn *redisNamespace) Auth(ctx context.Context, password string) *redis.StatusCmd {
	return rn.Auth(ctx, password)
}
func (rn *redisNamespace) AuthACL(ctx context.Context, username, password string) *redis.StatusCmd {
	return rn.AuthACL(ctx, username, password)
}
func (rn *redisNamespace) Select(ctx context.Context, index int) *redis.StatusCmd {
	return rn.Select(ctx, index)
}
func (rn *redisNamespace) SwapDB(ctx context.Context, index1, index2 int) *redis.StatusCmd {
	return rn.SwapDB(ctx, index1, index2)
}
func (rn *redisNamespace) ClientSetName(ctx context.Context, name string) *redis.BoolCmd {
	return rn.ClientSetName(ctx, name)
}

func (rn *redisNamespace) appendNamespaceToKey(key string) string {
	if len(rn.namespace) == 0 {
		return key
	}
	return rn.namespace + namespaceSeperator + key
}

func (rn *redisNamespace) appendNamespaceToKeys(keys []string) []string {
	if len(rn.namespace) == 0 {
		return keys
	}
	namespacedKeys := make([]string, len(keys))
	for _, key := range keys {
		key = rn.namespace + namespaceSeperator + key
		namespacedKeys = append(namespacedKeys, key)
	}
	return namespacedKeys
}

func (rn *redisNamespace) appendNamespaceToPairs(args []interface{}) []interface{} {
	if len(rn.namespace) == 0 {
		return args
	}
	namespacedArgs := make([]interface{}, len(args))
	for i, arg := range args {
		if i&1 == 0 {
			arg = rn.namespace + namespaceSeperator + arg.(string)
		}
		namespacedArgs = append(namespacedArgs, arg)
	}
	return namespacedArgs
}
