<?php
/**
 * @description
 *
 * @package
 *
 * @author kovey
 *
 * @time 2021-07-08 17:50:01
 *
 */
namespace Kovey\Sharding;

use Kovey\Connection\ManualCollectInterface;
use Kovey\Connection\Pool;
use Kovey\Redis\RedisInterface;
use Kovey\Db\DbInterface;

abstract class Base implements ManualCollectInterface
{
    /**
     * @description db connections
     *
     * @var Array
     */
    protected Array $connections;

    /**
     * @description init pool event
     */
    protected mixed $initPool;

    /**
     * @description construct
     *
     * @param int $count
     *
     * @param callable | Array $initPool
     *
     * @return Mysql
     */
    public function __construct(int $count, callable | Array $initPool, Array $shardingKeys = array())
    {
        if (!is_callable($initPool)) {
            throw \RuntimeException('initPool event is not callable', 1014);
        }

        $this->initPool = $initPool;
        $this->initAlgorithm($count);

        $this->connections = array();
        foreach ($shardingKeys as $key) {
            $this->addShardingKey($key);
        }
    }

    /**
     * @description add sharding key
     *
     * @param string | int $shardingKey
     *
     * @return RedisInterface
     *
     */
    public function addShardingKey(string | int $shardingKey) : RedisInterface
    {
        $partition = $this->getShardingKey($shardingKey);
        if (isset($this->connections[$partition])) {
            return $this;
        }

        $pool = call_user_func($this->initPool, $partition);
        if (!$pool instanceof Pool) {
            throw new \RuntimeException('pool is not instanceof Pool', 1011);
        }

        $pool->traceId = $this->traceId ?? '';
        $pool->spanId = $this->spanId ?? '';
        $pool->initConnection();

        $this->connections[$partition] = $pool;
        return $this;
    }

    /**
     * @description get connection
     *
     * @param string | int $shardingKey
     *
     * @return RedisInterface | DbInterface
     */
    public function getConnection(string | int $shardingKey) : RedisInterface | DbInterface
    {
        $shardingKey = $this->getShardingKey($shardingKey);
        if (!isset($this->connections[$shardingKey])) {
            throw new \RuntimeException("connection of $shardingKey is not exists.", 1009);
        }

        return $this->connections[$shardingKey]->getConnection();
    }

    public function collect() : void
    {
        foreach ($this->connections as $pool) {
            if (!$pool instanceof ManualCollectInterface) {
                continue;
            }

            $pool->collect();
        }
    }

    abstract public function getShardingKey(string | int $shardingKey) : int;

    abstract protected function initAlgorithm(int $count) : void;
}
