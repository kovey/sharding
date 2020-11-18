<?php
/**
 * @description
 *
 * @package
 *
 * @author kovey
 *
 * @time 2020-11-16 23:17:03
 *
 */
namespace Kovey\Sharding;

use Kovey\Sharding\Algorithm\ConsistencyHash;
use Kovey\Connection\Pool;
use Kovey\Redis\RedisInterface as RI;

class Redis implements RedisInterface
{
    /**
     * @description data base
     *
     * @var ConsistencyHash
     */
    private ConsistencyHash $hash;

    /**
     * @description db connections
     *
     * @var Array
     */
    private Array $connections;

    /**
     * @description init pool event
     */
    private $initPool;

	/**
	 * @description construct
	 *
     * @param int $redisCount
     *
     * @param callable | Array $initPool
     *
     * @return Mysql
	 */
    public function __construct(int $redisCount, callable | Array $initPool, Array $shardingKeys = array())
    {
        if (!is_callable($initPool)) {
            throw \RuntimeException('initPool event is not callable', 1014);
        }

        $this->initPool = $initPool;
        $this->hash = new ConsistencyHash(32);
        for ($i = 0; $i < $redisCount; $i ++) {
            $this->hash->addNode($i);
        }

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
        $partition = $this->hash->getNode($shardingKey);
        if (isset($this->connections[$partition])) {
            return $this;
        }

        $pool = call_user_func($this->initPool, $partition);
        if (!$pool instanceof Pool) {
            throw new \RuntimeException('pool is not instanceof Pool', 1011);
        }

        $this->connections[$partition] = $pool;
        return $this;
    }

    /**
     * @description get sharding key
     *
     * @param string | int $shardingKey
     *
     * @return int | string
     */
    public function getShardingKey(string | int $shardingKey) : int | string
    {
        return $this->hash->getNode($shardingKey);
    }
    
    /**
     * @description get connection
     *
     * @param string | int $shardingKey
     *
     * @return RedisInterface
     */
    public function getConnection(string | int $shardingKey) : RI
    {
        $shardingKey = $this->hash->getNode($shardingKey);
        if (!isset($this->connections[$shardingKey])) {
            throw new \RuntimeException("connection of $shardingKey is not exists.", 1009);
        }

        return $this->connections[$shardingKey]->getConnection();
    }

    public function __call(string $method, Array $params)
    {
        if (empty($params)) {
            throw new \RuntimeException('params is empty.');
        }

        return $this->getConnection($params[0])->$method(...$params);
    }
}
