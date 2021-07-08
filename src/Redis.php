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

class Redis extends Base implements RedisInterface
{
    /**
     * @description data base
     *
     * @var ConsistencyHash
     */
    protected ConsistencyHash $hash;

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

        $this->connections = array();
        foreach ($shardingKeys as $key) {
            $this->addShardingKey($key);
        }
    }

    protected function initAlgorithm(int $count) : void
    {
        $this->hash = new ConsistencyHash(32);
        for ($i = 0; $i < $count; $i ++) {
            $this->hash->addNode($i);
        }
    }

    /**
     * @description get sharding key
     *
     * @param string | int $shardingKey
     *
     * @return int
     */
    public function getShardingKey(string | int $shardingKey) : int
    {
        return $this->hash->getNode($shardingKey);
    }
    
    public function __call(string $method, Array $params) : mixed
    {
        if (empty($params)) {
            throw new \RuntimeException('params is empty.');
        }

        return $this->getConnection($params[0])->$method(...$params);
    }
}
