<?php
/**
 * @description
 *
 * @package
 *
 * @author kovey
 *
 * @time 2020-10-22 17:48:03
 *
 */
namespace Kovey\Db\Sharding;

use PHPUnit\Framework\TestCase;
use Kovey\Db\Mysql;
use Kovey\Db\Adapter;
use Kovey\Redis\Redis\Redis;
use Kovey\Db\Sql\Insert;

class GlobalIdentifyTest extends TestCase
{
    protected $mysql;

    protected $redis;

    protected function setUp() : void
    {
        $this->mysql = new Mysql(array(
            'dbname' => 'test',
            'host' => '127.0.0.1',
            'username' => 'root',
            'password' => '',
            'port' => 3306,
            'charset' => 'UTF8',
            'adapter' => Adapter::DB_ADAPTER_PDO,
            'options' => array()
        ));
        $this->mysql->connect();
        $this->mysql->exec('create table test (id int AUTO_INCREMENT, number int, PRIMARY KEY (id))');

        $this->redis = new Redis(array(
            'host' => '127.0.0.1',
            'port' => 6379,
            'db' => 0
        ));
        $this->redis->connect();

        $insert = new Insert('test');
        $insert->number = 10000;
        $this->mysql->insert($insert);
    }

    public function testGetShardingKey()
    {
        $gid = new GlobalIdentify($this->redis, $this->mysql);
        $gid->setTableInfo('test', 'number', 'id');
        $this->assertEquals(10000, $gid->getGlobalIdentify());
        $this->assertEquals(10001, $gid->getGlobalIdentify());
    }

    protected function tearDown() : void
    {
        $this->mysql->exec('drop table test');
        $this->redis->del('global_indentify_key_test');
    }
}
