<?php
/**
 * @description
 *
 * @package
 *
 * @author kovey
 *
 * @time 2020-10-22 18:13:15
 *
 */
namespace Kovey\Db\Sharding;

require_once __DIR__ . '/Cases/ShardingTable.php';

use PHPUnit\Framework\TestCase;
use Kovey\Sharding\Mysql;
use Kovey\Connection\Pool\Mysql as PM;
use Kovey\Connection\Pool;
use Kovey\Db\Adapter;
use Kovey\Sharding\Model\Cases\ShardingTable;

class BaseTest extends TestCase
{
    protected function setUp() : void
    {
        $this->mysql = new Mysql(2, function ($partition) {
            $pool = new PM(array(
                'min' => 2,
                'max' => 4
            ), array(
                'dbname' => 'test_' . $partition,
                'host' => '127.0.0.1',
                'username' => 'root',
                'password' => '',
                'port' => 3306,
                'charset' => 'UTF8',
                'adapter' => Adapter::DB_ADAPTER_PDO,
                'options' => array()
            ));
            $pool->init();
            return new Pool($pool);
        });
        $this->mysql->addShardingKey(10000)
             ->addShardingKey('kovey');

        $this->mysql->exec('create table test_0 (id int AUTO_INCREMENT, number int, PRIMARY KEY (id))', 10000);
        $this->mysql->exec('create table test_1 (id int AUTO_INCREMENT, number int, PRIMARY KEY (id))', 'kovey');
    }

    public function testInsert()
    {
        $table = new ShardingTable();
        $table->database = $this->mysql;
        $this->assertEquals(1, $table->insert(array(
            'number' => 1
        ), 10000));
        $this->assertEquals(array('id' => 1, 'number' => 1), $table->fetchRow(array('id' => 1), array('id', 'number'), 10000));

        $this->assertEquals(1, $table->insert(array(
            'number' => 1
        ), 'kovey'));
        $this->assertEquals(array('id' => 1, 'number' => 1), $table->fetchRow(array('id' => 1), array('id', 'number'), 'kovey'));
    }

    public function testUpdate()
    {
        $table = new ShardingTable();
        $table->database = $this->mysql;
        $table->insert(array(
            'number' => 1
        ), 10000);

        $this->assertEquals(1, $table->update(array(
            'number' => 3
        ), array('id' => 1), 10000));

        $this->assertEquals(array('id' => 1, 'number' => 3), $table->fetchRow(array('id' => 1), array('id', 'number'), 10000));

        $table->insert(array(
            'number' => 1
        ), 'kovey');

        $this->assertEquals(1, $table->update(array(
            'number' => 3
        ), array('id' => 1), 'kovey'));

        $this->assertEquals(array('id' => 1, 'number' => 3), $table->fetchRow(array('id' => 1), array('id', 'number'), 'kovey'));
    }

    public function testDelete()
    {
        $table = new ShardingTable();
        $table->database = $this->mysql;
        $this->assertEquals(1, $table->insert(array(
            'number' => 1
        ), 10000));

        $this->assertEquals(array('id' => 1, 'number' => 1), $table->fetchRow(array('id' => 1), array('id', 'number'), 10000));

        $this->assertEquals(1, $table->delete(array('id' => 1), 10000));
        $this->assertFalse($table->fetchRow(array('id' => 1), array('id', 'number'), 10000));

        $this->assertEquals(1, $table->insert(array(
            'number' => 1
        ), 'kovey'));

        $this->assertEquals(array('id' => 1, 'number' => 1), $table->fetchRow(array('id' => 1), array('id', 'number'), 'kovey'));

        $this->assertEquals(1, $table->delete(array('id' => 1), 'kovey'));
        $this->assertFalse($table->fetchRow(array('id' => 1), array('id', 'number'), 'kovey'));
    }

    public function testFetchRow()
    {
        $table = new ShardingTable();
        $table->database = $this->mysql;
        $table->insert(array(
            'number' => 1
        ), 10000);

        $this->assertEquals(array('id' => 1, 'number' => 1), $table->fetchRow(array('id' => 1), array('id', 'number'), 10000));

        $table->insert(array(
            'number' => 1
        ), 'kovey');

        $this->assertEquals(array('id' => 1, 'number' => 1), $table->fetchRow(array('id' => 1), array('id', 'number'), 'kovey'));
    }

    public function testFetchAll()
    {
        $table = new ShardingTable();
        $table->database = $this->mysql;
        $table->insert(array(
            'number' => 1
        ), 10000);
        $table->insert(array(
            'number' => 2
        ), 10000);

        $this->assertEquals(array(
            array('id' => 1, 'number' => 1),
            array('id' => 2, 'number' => 2),
        ), $table->fetchAll(array(), array('id', 'number'), 10000));
    
        $table->insert(array(
            'number' => 1
        ), 'kovey');
        $table->insert(array(
            'number' => 2
        ), 'kovey');

        $this->assertEquals(array(
            array('id' => 1, 'number' => 1),
            array('id' => 2, 'number' => 2),
        ), $table->fetchAll(array(), array('id', 'number'), 'kovey'));
    }

    public function testBatchInsert()
    {
        $table = new ShardingTable();
        $table->database = $this->mysql;
        $this->assertEquals(2, $table->batchInsert(array(
            array(
                'number' => 1
            ),
            array(
                'number' => 2
            )
        ), 10000));
        $this->assertEquals(array(
            array('id' => 1, 'number' => 1),
            array('id' => 2, 'number' => 2),
        ), $table->fetchAll(array(), array('id', 'number'), 10000));

        $this->assertEquals(2, $table->batchInsert(array(
            array(
                'number' => 1
            ),
            array(
                'number' => 2
            )
        ), 'kovey'));
        $this->assertEquals(array(
            array('id' => 1, 'number' => 1),
            array('id' => 2, 'number' => 2),
        ), $table->fetchAll(array(), array('id', 'number'), 'kovey'));
    }

    protected function tearDown() : void
    {
        $this->mysql->exec('drop table test_0', 10000);
        $this->mysql->exec('drop table test_1', 'kovey');
    }
}
