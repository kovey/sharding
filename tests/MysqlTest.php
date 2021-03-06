<?php
/**
 * @description
 *
 * @package
 *
 * @author kovey
 *
 * @time 2020-10-29 15:08:30
 *
 */
namespace Kovey\Sharding;

use PHPUnit\Framework\TestCase;
use Kovey\Connection\Pool\Mysql as PM;
use Kovey\Connection\Pool;
use Kovey\Db\Adapter;
use Kovey\Db\Sql\Insert;
use Kovey\Db\Sql\Update;
use Kovey\Db\Sql\Select;
use Kovey\Db\Sql\Delete;
use Kovey\Db\Sql\BatchInsert;
use Kovey\Db\Exception\DbException;

class MysqlTest extends TestCase
{
    protected static ?Mysql $mysql;

    public static function setUpBeforeClass() : void
    {
        self::$mysql = new Mysql(2, function ($partition) {
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

        self::$mysql->addShardingKey(10000)
             ->addShardingKey('kovey');
    }

    protected function setUp() : void
    {
        self::$mysql->exec('create table test_0 (id int AUTO_INCREMENT, name varchar(512) NOT NULL DEFAULT \'\', PRIMARY KEY (id))', 10000);
        self::$mysql->exec('create table test_1 (id int AUTO_INCREMENT, name varchar(512) NOT NULL DEFAULT \'\', PRIMARY KEY (id))', 'kovey');
    }

    public function testQuery()
    {
        $this->assertEquals(array(), self::$mysql->query('select * from test_0', 10000));
        $this->assertEquals(array(), self::$mysql->query('select * from test_1', 'kovey'));
    }

    public function testInsert()
    {
        $insert = new Insert('test_0');
        $insert->name = 'kovey0';
        $this->assertEquals(1, self::$mysql->insert($insert, 10000));
        $insert = new Insert('test_1');
        $insert->name = 'kovey1';
        $this->assertEquals(1, self::$mysql->insert($insert, 'kovey'));

        $this->assertEquals(array(array('id' => 1, 'name' => 'kovey0')), self::$mysql->query('select * from test_0', 10000));
        $this->assertEquals(array(array('id' => 1, 'name' => 'kovey1')), self::$mysql->query('select * from test_1', 'kovey'));
    }

    public function testUpdate()
    {
        $insert = new Insert('test_0');
        $insert->name = 'kovey0';
        self::$mysql->insert($insert, 10000);
        $insert = new Insert('test_1');
        $insert->name = 'kovey1';
        self::$mysql->insert($insert, 'kovey');

        $update = new Update('test_0');
        $update->name = 'kovey2';
        $update->where(array('id' => 1));
        $this->assertEquals(1, self::$mysql->update($update, 10000));

        $update = new Update('test_1');
        $update->name = 'kovey3';
        $update->where(array('id' => 1));
        $this->assertEquals(1, self::$mysql->update($update, 'kovey'));

        $this->assertEquals(array(array('id' => 1, 'name' => 'kovey2')), self::$mysql->query('select * from test_0', 10000));
        $this->assertEquals(array(array('id' => 1, 'name' => 'kovey3')), self::$mysql->query('select * from test_1', 'kovey'));
    }

    public function testSelect()
    {
        $insert = new Insert('test_0');
        $insert->name = 'kovey0';
        self::$mysql->insert($insert, 10000);
        $insert = new Insert('test_1');
        $insert->name = 'kovey1';
        self::$mysql->insert($insert, 'kovey');

        $select = new Select('test_0');
        $select->columns(array('id', 'name'))
            ->where(array('id' => 1));
        $this->assertEquals(array(array('id' => 1, 'name' => 'kovey0')), self::$mysql->select($select, 10000));
        $this->assertEquals(array('id' => 1, 'name' => 'kovey0'), self::$mysql->select($select, 10000, $select::SINGLE));

        $select = new Select('test_1');
        $select->columns(array('id', 'name'))
            ->where(array('id' => 1));
        $this->assertEquals(array(array('id' => 1, 'name' => 'kovey1')), self::$mysql->select($select, 'kovey'));
        $this->assertEquals(array('id' => 1, 'name' => 'kovey1'), self::$mysql->select($select, 'kovey', $select::SINGLE));
    }

    public function testDelete()
    {
        $insert = new Insert('test_0');
        $insert->name = 'kovey0';
        self::$mysql->insert($insert, 10000);
        $insert = new Insert('test_1');
        $insert->name = 'kovey1';
        self::$mysql->insert($insert, 'kovey');

        $delete = new Delete('test_0');
        $delete->where(array('id' => 1));
        $this->assertEquals(1, self::$mysql->delete($delete, 10000));

        $delete = new Delete('test_1');
        $delete->where(array('id' => 1));
        $this->assertEquals(1, self::$mysql->delete($delete, 'kovey'));

        $this->assertEquals(array(), self::$mysql->query('select * from test_0', 10000));
        $this->assertEquals(array(), self::$mysql->query('select * from test_1', 'kovey'));
    }

    public function testBatchInsert()
    {
        $batchInsert = new BatchInsert('test_0');
        for ($i = 0; $i < 2; $i ++) {
            $insert = new Insert('test_0');
            $insert->name = 'kovey' . $i;
            $batchInsert->add($insert);
        }

        $this->assertEquals(2, self::$mysql->batchInsert($batchInsert, 10000));
        $this->assertEquals(array(
            array(
                'id' => 1,
                'name' => 'kovey0'
            ),
            array(
                'id' => 2,
                'name' => 'kovey1'
            ),
        ), self::$mysql->query('select * from test_0', 10000));
        
        $batchInsert = new BatchInsert('test_1');
        for ($i = 0; $i < 2; $i ++) {
            $insert = new Insert('test_1');
            $insert->name = 'kovey' . $i;
            $batchInsert->add($insert);
        }

        $this->assertEquals(2, self::$mysql->batchInsert($batchInsert, 'kovey'));
        $this->assertEquals(array(
            array(
                'id' => 1,
                'name' => 'kovey0'
            ),
            array(
                'id' => 2,
                'name' => 'kovey1'
            ),
        ), self::$mysql->query('select * from test_1', 'kovey'));
    }

    public function testFetchRow()
    {
        $insert = new Insert('test_0');
        $insert->name = 'kovey0';
        self::$mysql->insert($insert, 10000);
        $insert = new Insert('test_1');
        $insert->name = 'kovey1';
        self::$mysql->insert($insert, 'kovey');
        
        $this->assertEquals(array('id' => 1, 'name' => 'kovey0'), self::$mysql->fetchRow('test_0', array('id' => 1), array('id', 'name'), 10000));
        $this->assertEquals(array('id' => 1, 'name' => 'kovey1'), self::$mysql->fetchRow('test_1', array('id' => 1), array('id', 'name'), 'kovey'));
    }

    public function testFetchAll()
    {
        $batchInsert = new BatchInsert('test_0');
        for ($i = 0; $i < 2; $i ++) {
            $insert = new Insert('test_0');
            $insert->name = 'kovey' . $i;
            $batchInsert->add($insert);
        }

        self::$mysql->batchInsert($batchInsert, 10000);
        $this->assertEquals(array(
            array(
                'id' => 1,
                'name' => 'kovey0'
            ),
            array(
                'id' => 2,
                'name' => 'kovey1'
            ),
        ), self::$mysql->fetchAll('test_0', array(), array('id', 'name'), 10000));
        
        $batchInsert = new BatchInsert('test_1');
        for ($i = 0; $i < 2; $i ++) {
            $insert = new Insert('test_1');
            $insert->name = 'kovey' . $i;
            $batchInsert->add($insert);
        }

        self::$mysql->batchInsert($batchInsert, 'kovey');
        $this->assertEquals(array(
            array(
                'id' => 1,
                'name' => 'kovey0'
            ),
            array(
                'id' => 2,
                'name' => 'kovey1'
            ),
        ), self::$mysql->fetchAll('test_1', array(), array('id', 'name'), 'kovey'));
    }

    public function testTransactionSuccess()
    {
        $result = self::$mysql->transaction(function ($mysql, $params) {
            $insert = new Insert('test_0');
            $insert->name = 'kovey0';
            $mysql->insert($insert, 10000);
            $insert = new Insert('test_1');
            $insert->name = 'kovey1';
            $mysql->insert($insert, 'kovey');
            $this->assertEquals('aaaa', $params);
        }, function ($mysql, $params) {
            $this->assertInstanceOf(DbInterface::class, $mysql);
            $this->assertEquals('aaaa', $params);
        }, 'aaaa');

        $this->assertTrue($result);
        $this->assertEquals(array('id' => 1, 'name' => 'kovey0'), self::$mysql->fetchRow('test_0', array('id' => 1), array('id', 'name'), 10000));
        $this->assertEquals(array('id' => 1, 'name' => 'kovey1'), self::$mysql->fetchRow('test_1', array('id' => 1), array('id', 'name'), 'kovey'));
    }

    public function testTransactionFailure()
    {
        $this->expectException(DbException::class);
        $result = self::$mysql->transaction(function ($mysql, $params) {
            $insert = new Insert('test_0');
            $insert->name = 'kovey0';
            $mysql->insert($insert, 10000);
            $insert = new Insert('test_0');
            $insert->name = 'kovey1';
            $mysql->insert($insert, 'kovey');
        }, function ($mysql, $params) {
            $this->assertInstanceOf(DbInterface::class, $mysql);
            $this->assertEquals('aaaa', $params);
        }, 'aaaa');

        $this->assertFalse(self::$mysql->fetchRow('test_0', array('id' => 1), array('id', 'name'), 10000));
        $this->assertFalse(self::$mysql->fetchRow('test_1', array('id' => 1), array('id', 'name'), 'kovey'));
    }

    protected function tearDown() : void
    {
        self::$mysql->exec('drop table test_0', 10000);
        self::$mysql->exec('drop table test_1', 'kovey');
    }

    public static function tearDownAfterClass() : void
    {
        self::$mysql = null;
    }
}
