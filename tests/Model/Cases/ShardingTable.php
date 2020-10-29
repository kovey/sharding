<?php
/**
 * @description
 *
 * @package
 *
 * @author kovey
 *
 * @time 2020-10-22 17:48:59
 *
 */
namespace Kovey\Db\Model\Cases;

use Kovey\Db\Model\ShardingBase;

class ShardingTable extends ShardingBase
{
    protected string $tableName = 'test';
}
