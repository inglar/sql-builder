<?php

namespace SqlBuilder\Tests\SqlBuilder\Query;

use SqlBuilder\DbAdapter\DbAdapter;
use SqlBuilder\DbAdapter\DbPostgreSql;
use SqlBuilder\Query\Join;

class JoinTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var DbAdapter
     */
    private $db;

    protected function setUp()
    {
        parent::setUp();
        $this->db = new DbPostgreSql();
    }

    protected function tearDown()
    {
        parent::tearDown();
        $this->db = null;
    }

    public function testJoin()
    {
        $join = new Join($this->db, 'table', 'a = b');

        $this->assertEquals(
            'INNER JOIN "table" ON a = b',
            (string)$join
        );
    }

    public function testJoinWithAlias()
    {
        $join = new Join($this->db, 'table', 'a = b', 't');

        $this->assertEquals(
            'INNER JOIN "table" AS "t" ON a = b',
            (string)$join
        );
    }

    public function testLeftJoin()
    {
        $join = new Join($this->db, 'table', 'a = b', 't', Join::LEFT);

        $this->assertEquals(
            'LEFT JOIN "table" AS "t" ON a = b',
            (string)$join
        );
    }

    public function testRightJoin()
    {
        $join = new Join($this->db, 'table', 'a = b', 't', Join::RIGHT);

        $this->assertEquals(
            'RIGHT JOIN "table" AS "t" ON a = b',
            (string)$join
        );
    }
}
