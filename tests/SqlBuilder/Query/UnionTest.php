<?php

namespace SqlBuilder\Tests\SqlBuilder\Query;

use SqlBuilder\DbAdapter\DbAdapter;
use SqlBuilder\DbAdapter\DbPostgreSql;
use SqlBuilder\Query\Select;
use SqlBuilder\Query\Union;

class UnionTest extends \PHPUnit_Framework_TestCase
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

    public function testUnion()
    {
        $selectA = new Select($this->db);
        $selectA
            ->column('*')
            ->from('table1')
            ->where("table1.status_id = :status_id")
            ->bindParam(':table1_status_id', 1);

        $selectB = new Select($this->db);
        $selectB
            ->column('*')
            ->from('table2')
            ->where("table2.status_id = :status_id")
            ->bindParam(':table2_status_id', 2);

        // default
        $select = new Select($this->db);
        $select
            ->column('*')
            ->from(new Union($this->db, array($selectA, $selectB)), 't')
            ->orderBy('t.created', Select::DESC);

        $this->assertEquals(
            'SELECT * FROM (SELECT * FROM "table1" WHERE table1.status_id = :status_id UNION ALL SELECT * FROM "table2" WHERE table2.status_id = :status_id) AS "t" ORDER BY t.created ' . Select::DESC,
            (string)$select
        );

        $bindParams = $select->getBindParams();
        $this->assertEquals(1, $bindParams[':table1_status_id']);
        $this->assertEquals(2, $bindParams[':table2_status_id']);

        // all
        $select = new Select($this->db);
        $select
            ->column('*')
            ->from(new Union($this->db, array($selectA, $selectB), array(Union::ALL)), 't')
            ->orderBy('t.created', Select::DESC);

        $this->assertEquals(
            'SELECT * FROM (SELECT * FROM "table1" WHERE table1.status_id = :status_id UNION ALL SELECT * FROM "table2" WHERE table2.status_id = :status_id) AS "t" ORDER BY t.created ' . Select::DESC,
            (string)$select
        );

        $bindParams = $select->getBindParams();
        $this->assertEquals(1, $bindParams[':table1_status_id']);
        $this->assertEquals(2, $bindParams[':table2_status_id']);

        // distinct
        $select = new Select($this->db);
        $select
            ->column('*')
            ->from(new Union($this->db, array($selectA, $selectB), array(Union::DISTINCT)), 't')
            ->orderBy('t.created', Select::DESC);

        $this->assertEquals(
            'SELECT * FROM (SELECT * FROM "table1" WHERE table1.status_id = :status_id UNION DISTINCT SELECT * FROM "table2" WHERE table2.status_id = :status_id) AS "t" ORDER BY t.created ' . Select::DESC,
            (string)$select
        );

        $bindParams = $select->getBindParams();
        $this->assertEquals(1, $bindParams[':table1_status_id']);
        $this->assertEquals(2, $bindParams[':table2_status_id']);
    }
}
