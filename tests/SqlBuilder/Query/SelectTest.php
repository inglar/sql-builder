<?php

namespace SqlBuilder\Tests\SqlBuilder\Query;

use SqlBuilder\DbAdapter\DbAdapter;
use SqlBuilder\DbAdapter\DbMySql;
use SqlBuilder\DbAdapter\DbPostgreSql;
use SqlBuilder\Query\Select;
use SqlBuilder\SqlBuilderException;

class SelectTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var DbAdapter
     */
    private $db;

    protected function setUp()
    {
        parent::setUp();
        $this->db = $this->getPostgreSqlDb();
    }

    protected function tearDown()
    {
        parent::tearDown();
        $this->db = null;
    }

    public function testWith()
    {
        try {
            $with = new Select($this->db);
            $select = new Select($this->db);
            $select
                ->with(null, $with);

            $this->fail("Expected exception " . SqlBuilderException::class . " not thrown");
        } catch (SqlBuilderException $e) {
            $this->assertEquals("Alias can't be empty", $e->getMessage());
        }

        $with = new Select($this->db);
        $with
            ->column('*')
            ->from('table1')
            ->where("table1.col = :t1col")
            ->bindParam(':t1col', 'val1');

        $select = new Select($this->db);
        $select
            ->with('tbl1', $with)
            ->column('*')
            ->from('table2')
            ->where("table2.col = :t2col")
            ->bindParam(':t2col', 'val2');

        $this->assertEquals(
            'WITH "tbl1" AS (SELECT * FROM "table1" WHERE table1.col = :t1col) SELECT * FROM "table2" WHERE table2.col = :t2col',
            (string)$select
        );

        $bindParams = $select->getBindParams();
        $this->assertEquals('val1', $bindParams[':t1col']);
        $this->assertEquals('val2', $bindParams[':t2col']);
    }

    public function testWithMysql()
    {
        $this->db = $this->getMySqlDb();

        try {
            $with = new Select($this->db);
            $select = new Select($this->db);
            $select
                ->with('tbl1', $with);

            $this->fail("Expected exception " . SqlBuilderException::class . " not thrown");
        } catch (SqlBuilderException $e) {
            $this->assertEquals("Current db driver doesn't support WITH clause", $e->getMessage());
        }
    }

    public function testWithMultiple()
    {
        $with1 = new Select($this->db);
        $with1
            ->column('*')
            ->from('table1')
            ->where("table1.col = :t1col")
            ->bindParam(':t1col', 'val1');

        $with2 = new Select($this->db);
        $with2
            ->column('*')
            ->from('table2')
            ->where("table2.col = :t2col")
            ->bindParam(':t2col', 'val2');

        $select = new Select($this->db);
        $select
            ->with('tbl1', $with1)
            ->with('tbl2', $with2)
            ->column('*')
            ->from('table3')
            ->where("table3.col = :t3col")
            ->bindParam(':t3col', 'val3');

        $this->assertEquals(
            'WITH "tbl1" AS (SELECT * FROM "table1" WHERE table1.col = :t1col), "tbl2" AS (SELECT * FROM "table2" WHERE table2.col = :t2col) SELECT * FROM "table3" WHERE table3.col = :t3col',
            (string)$select
        );

        $bindParams = $select->getBindParams();
        $this->assertEquals('val1', $bindParams[':t1col']);
        $this->assertEquals('val2', $bindParams[':t2col']);
    }

    public function testDistinct()
    {
        $select = new Select($this->db);
        $select
            ->distinct()
            ->column('*')
            ->from('table');

        $this->assertEquals(
            'SELECT DISTINCT * FROM "table"',
            (string)$select
        );

        $select->distinct(false);

        $this->assertEquals(
            'SELECT * FROM "table"',
            (string)$select
        );
    }

    public function testColumns()
    {
        $select = new Select($this->db);
        $select
            ->column('col1')
            ->from('table');

        $this->assertEquals(
            'SELECT "col1" FROM "table"',
            (string)$select
        );

        $select->column('col2');

        $this->assertEquals(
            'SELECT "col1", "col2" FROM "table"',
            (string)$select
        );

        $select->columns(array('col3', 'col4'));

        $this->assertEquals(
            'SELECT "col1", "col2", "col3", "col4" FROM "table"',
            (string)$select
        );

        $select->column('table.col5');

        $this->assertEquals(
            'SELECT "col1", "col2", "col3", "col4", table.col5 FROM "table"',
            (string)$select
        );

        $select->column("'value6'", 'col6');

        $this->assertEquals(
            'SELECT "col1", "col2", "col3", "col4", table.col5, \'value6\' AS "col6" FROM "table"',
            (string)$select
        );
    }

    public function testColumnsMysql()
    {
        $this->db = $this->getMySqlDb();

        $select = new Select($this->db);
        $select
            ->column('col1')
            ->from('table');

        $this->assertEquals(
            'SELECT `col1` FROM `table`',
            (string)$select
        );

        $select->column('col2');

        $this->assertEquals(
            'SELECT `col1`, `col2` FROM `table`',
            (string)$select
        );

        $select->columns(array('col3', 'col4'));

        $this->assertEquals(
            'SELECT `col1`, `col2`, `col3`, `col4` FROM `table`',
            (string)$select
        );
    }

    public function testColumnSelect()
    {
        $columnSelect = new Select($this->db);
        $columnSelect
            ->column('1')
            ->from('user_token')
            ->where("user_token.status_id = :status_id")
            ->limit(1)
            ->bindParam(':status_id', 1);

        $sqlIn = 'SELECT 1 FROM "user_token" WHERE user_token.status_id = :status_id LIMIT 1';
        $this->assertEquals($sqlIn, (string)$columnSelect);

        $bindParams = $columnSelect->getBindParams();
        $this->assertEquals(1, $bindParams[':status_id']);

        $select = new Select($this->db);
        $select
            ->columns(array(
                'user.id',
                'user.name',
                'have_tokens' => $columnSelect,
            ))
            ->from('user')
            ->where("user.created > :created")
            ->bindParam(':created', '2000-01-01 00:00:00');

        $this->assertEquals(
            'SELECT user.id, user.name, (' . $sqlIn . ') AS "have_tokens" FROM "user" WHERE user.created > :created',
            (string)$select
        );

        $bindParams = $select->getBindParams();
        $this->assertEquals(1, $bindParams[':status_id']);
        $this->assertEquals('2000-01-01 00:00:00', $bindParams[':created']);
    }

    public function testFrom()
    {
        $select = new Select($this->db);
        $select
            ->column('*')
            ->from('user');

        $this->assertEquals(
            'SELECT * FROM "user"',
            (string)$select
        );

        $select->from('transaction');

        $this->assertEquals(
            'SELECT * FROM "user", "transaction"',
            (string)$select
        );
    }

    public function testFromMysql()
    {
        $this->db = $this->getMySqlDb();

        $select = new Select($this->db);
        $select
            ->column('*')
            ->from('user');

        $this->assertEquals(
            'SELECT * FROM `user`',
            (string)$select
        );

        $select->from('transaction');

        $this->assertEquals(
            'SELECT * FROM `user`, `transaction`',
            (string)$select
        );
    }

    public function testFromWithAlias()
    {
        $select = new Select($this->db);
        $select
            ->column('*')
            ->from('user', 'u');

        $this->assertEquals(
            'SELECT * FROM "user" AS "u"',
            (string)$select
        );

        $select->from('transaction', 't');

        $this->assertEquals(
            'SELECT * FROM "user" AS "u", "transaction" AS "t"',
            (string)$select
        );
    }

    public function testFromSelect()
    {
        $fromSelect = new Select($this->db);
        $fromSelect
            ->columns(array('user.id', 'user.name'))
            ->from('user')
            ->where("user.status_id = :status_id")
            ->bindParam(':status_id', 1);

        $sqlIn = 'SELECT user.id, user.name FROM "user" WHERE user.status_id = :status_id';
        $this->assertEquals($sqlIn, (string)$fromSelect);

        $bindParams = $fromSelect->getBindParams();
        $this->assertEquals(1, $bindParams[':status_id']);

        $select = new Select($this->db);
        $select
            ->column('COUNT(*)', 'cnt')
            ->from($fromSelect, 'u')
            ->where("u.created > :created")
            ->bindParam(':created', '1970-01-01 00:00:00');

        $this->assertEquals(
            'SELECT COUNT(*) AS "cnt" FROM (' . $sqlIn . ') AS "u" WHERE u.created > :created',
            (string)$select
        );

        $bindParams = $select->getBindParams();
        $this->assertEquals(1, $bindParams[':status_id']);
        $this->assertEquals('1970-01-01 00:00:00', $bindParams[':created']);
    }

    public function testJoin()
    {
        $select = new Select($this->db);
        $select
            ->column('*')
            ->from('user')
            ->join('transaction', 'transaction.user_id = user.id');

        $this->assertEquals(
            'SELECT * FROM "user" INNER JOIN "transaction" ON transaction.user_id = user.id',
            (string)$select
        );
    }

    public function testJoinSelect()
    {
        $joinSelect = new Select($this->db);
        $joinSelect
            ->columns(array('user.id', 'user.name'))
            ->from('user')
            ->where("user.status_id = :status_id")
            ->bindParam(':status_id', 1);

        $sqlIn = 'SELECT user.id, user.name FROM "user" WHERE user.status_id = :status_id';
        $this->assertEquals($sqlIn, (string)$joinSelect);

        $bindParams = $joinSelect->getBindParams();
        $this->assertEquals(1, $bindParams[':status_id']);

        $select = new Select($this->db);
        $select
            ->column('*')
            ->from('user_token')
            ->join($joinSelect, 'user.id = user_token.user_id', 'user')
            ->where("user_token.created > :created")
            ->bindParam(':created', '1970-01-01 00:00:00');

        $this->assertEquals(
            'SELECT * FROM "user_token" INNER JOIN (' . $sqlIn . ') AS "user" ON user.id = user_token.user_id WHERE user_token.created > :created',
            (string)$select
        );

        $bindParams = $select->getBindParams();
        $this->assertEquals(1, $bindParams[':status_id']);
        $this->assertEquals('1970-01-01 00:00:00', $bindParams[':created']);
    }

    public function testWhere()
    {
        $select = new Select($this->db);
        $select
            ->column('*')
            ->from('user')
            ->where('user.status_id = :status_id');

        $this->assertEquals(
            'SELECT * FROM "user" WHERE user.status_id = :status_id',
            (string)$select
        );

        $select->where('user.created > :created');

        $this->assertEquals(
            'SELECT * FROM "user" WHERE user.status_id = :status_id AND user.created > :created',
            (string)$select
        );
    }

    public function testGroupBy()
    {
        $select = new Select($this->db);
        $select
            ->columns(array(
                'user_token.user_id',
                'cnt' => 'COUNT(*)',
            ))
            ->from('user_token')
            ->groupBy('user_token.user_id');

        $this->assertEquals(
            'SELECT user_token.user_id, COUNT(*) AS "cnt" FROM "user_token" GROUP BY user_token.user_id',
            (string)$select
        );

        $select
            ->column('user_token.created')
            ->groupBy('user_token.created');

        $this->assertEquals(
            'SELECT user_token.user_id, COUNT(*) AS "cnt", user_token.created FROM "user_token" GROUP BY user_token.user_id, user_token.created',
            (string)$select
        );
    }

    public function testHaving()
    {
        $select = new Select($this->db);
        $select
            ->columns(array(
                'user_token.user_id',
                'cnt' => 'COUNT(user_token.id)',
            ))
            ->from('user_token')
            ->groupBy('user_token.user_id')
            ->having('COUNT(user_token.id) > 0');

        $this->assertEquals(
            'SELECT user_token.user_id, COUNT(user_token.id) AS "cnt" FROM "user_token" GROUP BY user_token.user_id HAVING COUNT(user_token.id) > 0',
            (string)$select
        );

        $select
            ->column('SUM(user_token.counter)')
            ->having('SUM(user_token.counter) = 0');

        $this->assertEquals(
            'SELECT user_token.user_id, COUNT(user_token.id) AS "cnt", SUM(user_token.counter) FROM "user_token" GROUP BY user_token.user_id HAVING COUNT(user_token.id) > 0 AND SUM(user_token.counter) = 0',
            (string)$select
        );
    }

    public function testOrderBy()
    {
        $select = new Select($this->db);
        $select
            ->column('*')
            ->from('user')
            ->orderBy('created');

        $this->assertEquals(
            'SELECT * FROM "user" ORDER BY created',
            (string)$select
        );

        $select->orderBy('name', Select::DESC);

        $this->assertEquals(
            'SELECT * FROM "user" ORDER BY created, name DESC',
            (string)$select
        );

        $select->orderBy('balance', 'desc');

        $this->assertEquals(
            'SELECT * FROM "user" ORDER BY created, name DESC, balance DESC',
            (string)$select
        );

        $select->orderBy('last_name', 'asc');

        $this->assertEquals(
            'SELECT * FROM "user" ORDER BY created, name DESC, balance DESC, last_name',
            (string)$select
        );
    }

    public function testLimit()
    {
        $select = new Select($this->db);
        $select
            ->column('*')
            ->from('user')
            ->limit(10);

        $this->assertEquals(
            'SELECT * FROM "user" LIMIT 10',
            (string)$select
        );

        $select->limit(20, 10);

        $this->assertEquals(
            'SELECT * FROM "user" LIMIT 20 OFFSET 10',
            (string)$select
        );
    }

    public function testLock()
    {
        $select = new Select($this->db);
        $select
            ->column('*')
            ->from('user')
            ->where("id = :id")
            ->lock(Select::LOCK_FOR_SHARE);

        $this->assertEquals(
            'SELECT * FROM "user" WHERE id = :id FOR SHARE',
            (string)$select
        );

        $select = new Select($this->db);
        $select
            ->column('*')
            ->from('user')
            ->where("id = :id")
            ->lock(Select::LOCK_FOR_UPDATE);

        $this->assertEquals(
            'SELECT * FROM "user" WHERE id = :id FOR UPDATE',
            (string)$select
        );
    }

    public function testBindParams()
    {
        $select = new Select($this->db);
        $select->bindParam(':p1', 'v1');

        $bindParams = $select->getBindParams();
        $this->assertEquals('v1', $bindParams[':p1']);

        $select->bindParam(':p2', 'v2');

        $bindParams = $select->getBindParams();
        $this->assertEquals('v1', $bindParams[':p1']);
        $this->assertEquals('v2', $bindParams[':p2']);

        $select->bindParams(array(
            ':p3' => 'v3',
            ':p4' => 'v4',
        ));

        $bindParams = $select->getBindParams();
        $this->assertEquals('v1', $bindParams[':p1']);
        $this->assertEquals('v2', $bindParams[':p2']);
        $this->assertEquals('v3', $bindParams[':p3']);
        $this->assertEquals('v4', $bindParams[':p4']);
    }

    public function testBindParamsColumnSelect()
    {
        $columnSelect = new Select($this->db);
        $columnSelect->bindParams(array(
            ':p1' => 'v1',
            ':p2' => 'v2',
        ));

        $select = new Select($this->db);
        $select
            ->column($columnSelect, 'a')
            ->bindParams(array(
                ':p3' => 'v3',
                ':p4' => 'v4',
            ));

        $bindParams = $select->getBindParams();
        $this->assertEquals('v1', $bindParams[':p1']);
        $this->assertEquals('v2', $bindParams[':p2']);
        $this->assertEquals('v3', $bindParams[':p3']);
        $this->assertEquals('v4', $bindParams[':p4']);
    }

    public function testBindParamsFromSelect()
    {
        $fromSelect = new Select($this->db);
        $fromSelect->bindParams(array(
            ':p1' => 'v1',
            ':p2' => 'v2',
        ));

        $select = new Select($this->db);
        $select
            ->from($fromSelect, 'a')
            ->bindParams(array(
                ':p3' => 'v3',
                ':p4' => 'v4',
            ));

        $bindParams = $select->getBindParams();
        $this->assertEquals('v1', $bindParams[':p1']);
        $this->assertEquals('v2', $bindParams[':p2']);
        $this->assertEquals('v3', $bindParams[':p3']);
        $this->assertEquals('v4', $bindParams[':p4']);
    }

    public function testBindParamsJoinSelect()
    {
        $joinSelect = new Select($this->db);
        $joinSelect->bindParams(array(
            ':p1' => 'v1',
            ':p2' => 'v2',
        ));

        $select = new Select($this->db);
        $select
            ->join($joinSelect, '', 'a')
            ->bindParams(array(
                ':p3' => 'v3',
                ':p4' => 'v4',
            ));

        $bindParams = $select->getBindParams();
        $this->assertEquals('v1', $bindParams[':p1']);
        $this->assertEquals('v2', $bindParams[':p2']);
        $this->assertEquals('v3', $bindParams[':p3']);
        $this->assertEquals('v4', $bindParams[':p4']);
    }

    public function testReplaceArrayParameterPlaceholders()
    {
        $select = new Select($this->db);
        $select
            ->column('*')
            ->from('user')
            ->where('type_id = :type_id')
            ->where('status_id IN (:status_id)')
            ->bindParams(array(
                ':type_id' => 1,
                ':status_id' => 2,
            ));

        $this->assertEquals(
            'SELECT * FROM "user" WHERE type_id = :type_id AND status_id IN (:status_id)',
            (string)$select
        );

        $bindParams = $select->getBindParams();
        $this->assertArrayHasKey(':type_id', $bindParams);
        $this->assertEquals(1, $bindParams[':type_id']);
        $this->assertArrayHasKey(':status_id', $bindParams);
        $this->assertEquals(2, $bindParams[':status_id']);

        $select = new Select($this->db);
        $select
            ->column('*')
            ->from('user')
            ->where('type_id = :type_id')
            ->where('status_id IN (:status_id)')
            ->bindParams(array(
                ':type_id' => 1,
                ':status_id' => array(2, 3),
            ));

        $this->assertEquals(
            'SELECT * FROM "user" WHERE type_id = :type_id AND status_id IN (:status_id1, :status_id2)',
            (string)$select
        );

        $bindParams = $select->getBindParams();
        $this->assertArrayHasKey(':type_id', $bindParams);
        $this->assertEquals(1, $bindParams[':type_id']);
        $this->assertArrayHasKey(':status_id1', $bindParams);
        $this->assertEquals(2, $bindParams[':status_id1']);
        $this->assertArrayHasKey(':status_id2', $bindParams);
        $this->assertEquals(3, $bindParams[':status_id2']);

        $select = new Select($this->db);
        $select
            ->column('*')
            ->from('user')
            ->where('type_id = :type_id')
            ->where('status_id IN (:status_id)')
            ->bindParams(array(
                ':type_id' => 1,
                ':status_id' => new \ArrayIterator(array(2, 3)),
            ));

        $this->assertEquals(
            'SELECT * FROM "user" WHERE type_id = :type_id AND status_id IN (:status_id1, :status_id2)',
            (string)$select
        );

        $bindParams = $select->getBindParams();
        $this->assertArrayHasKey(':type_id', $bindParams);
        $this->assertEquals(1, $bindParams[':type_id']);
        $this->assertArrayHasKey(':status_id1', $bindParams);
        $this->assertEquals(2, $bindParams[':status_id1']);
        $this->assertArrayHasKey(':status_id2', $bindParams);
        $this->assertEquals(3, $bindParams[':status_id2']);
    }

    /**
     * @return DbMySql
     */
    private function getMySqlDb()
    {
        return new DbMySql();
    }

    /**
     * @return DbPostgreSql
     */
    private function getPostgreSqlDb()
    {
        return new DbPostgreSql();
    }
}
