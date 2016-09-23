<?php

namespace SqlBuilder\Tests\SqlBuilder\Query;

use SqlBuilder\DbAdapter\DbAdapter;
use SqlBuilder\DbAdapter\DbMySql;
use SqlBuilder\DbAdapter\DbPostgreSql;
use SqlBuilder\Query\Select;
use SqlBuilder\Query\Update;
use SqlBuilder\SqlBuilderException;

class UpdateTest extends \PHPUnit_Framework_TestCase
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

    public function testTable()
    {
        $update = new Update($this->db);
        $update
            ->table('user')
            ->column('first_name', ':first_name');

        $this->assertEquals(
            'UPDATE "user" SET "first_name" = :first_name',
            (string)$update
        );
    }

    public function testTableMySql()
    {
        $this->db = $this->getMySqlDb();

        $update = new Update($this->db);
        $update
            ->table('user')
            ->column('first_name', ':first_name');

        $this->assertEquals(
            'UPDATE `user` SET `first_name` = :first_name',
            (string)$update
        );
    }

    public function testColumnsException()
    {
        try {
            $update = new Update($this->db);
            $update->table('user');
            $update->__toString();

            $this->fail("Expected exception " . SqlBuilderException::class . " not thrown");
        } catch (SqlBuilderException $e) {
            $this->assertEquals("'columns' property can't be empty in update statement", $e->getMessage());
        }

        try {
            $update = new Update($this->db);
            $update
                ->table('user')
                ->column('balance', [100, '**/']);
            $update->__toString();

            $this->fail("Expected exception " . SqlBuilderException::class . " not thrown");
        } catch (SqlBuilderException $e) {
            $this->assertEquals("Invalid operator **/", $e->getMessage());
        }
    }

    public function testColumns()
    {
        $select = new Select($this->db);
        $select
            ->column('phone')
            ->from('user_phone')
            ->where('user_id = user.id')
            ->limit(1);
        $update = new Update($this->db);
        $update
            ->table('user')
            ->columns([
                'phone' => $select,
                'nickname' => null,
                'status_id' => 1,
                'first_name' => ':first_name',
                'last_name' => 'Last Name',
                'balance' => [100, Update::OPERATOR_ADDITION],
            ]);

        $this->assertEquals(
            'UPDATE "user" SET "phone" = (SELECT "phone" FROM "user_phone" WHERE user_id = user.id LIMIT 1), "nickname" = NULL, "status_id" = 1, "first_name" = :first_name, "last_name" = \'Last Name\', "balance" = "balance" + 100',
            (string)$update
        );
    }

    public function testWhere()
    {
        $update = new Update($this->db);
        $update
            ->table('user')
            ->columns([
                'first_name' => ':first_name',
                'last_name' => 'Last Name',
            ])
            ->where("id = :id");

        $this->assertEquals(
            'UPDATE "user" SET "first_name" = :first_name, "last_name" = \'Last Name\' WHERE id = :id',
            (string)$update
        );
    }

    public function testBindParams()
    {
        $update = new Update($this->db);
        $update->bindParam(':p1', 'v1');

        $bindParams = $update->getBindParams();
        $this->assertEquals('v1', $bindParams[':p1']);

        $update->bindParam(':p2', 'v2');

        $bindParams = $update->getBindParams();
        $this->assertEquals('v1', $bindParams[':p1']);
        $this->assertEquals('v2', $bindParams[':p2']);

        $update->bindParams(array(
            ':p3' => 'v3',
            ':p4' => 'v4',
        ));

        $bindParams = $update->getBindParams();
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

        $update = new Update($this->db);
        $update
            ->column('col', $columnSelect)
            ->bindParams(array(
                ':p3' => 'v3',
                ':p4' => 'v4',
            ));

        $bindParams = $update->getBindParams();
        $this->assertEquals('v1', $bindParams[':p1']);
        $this->assertEquals('v2', $bindParams[':p2']);
        $this->assertEquals('v3', $bindParams[':p3']);
        $this->assertEquals('v4', $bindParams[':p4']);
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
