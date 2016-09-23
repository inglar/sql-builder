<?php

namespace SqlBuilder;

use SqlBuilder\DbAdapter\DbAdapter;
use SqlBuilder\DbAdapter\DbMySql;
use SqlBuilder\DbAdapter\DbPostgreSql;
use SqlBuilder\Query\Select;
use SqlBuilder\Query\Union;
use SqlBuilder\Query\Update;

class SqlBuilder
{
    /**
     * @var DbAdapter
     */
    private $db;

    /**
     * @var array
     */
    private $adapters = array(
        'mysql' => DbMySql::class,
        'pgsql' => DbPostgreSql::class,
    );

    /**
     * @param string $dbName
     * @throws SqlBuilderException
     */
    public function __construct($dbName)
    {
        if (empty($dbName) || !isset($this->adapters[$dbName])) {
            throw new SqlBuilderException("Unsupported driver '$dbName'");
        }
        
        $this->db = new $this->adapters[$dbName];
    }

    /**
     * @return DbAdapter
     */
    public function getAdapter()
    {
        return $this->db;
    }

    /**
     * @return Select
     */
    public function select()
    {
        return new Select($this->db);
    }

    /**
     * @param Select[] $selects
     * @param array $unionTypes
     * @param string $alias
     * @return Union
     */
    public function union(array $selects, array $unionTypes = [], $alias = null)
    {
        return new Union($this->db, $selects, $unionTypes, $alias);
    }

    /**
     * @param array $allowedColumns
     * @return Update
     */
    public function update(array $allowedColumns = [])
    {
        return new Update($this->db, $allowedColumns);
    }
}
