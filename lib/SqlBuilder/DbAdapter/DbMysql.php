<?php

namespace SqlBuilder\DbAdapter;

class DbMySql extends DbAdapter
{
    /**
     * @inheritdoc
     */
    public function quoteIdentifier($text)
    {
        return '`' . $text . '`';
    }

    /**
     * @inheritdoc
     */
    public function supportWith()
    {
        return false;
    }
}
