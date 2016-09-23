<?php

namespace SqlBuilder\DbAdapter;

class DbPostgreSql extends DbAdapter
{
    /**
     * @inheritdoc
     */
    public function supportWith()
    {
        return true;
    }
}
