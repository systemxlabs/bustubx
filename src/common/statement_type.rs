pub enum StatementType {
    INVALID_STATEMENT,       // invalid statement type
    SELECT_STATEMENT,        // select statement type
    INSERT_STATEMENT,        // insert statement type
    UPDATE_STATEMENT,        // update statement type
    CREATE_STATEMENT,        // create statement type
    DELETE_STATEMENT,        // delete statement type
    EXPLAIN_STATEMENT,       // explain statement type
    DROP_STATEMENT,          // drop statement type
    INDEX_STATEMENT,         // index statement type
    VARIABLE_SET_STATEMENT,  // set variable statement type
    VARIABLE_SHOW_STATEMENT, // show variable statement type
}
