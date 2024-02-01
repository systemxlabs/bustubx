#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableReference {
    /// An unqualified table reference, e.g. "table"
    Bare {
        /// The table name
        table: String,
    },
    /// A partially resolved table reference, e.g. "schema.table"
    Partial {
        /// The schema containing the table
        schema: String,
        /// The table name
        table: String,
    },
    /// A fully resolved table reference, e.g. "catalog.schema.table"
    Full {
        /// The catalog (aka database) containing the table
        catalog: String,
        /// The schema containing the table
        schema: String,
        /// The table name
        table: String,
    },
}

impl TableReference {
    pub fn bare(table: String) -> Self {
        Self::Bare { table }
    }

    pub fn partial(schema: String, table: String) -> Self {
        Self::Partial { schema, table }
    }

    pub fn full(catalog: String, schema: String, table: String) -> Self {
        Self::Full {
            catalog,
            schema,
            table,
        }
    }

    pub fn table(&self) -> &str {
        match self {
            Self::Full { table, .. } | Self::Partial { table, .. } | Self::Bare { table } => table,
        }
    }

    pub fn schema(&self) -> Option<&str> {
        match self {
            Self::Full { schema, .. } | Self::Partial { schema, .. } => Some(schema),
            _ => None,
        }
    }

    pub fn catalog(&self) -> Option<&str> {
        match self {
            Self::Full { catalog, .. } => Some(catalog),
            _ => None,
        }
    }
}

impl std::fmt::Display for TableReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableReference::Bare { table } => write!(f, "{table}"),
            TableReference::Partial { schema, table } => {
                write!(f, "{schema}.{table}")
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => write!(f, "{catalog}.{schema}.{table}"),
        }
    }
}
