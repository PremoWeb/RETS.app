// Helper function to convert RETS data type to MySQL type
export function getRetsToMySQLType(field: any): string {
  const dataType = field.DataType?.toLowerCase();
  const interpretation = field.Interpretation;
  const maxLength = field.MaxLength ? parseInt(field.MaxLength) : undefined;
  const precision = field.Precision ? parseInt(field.Precision) : undefined;

  if (interpretation === 'LookupMulti') {
    return 'TEXT';
  }
  if (interpretation === 'Lookup') {
    return 'VARCHAR(50)';
  }

  switch (dataType) {
    case 'int':
    case 'small':
    case 'tiny':
      return 'INT';
    case 'long':
      return 'BIGINT';
    case 'datetime':
      return "DATETIME default '0000-00-00 00:00:00' NOT NULL";
    case 'character':
      if (typeof maxLength === 'number' && maxLength > 0 && maxLength <= 255) {
        return `VARCHAR(${maxLength})`;
      }
      if (typeof maxLength === 'number' && maxLength > 255) {
        return 'TEXT';
      }
      return 'TEXT'; // fallback for missing/invalid MaxLength
    case 'decimal':
      if (typeof maxLength === 'number' && typeof precision === 'number' && maxLength > 0 && precision >= 0 && maxLength > precision) {
        return `DECIMAL(${maxLength},${precision})`;
      }
      return 'DECIMAL(10,2)';
    case 'boolean':
      return 'CHAR(1)';
    case 'date':
      return "DATE default '0000-00-00' NOT NULL";
    case 'time':
      return "TIME default '00:00:00' NOT NULL";
    default:
      return 'TEXT';
  }
}

export function generateCreateTableSQL(tableName: string, tableMetadata: any[], resource: any, cls?: any): string {
  const sql: string[] = [];
  sql.push(`CREATE TABLE IF NOT EXISTS \`${tableName}\` (`);
  if (!resource.KeyField) {
    sql.push('  `id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,');
  }
  const fields = tableMetadata.map((field: any) => {
    const sqlType = getRetsToMySQLType(field);
    const isRequired = field.Required === '1' ? ' NOT NULL' : '';
    const isPrimary = field.SystemName === resource.KeyField ? ' PRIMARY KEY' : '';
    return `  \`${field.SystemName}\` ${sqlType}${isRequired}${isPrimary} COMMENT '${field.LongName}'`;
  });
  sql.push(fields.join(',\n'));
  let tableComment = resource.Description;
  if (cls && cls.Description) {
    tableComment += ` - ${cls.Description}`;
  }
  sql.push(`) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT '${tableComment}';`);
  return sql.join('\n');
} 