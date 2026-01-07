import Papa from 'papaparse';

/**
 * Parse CSV file and validate based on type
 */
export function parseCSV(file, type, schedules, onComplete) {
  Papa.parse(file, {
    header: true,
    skipEmptyLines: true,
    complete: (results) => {
      const { validRows, errors } = validateCSVData(results.data, type, schedules);
      onComplete(validRows, errors);
    },
    error: (err) => {
      onComplete([], [`Parse error: ${err.message}`]);
    }
  });
}

/**
 * Validate CSV data based on type
 */
function validateCSVData(data, type, schedules) {
  const validationErrors = [];
  const validRows = [];
  
  data.forEach((row, idx) => {
    const rowNum = idx + 1;
    const rowErrors = [];
    
    if (type === 'tables') {
      // Required fields for tables
      if (!row.src_schema) rowErrors.push(`Missing src_schema`);
      if (!row.src_table) rowErrors.push(`Missing src_table`);
      if (!row.schedule_name) rowErrors.push(`Missing schedule_name`);
      
      // Check schedule exists
      if (row.schedule_name && !schedules.find(s => s.name === row.schedule_name)) {
        rowErrors.push(`Schedule '${row.schedule_name}' not found`);
      }
      
      if (rowErrors.length === 0) {
        validRows.push({
          ...row,
          name: row.name || `${row.src_schema}.${row.src_table}`,
          tgt_schema: row.tgt_schema || row.src_schema,
          tgt_table: row.tgt_table || row.src_table,
          is_active: row.is_active !== 'false' && row.is_active !== '0',
          pk_columns: row.pk_columns ? row.pk_columns.split(',').map(s => s.trim()) : null,
          include_columns: row.include_columns ? row.include_columns.split(',').map(s => s.trim()) : [],
          exclude_columns: row.exclude_columns ? row.exclude_columns.split(',').map(s => s.trim()) : [],
        });
      }
    } else if (type === 'queries') {
      // Required fields for queries
      if (!row.sql) rowErrors.push(`Missing sql`);
      if (!row.schedule_name) rowErrors.push(`Missing schedule_name`);
      
      // Check schedule exists
      if (row.schedule_name && !schedules.find(s => s.name === row.schedule_name)) {
        rowErrors.push(`Schedule '${row.schedule_name}' not found`);
      }
      
      if (rowErrors.length === 0) {
        validRows.push({
          ...row,
          name: row.name || `Query ${rowNum}`,
          is_active: row.is_active !== 'false' && row.is_active !== '0',
          pk_columns: row.pk_columns ? row.pk_columns.split(',').map(s => s.trim()) : null,
          include_columns: row.include_columns ? row.include_columns.split(',').map(s => s.trim()) : [],
          exclude_columns: row.exclude_columns ? row.exclude_columns.split(',').map(s => s.trim()) : [],
        });
      }
    }
    
    // Add row-specific errors to the main errors array
    rowErrors.forEach(err => validationErrors.push(`Row ${rowNum}: ${err}`));
  });
  
  return { validRows, errors: validationErrors };
}

