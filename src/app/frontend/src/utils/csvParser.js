import Papa from 'papaparse';

/**
 * Parse CSV file and validate based on type
 */
export function parseCSV(file, type, schedules, onComplete, systems = []) {
  Papa.parse(file, {
    header: true,
    skipEmptyLines: true,
    complete: (results) => {
      const { validRows, errors } = validateCSVData(results.data, type, schedules, systems);
      onComplete(validRows, errors);
    },
    error: (err) => {
      onComplete([], [`Parse error: ${err.message}`]);
    }
  });
}

// Parse boolean from CSV - defaults to true, recognizes various false values
const parseBool = (val) => {
  if (!val) return true;
  const v = String(val).toLowerCase().trim();
  return !['false', 'f', '0', 'no', 'n'].includes(v);
};

// Parse JSON from CSV - returns null if empty/invalid
// Also fixes smart quotes from apps like Mac Numbers
const parseJson = (val, errors, field) => {
  if (!val || !val.trim()) return null;
  // Replace curly/smart quotes with straight quotes
  const fixed = val.replace(/[\u201C\u201D\u201E\u201F\u2033\u2036]/g, '"')
                   .replace(/[\u2018\u2019\u201A\u201B\u2032\u2035]/g, "'");
  try {
    return JSON.parse(fixed);
  } catch (e) {
    const preview = val.length > 50 ? val.substring(0, 50) + '...' : val;
    errors.push(`Invalid JSON in ${field}: "${preview}" (${e.message})`);
    return null;
  }
};

/**
 * Validate CSV data based on type
 */
function validateCSVData(data, type, schedules, systems) {
  const validationErrors = [];
  const validRows = [];
  
  data.forEach((row, idx) => {
    const rowNum = idx + 1;
    const rowErrors = [];
    
    if (type === 'tables') {
      if (!row.src_schema) rowErrors.push(`Missing src_schema`);
      if (!row.src_table) rowErrors.push(`Missing src_table`);
      
      const scheduleNames = row.schedule_name ? row.schedule_name.split(',').map(s => s.trim()).filter(s => s) : [];
      for (const sn of scheduleNames) {
        if (!schedules.find(s => s.name === sn)) rowErrors.push(`Schedule '${sn}' not found`);
      }
      
      const srcSystemName = row.source || row.src_system || row.src_system_name || null;
      const tgtSystemName = row.target || row.tgt_system || row.tgt_system_name || null;
      if (srcSystemName && systems.length > 0 && !systems.find(s => s.name === srcSystemName)) {
        rowErrors.push(`Source system '${srcSystemName}' not found`);
      }
      if (tgtSystemName && systems.length > 0 && !systems.find(s => s.name === tgtSystemName)) {
        rowErrors.push(`Target system '${tgtSystemName}' not found`);
      }
      
      const hasOptions = row.hasOwnProperty('options');
      const hasConfigOverrides = row.hasOwnProperty('config_overrides');
      const configOverrides = hasConfigOverrides ? parseJson(row.config_overrides, rowErrors, 'config_overrides') : undefined;
      let options = hasOptions ? (parseJson(row.options, rowErrors, 'options') || {}) : undefined;
      // Resolve system names to IDs in column_overrides (CSV export uses names for readability)
      if (options?.column_overrides && systems.length > 0) {
        const resolved = {};
        for (const [col, entries] of Object.entries(options.column_overrides)) {
          resolved[col] = {};
          for (const [key, expr] of Object.entries(entries)) {
            const sys = systems.find(s => s.name === key);
            resolved[col][sys ? String(sys.id) : key] = expr;
          }
        }
        options.column_overrides = resolved;
      }
      
      if (rowErrors.length === 0) {
        validRows.push({
          ...row,
          name: row.name || `${row.src_schema}.${row.src_table}`,
          tgt_schema: row.tgt_schema || row.src_schema,
          tgt_table: row.tgt_table || row.src_table,
          is_active: parseBool(row.is_active),
          pk_columns: row.pk_columns ? row.pk_columns.split(',').map(s => s.trim()) : null,
          include_columns: row.include_columns ? row.include_columns.split(',').map(s => s.trim()) : [],
          exclude_columns: row.exclude_columns ? row.exclude_columns.split(',').map(s => s.trim()) : [],
          ...(hasOptions ? { options } : {}),
          ...(hasConfigOverrides ? { config_overrides: configOverrides } : {}),
          tags: row.tags ? row.tags.split(',').map(s => s.trim()).filter(s => s) : [],
          schedule_names: scheduleNames,
          src_system_name: srcSystemName,
          tgt_system_name: tgtSystemName,
        });
      }
    } else if (type === 'schedules') {
      if (!row.name) rowErrors.push(`Missing name`);
      if (!row.cron_expr) rowErrors.push(`Missing cron_expr`);

      if (rowErrors.length === 0) {
        validRows.push({
          name: row.name.trim(),
          cron_expr: row.cron_expr.trim(),
          timezone: row.timezone?.trim() || 'UTC',
          enabled: parseBool(row.enabled),
        });
      }
    } else if (type === 'queries') {
      const srcSqlVal = (row.src_sql || row.sql || '').trim();
      if (!srcSqlVal) rowErrors.push(`Missing src_sql (column src_sql, or legacy sql)`);
      
      const scheduleNames = row.schedule_name ? row.schedule_name.split(',').map(s => s.trim()).filter(s => s) : [];
      for (const sn of scheduleNames) {
        if (!schedules.find(s => s.name === sn)) rowErrors.push(`Schedule '${sn}' not found`);
      }
      
      const srcSystemName = row.source || row.src_system || row.src_system_name || null;
      const tgtSystemName = row.target || row.tgt_system || row.tgt_system_name || null;
      if (srcSystemName && systems.length > 0 && !systems.find(s => s.name === srcSystemName)) {
        rowErrors.push(`Source system '${srcSystemName}' not found`);
      }
      if (tgtSystemName && systems.length > 0 && !systems.find(s => s.name === tgtSystemName)) {
        rowErrors.push(`Target system '${tgtSystemName}' not found`);
      }
      
      const hasConfigOverrides = row.hasOwnProperty('config_overrides');
      const configOverrides = hasConfigOverrides ? parseJson(row.config_overrides, rowErrors, 'config_overrides') : undefined;
      
      if (rowErrors.length === 0) {
        const tgtRaw = row.tgt_sql != null && String(row.tgt_sql).trim() ? String(row.tgt_sql).trim() : null;
        const { sql: _legacySql, src_sql: _s, ...rest } = row;
        validRows.push({
          ...rest,
          name: row.name || `Query ${rowNum}`,
          src_sql: srcSqlVal,
          tgt_sql: tgtRaw,
          is_active: parseBool(row.is_active),
          pk_columns: row.pk_columns ? row.pk_columns.split(',').map(s => s.trim()) : null,
          include_columns: row.include_columns ? row.include_columns.split(',').map(s => s.trim()) : [],
          exclude_columns: row.exclude_columns ? row.exclude_columns.split(',').map(s => s.trim()) : [],
          ...(hasConfigOverrides ? { config_overrides: configOverrides } : {}),
          tags: row.tags ? row.tags.split(',').map(s => s.trim()).filter(s => s) : [],
          schedule_names: scheduleNames,
          src_system_name: srcSystemName,
          tgt_system_name: tgtSystemName,
        });
      }
    }
    
    // Add row-specific errors to the main errors array
    rowErrors.forEach(err => validationErrors.push(`Row ${rowNum}: ${err}`));
  });
  
  return { validRows, errors: validationErrors };
}

