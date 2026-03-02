import React from 'react';
import { MissingRowsSection } from './MissingRowsSection';

export function PKRowCountMismatchView({ samples, validation }) {
  if (samples.skipped) {
    return (
      <div className="p-4 bg-yellow-900/20 border border-yellow-700 rounded">
        <p className="text-yellow-300 text-sm font-semibold">Analysis Skipped</p>
        <p className="text-yellow-200 text-xs mt-2">
          Source data was limited and source count ({validation.row_count_source?.toLocaleString()}) is less than 
          target count ({validation.row_count_target?.toLocaleString()}). Results would be unreliable.
        </p>
        {validation?.databricks_run_url && (
          <a href={validation.databricks_run_url} target="_blank" rel="noopener noreferrer" className="inline-block mt-2 text-xs text-blue-400 hover:text-blue-300 underline">
            View Notebook Run for details
          </a>
        )}
      </div>
    );
  }
  
  const { missing_in_target, missing_in_source } = samples;
  const pkColumns = validation.pk_columns || samples.pk_columns || [];
  const sourceTable = validation.source_table || 'SOURCE_TABLE';
  const targetTable = validation.target_table || 'TARGET_TABLE';
  
  return (
    <div className="space-y-3">
      <div className="p-2 bg-orange-900/20 border border-orange-700 rounded">
        <p className="text-orange-300 text-xs">
          Row count mismatch: Source has {validation.row_count_source?.toLocaleString()} rows, 
          Target has {validation.row_count_target?.toLocaleString()} rows 
          (diff: {Math.abs((validation.row_count_source || 0) - (validation.row_count_target || 0)).toLocaleString()})
        </p>
      </div>
      
      <MissingRowsSection title="Missing in Target" data={missing_in_target} tableName={sourceTable} pkColumns={pkColumns} defaultExpanded={true} />
      <MissingRowsSection title="Missing in Source (Extra in Target)" data={missing_in_source} tableName={targetTable} pkColumns={pkColumns} defaultExpanded={missing_in_target?.count === 0} />
    </div>
  );
}
