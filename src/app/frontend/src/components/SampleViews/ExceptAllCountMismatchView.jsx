import React from 'react';
import { ColumnDifferencesSection } from './ColumnDifferencesSection';
import { ExceptAllDiffSection } from './ExceptAllDiffSection';

export function ExceptAllCountMismatchView({ samples, validation }) {
  if (samples.data?.skipped) {
    return (
      <div className="p-4 bg-yellow-900/20 border border-yellow-700 rounded">
        <p className="text-yellow-300 text-sm font-semibold">Analysis Skipped</p>
        <p className="text-yellow-200 text-xs mt-2">
          Source data was limited and source count ({validation.row_count_source?.toLocaleString()}) is less than 
          target count ({validation.row_count_target?.toLocaleString()}). Results would be unreliable.
        </p>
      </div>
    );
  }
  
  const { column_differences, in_source_not_target, in_target_not_source } = samples.data || {};
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
      
      {column_differences && column_differences.length > 0 && (
        <ColumnDifferencesSection differences={column_differences} />
      )}
      
      <ExceptAllDiffSection title="In Source, Not in Target" data={in_source_not_target} tableName={sourceTable} defaultExpanded={true} />
      <ExceptAllDiffSection title="In Target, Not in Source" data={in_target_not_source} tableName={targetTable} defaultExpanded={in_source_not_target?.count === 0} />
    </div>
  );
}
