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
      <div className="p-3 bg-orange-900/20 border border-orange-700 rounded">
        <p className="text-orange-300 text-xs font-semibold mb-2">Row count mismatch:</p>
        <table className="text-xs w-auto border-collapse">
          <thead>
            <tr className="text-gray-400">
              <th className="text-left pr-6 pb-1"></th>
              <th className="text-right pr-6 pb-1">Source</th>
              <th className="text-right pr-6 pb-1">Target</th>
              <th className="text-right pb-1">Diffs</th>
            </tr>
          </thead>
          <tbody>
            <tr className="text-orange-200">
              <td className="pr-6 py-0.5 font-medium">Total Rows</td>
              <td className="text-right pr-6">{validation.row_count_source?.toLocaleString()}</td>
              <td className="text-right pr-6">{validation.row_count_target?.toLocaleString()}</td>
              <td className="text-right">{Math.abs((validation.row_count_source || 0) - (validation.row_count_target || 0)).toLocaleString()}</td>
            </tr>
            {samples.unique_count_source != null && (
              <tr className="text-orange-200/80">
                <td className="pr-6 py-0.5 font-medium">Unique Rows</td>
                <td className="text-right pr-6">{samples.unique_count_source?.toLocaleString()}</td>
                <td className="text-right pr-6">{samples.unique_count_target?.toLocaleString()}</td>
                <td className="text-right">{Math.abs((samples.unique_count_source || 0) - (samples.unique_count_target || 0)).toLocaleString()}</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      
      <MissingRowsSection title="Missing in Target" data={missing_in_target} tableName={sourceTable} pkColumns={pkColumns} defaultExpanded={true} />
      <MissingRowsSection title="Missing in Source (Extra in Target)" data={missing_in_source} tableName={targetTable} pkColumns={pkColumns} defaultExpanded={missing_in_target?.count === 0} />
    </div>
  );
}
