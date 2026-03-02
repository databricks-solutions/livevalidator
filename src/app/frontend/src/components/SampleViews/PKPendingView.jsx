import React from 'react';
import { CopySqlButton, ExpandableCell } from '../DiffComponents';

export function PKPendingView({ samples, validation }) {
  if (!samples || samples.length === 0) {
    return <p className="text-gray-400">No sample data available</p>;
  }
  
  const columns = Object.keys(samples[0]);
  const tableName = validation?.source_table || 'TABLE_NAME';
  const pkColumns = validation?.pk_columns || [];
  
  return (
    <div>
      <div className="mb-3 p-2 bg-yellow-900/20 border border-yellow-700 rounded">
        <p className="text-yellow-300 text-xs">
          Showing sample primary keys with mismatches. Full column-level analysis may follow.
        </p>
      </div>
      
      <div className="overflow-x-auto">
        <table className="w-full border border-charcoal-300 rounded">
          <thead className="bg-charcoal-400 sticky top-0">
            <tr>
              <th className="px-2 py-1.5 w-14 border-r border-charcoal-300"></th>
              {columns.map(col => (
                <th key={col} className="px-2 py-1.5 text-left text-xs font-semibold text-gray-300 border-r border-charcoal-300 last:border-r-0">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {samples.map((row, idx) => (
              <tr key={idx} className="border-t border-charcoal-300 hover:bg-charcoal-400/50">
                <td className="px-2 py-1.5 border-r border-charcoal-300 text-center">
                  <CopySqlButton tableName={tableName} row={row} columnsToUse={pkColumns.length > 0 ? pkColumns : null} />
                </td>
                {columns.map(col => (
                  <td key={col} className="px-2 py-1.5 text-xs text-gray-200 font-mono border-r border-charcoal-300 last:border-r-0">
                    <ExpandableCell value={row[col]} />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
