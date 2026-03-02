import React, { useState } from 'react';
import { CopySqlButton, ExpandableCell } from '../DiffComponents';

export function ExceptAllDiffSection({ title, data, tableName, defaultExpanded = false }) {
  const [expanded, setExpanded] = useState(defaultExpanded);
  
  if (!data) return null;
  
  const { count, samples } = data;
  const columns = samples?.[0] ? Object.keys(samples[0]) : [];
  
  return (
    <div className="border border-charcoal-300 rounded-lg overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-3 py-2 bg-charcoal-400 flex items-center justify-between hover:bg-charcoal-400/80 transition-colors"
      >
        <span className="text-sm font-semibold text-gray-200">
          {expanded ? '▼' : '▶'} {title} ({count.toLocaleString()} rows)
        </span>
      </button>
      
      {expanded && (
        <div className="p-3">
          {samples && samples.length > 0 ? (
            <>
              <p className="text-xs text-gray-400 mb-2">Samples ({samples.length} of {count.toLocaleString()})</p>
              <div className="overflow-x-auto">
                <table className="w-full border border-charcoal-300 rounded text-xs">
                  <thead className="bg-charcoal-400">
                    <tr>
                      <th className="px-2 py-1.5 w-14 border-r border-charcoal-300"></th>
                      {columns.map(col => (
                        <th key={col} className="px-2 py-1.5 text-left font-semibold text-gray-300 border-r border-charcoal-300 last:border-r-0 whitespace-nowrap">
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {samples.map((row, idx) => (
                      <tr key={idx} className="border-t border-charcoal-300 hover:bg-charcoal-400/50">
                        <td className="px-2 py-1.5 border-r border-charcoal-300 text-center">
                          <CopySqlButton tableName={tableName} row={row} />
                        </td>
                        {columns.map(col => (
                          <td key={col} className="px-2 py-1.5 font-mono text-gray-200 border-r border-charcoal-300 last:border-r-0">
                            <ExpandableCell value={row[col]} />
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          ) : (
            <p className="text-gray-500 text-sm italic">No rows in this category</p>
          )}
        </div>
      )}
    </div>
  );
}
