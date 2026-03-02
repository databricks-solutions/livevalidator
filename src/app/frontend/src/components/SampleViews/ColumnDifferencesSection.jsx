import React, { useState } from 'react';

function formatColStats(col) {
  if (!col) return '-';
  const parts = [];
  if (col.type === 'string' && col.cardinality != null) {
    parts.push(`${col.cardinality} unique`);
  } else if ((col.type === 'numeric' || col.type === 'time') && (col.min != null || col.max != null)) {
    const minStr = col.min != null ? String(col.min).slice(0, 16) : '?';
    const maxStr = col.max != null ? String(col.max).slice(0, 16) : '?';
    parts.push(minStr === maxStr ? minStr : `${minStr} → ${maxStr}`);
  }
  if (col.nulls != null && col.nulls > 0) {
    parts.push(`${col.nulls} nulls`);
  }
  return parts.length > 0 ? parts.join(', ') : '-';
}

export function ColumnDifferencesSection({ differences }) {
  const [expanded, setExpanded] = useState(true);
  
  if (!differences || differences.length === 0) return null;
  
  return (
    <div className="border border-charcoal-300 rounded-lg overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-3 py-2 bg-charcoal-400 flex items-center justify-between hover:bg-charcoal-400/80 transition-colors"
      >
        <span className="text-sm font-semibold text-gray-200">
          {expanded ? '▼' : '▶'} Column Differences ({differences.length} columns differ)
        </span>
      </button>
      
      {expanded && (
        <div className="p-3">
          <p className="text-xs text-gray-400 mb-2">Comparing stats between "source-not-in-target" vs "target-not-in-source" rows</p>
          <div className="overflow-x-auto">
            <table className="w-full border border-charcoal-300 rounded text-xs">
              <thead className="bg-charcoal-400">
                <tr>
                  <th className="px-3 py-1.5 text-left text-gray-300 font-semibold">Column</th>
                  <th className="px-3 py-1.5 text-left text-gray-300 font-semibold">Type</th>
                  <th className="px-3 py-1.5 text-left text-blue-300 font-semibold">Source Stats</th>
                  <th className="px-3 py-1.5 text-left text-purple-300 font-semibold">Target Stats</th>
                </tr>
              </thead>
              <tbody>
                {differences.map((diff, idx) => (
                  <tr key={idx} className="border-t border-charcoal-300">
                    <td className="px-3 py-1.5 font-mono text-gray-200">{diff.column}</td>
                    <td className="px-3 py-1.5 text-gray-400">
                      {diff.difference_type === 'type_mismatch' 
                        ? <span className="text-yellow-400">{diff.source?.type} → {diff.target?.type}</span>
                        : diff.source?.type || diff.target?.type || '-'}
                    </td>
                    <td className="px-3 py-1.5 text-blue-200 bg-blue-900/10">
                      {diff.source ? formatColStats(diff.source) : <span className="text-gray-500 italic">missing</span>}
                    </td>
                    <td className="px-3 py-1.5 text-purple-200 bg-purple-900/10">
                      {diff.target ? formatColStats(diff.target) : <span className="text-gray-500 italic">missing</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
