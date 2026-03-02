import React from 'react';

export function SummaryTable({ summary, isPK }) {
  if (!summary || summary.length === 0) return null;
  
  const checkPK = isPK || (() => false);
  const pkSummary = summary.filter(s => checkPK(s.name));
  const nonPkSummary = summary.filter(s => !checkPK(s.name));
  const displaySummary = [...pkSummary, ...nonPkSummary].map(s => ({ ...s, is_pk: checkPK(s.name) }));
  
  if (displaySummary.length === 0) return null;
  
  const formatRangeOrCard = (col) => {
    if (col.type === 'string') return `${col.cardinality} unique`;
    if (col.min === null && col.max === null) return '-';
    const minStr = col.min != null ? String(col.min).slice(0, 16) : '?';
    const maxStr = col.max != null ? String(col.max).slice(0, 16) : '?';
    return minStr === maxStr ? minStr : `${minStr} → ${maxStr}`;
  };
  
  return (
    <div className="overflow-x-auto">
      <table className="border border-charcoal-300 rounded text-xs">
        <thead className="bg-charcoal-400">
          <tr>
            <th className="px-2 py-1.5 text-left text-gray-400 font-normal sticky left-0 bg-charcoal-400 z-10 min-w-[70px]"></th>
            {displaySummary.map((col, idx) => (
              <th 
                key={col.name}
                className={`px-3 py-1.5 text-left font-semibold whitespace-nowrap border-l border-charcoal-300 ${
                  col.is_pk ? 'bg-purple-900/30 text-purple-300' : 'text-gray-300'
                } ${col.is_pk && idx === pkSummary.length - 1 ? 'border-r-2 border-r-purple-500' : ''}`}
              >
                {col.name}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          <tr className="border-t border-charcoal-300">
            <td className="px-2 py-1 text-gray-400 sticky left-0 bg-charcoal-500 z-10">Type</td>
            {displaySummary.map((col, idx) => (
              <td key={col.name} className={`px-3 py-1 border-l border-charcoal-300 ${col.is_pk ? 'bg-purple-900/10' : ''} ${col.is_pk && idx === pkSummary.length - 1 ? 'border-r-2 border-r-purple-500' : ''}`}>
                <span className="text-gray-400">{col.type}</span>
              </td>
            ))}
          </tr>
          <tr className="border-t border-charcoal-300">
            <td className="px-2 py-1 text-gray-400 sticky left-0 bg-charcoal-500 z-10">Range</td>
            {displaySummary.map((col, idx) => (
              <td key={col.name} className={`px-3 py-1 border-l border-charcoal-300 font-mono text-gray-200 ${col.is_pk ? 'bg-purple-900/10' : ''} ${col.is_pk && idx === pkSummary.length - 1 ? 'border-r-2 border-r-purple-500' : ''}`}>
                {formatRangeOrCard(col)}
              </td>
            ))}
          </tr>
          <tr className="border-t border-charcoal-300">
            <td className="px-2 py-1 text-gray-400 sticky left-0 bg-charcoal-500 z-10">Nulls</td>
            {displaySummary.map((col, idx) => (
              <td key={col.name} className={`px-3 py-1 border-l border-charcoal-300 ${col.is_pk ? 'bg-purple-900/10' : ''} ${col.is_pk && idx === pkSummary.length - 1 ? 'border-r-2 border-r-purple-500' : ''} ${col.nulls > 0 ? 'text-yellow-400' : 'text-gray-400'}`}>
                {col.nulls}
              </td>
            ))}
          </tr>
        </tbody>
      </table>
    </div>
  );
}
