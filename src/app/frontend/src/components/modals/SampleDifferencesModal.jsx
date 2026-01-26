import React, { useState } from 'react';
import { diffChars } from 'diff';

const TRUNCATE_LENGTH = 50;
const MAX_CHANGE_RATIO = 0.4; // If >40% of non-whitespace chars changed, show compact view

/**
 * Strip all whitespace from string for comparison
 */
function stripWhitespace(str) {
  return str.replace(/\s+/g, '');
}

/**
 * Make invisible characters visible - spaces kept as-is (shown via highlight)
 */
function makeVisible(str) {
  return str
    .replace(/\t/g, '⇥')        // tab symbol
    .replace(/\n/g, '↵\n')
    .replace(/\u00A0/g, ' ')    // non-breaking space → regular space
    .replace(/\u200B/g, '');    // zero-width space → remove
}

/**
 * Render diff with highlighting - guarantees ALL differences are shown
 * - Whitespace-only differences always use char diff
 * - Very different content uses compact view
 */
function DiffHighlight({ source, target }) {
  const srcIsNull = source === null || source === undefined;
  const tgtIsNull = target === null || target === undefined;
  
  // Both null
  if (srcIsNull && tgtIsNull) {
    return <span className="text-gray-500 italic">null</span>;
  }
  
  // Null vs non-null
  if (srcIsNull || tgtIsNull) {
    return (
      <span className="whitespace-pre-wrap">
        {srcIsNull && <span className="bg-red-900/60 text-red-300 line-through opacity-70 rounded-sm px-0.5 italic mr-1">null</span>}
        {!srcIsNull && <span className="bg-red-900/60 text-red-300 line-through opacity-70 rounded-sm px-0.5 mr-1">{makeVisible(String(source))}</span>}
        {tgtIsNull ? (
          <span className="bg-green-800/60 text-green-200 rounded-sm px-0.5 italic">null</span>
        ) : (
          <span className="bg-green-800/60 text-green-200 rounded-sm px-0.5">{makeVisible(String(target))}</span>
        )}
      </span>
    );
  }
  
  const srcStr = String(source);
  const tgtStr = String(target);
  
  // Identical
  if (srcStr === tgtStr) {
    return <span className="whitespace-pre-wrap">{tgtStr || <span className="text-gray-500 italic">empty</span>}</span>;
  }
  
  // Empty vs non-empty
  if (srcStr === '' || tgtStr === '') {
    return (
      <span className="whitespace-pre-wrap">
        {srcStr !== '' && <span className="bg-red-900/60 text-red-300 line-through opacity-70 rounded-sm px-0.5 mr-1">{makeVisible(srcStr)}</span>}
        {srcStr === '' && <span className="bg-red-900/60 text-red-300 line-through opacity-70 rounded-sm px-0.5 italic mr-1">empty</span>}
        {tgtStr === '' ? (
          <span className="bg-green-800/60 text-green-200 rounded-sm px-0.5 italic">empty</span>
        ) : (
          <span className="bg-green-800/60 text-green-200 rounded-sm px-0.5">{makeVisible(tgtStr)}</span>
        )}
      </span>
    );
  }
  
  // Check if difference is whitespace-only by comparing stripped versions
  const srcStripped = stripWhitespace(srcStr);
  const tgtStripped = stripWhitespace(tgtStr);
  const whitespaceOnly = srcStripped === tgtStripped;
  
  // Character-level diff
  const diff = diffChars(srcStr, tgtStr);
  
  // If whitespace-only difference, always show char diff
  if (whitespaceOnly) {
    return (
      <span className="whitespace-pre-wrap">
        {diff.map((part, i) => {
          if (part.added) {
            return <span key={i} className="bg-green-800/60 text-green-200 rounded-sm px-0.5">{makeVisible(part.value)}</span>;
          }
          if (part.removed) {
            return <span key={i} className="bg-red-900/60 text-red-300 line-through opacity-70 rounded-sm px-0.5">{makeVisible(part.value)}</span>;
          }
          return <span key={i}>{part.value}</span>;
        })}
      </span>
    );
  }
  
  // Calculate what % of non-whitespace characters are changed
  let changedChars = 0;
  for (const part of diff) {
    if (part.added || part.removed) {
      changedChars += stripWhitespace(part.value).length;
    }
  }
  const totalNonWsChars = srcStripped.length + tgtStripped.length;
  const changeRatio = totalNonWsChars > 0 ? changedChars / totalNonWsChars : 1;
  
  // Too different - show compact view
  if (changeRatio > MAX_CHANGE_RATIO) {
    return (
      <span 
        className="whitespace-pre-wrap bg-yellow-800/50 text-yellow-100 rounded-sm px-0.5 cursor-help"
        title={`Source: ${srcStr}`}
      >
        {tgtStr}
      </span>
    );
  }
  
  // Similar enough - show inline diff with visible whitespace
  return (
    <span className="whitespace-pre-wrap">
      {diff.map((part, i) => {
        if (part.added) {
          return <span key={i} className="bg-green-800/60 text-green-200 rounded-sm px-0.5">{makeVisible(part.value)}</span>;
        }
        if (part.removed) {
          return <span key={i} className="bg-red-900/60 text-red-300 line-through opacity-70 rounded-sm px-0.5">{makeVisible(part.value)}</span>;
        }
        return <span key={i}>{part.value}</span>;
      })}
    </span>
  );
}

/**
 * Generate a SQL SELECT query for a row
 */
function generateSqlQuery(tableName, row, columnsToUse = null) {
  const rowKeys = Object.keys(row);
  const cols = columnsToUse || rowKeys;
  
  const conditions = cols.map(col => {
    // Case-insensitive key lookup
    const actualKey = rowKeys.find(k => k.toLowerCase() === col.toLowerCase()) || col;
    const value = row[actualKey];
    if (value === null || value === undefined) {
      return `${col} IS NULL`;
    }
    if (typeof value === 'number') {
      return `${col} = ${value}`;
    }
    if (typeof value === 'boolean') {
      return `${col} = ${value}`;
    }
    // String - escape single quotes
    const escaped = String(value).replace(/'/g, "''");
    return `${col} = '${escaped}'`;
  }).join('\n  AND ');
  
  return `SELECT * FROM ${tableName}\nWHERE ${conditions};`;
}

/**
 * Button to copy SQL query for a row
 */
function CopySqlButton({ tableName, row, columnsToUse = null }) {
  const [copied, setCopied] = useState(false);
  
  const handleCopy = async (e) => {
    e.stopPropagation();
    const sql = generateSqlQuery(tableName, row, columnsToUse);
    try {
      await navigator.clipboard.writeText(sql);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  };
  
  return (
    <button
      onClick={handleCopy}
      className={`text-xs py-0.5 rounded border transition-all w-[3.25rem] text-center ${
        copied 
          ? 'bg-green-900/50 border-green-600 text-green-300' 
          : 'bg-charcoal-600 border-charcoal-300 text-gray-400 hover:text-gray-200 hover:border-gray-400'
      }`}
      title="Copy SELECT query to clipboard"
    >
      {copied ? 'Copied' : 'SQL'}
    </button>
  );
}

/**
 * Expandable cell for long values - truncates by default, click to expand
 */
function ExpandableCell({ value, className = '' }) {
  const [expanded, setExpanded] = useState(false);
  
  if (value === null || value === undefined) {
    return <span className="text-gray-500 italic">null</span>;
  }
  
  const strValue = String(value);
  const needsTruncation = strValue.length > TRUNCATE_LENGTH;
  
  if (!needsTruncation) {
    // whitespace-pre-wrap preserves spaces/tabs so whitespace diffs are visible
    return <span className={`whitespace-pre-wrap ${className}`}>{strValue}</span>;
  }
  
  return (
    <span 
      className={`cursor-pointer hover:bg-charcoal-300/30 rounded px-0.5 -mx-0.5 whitespace-pre-wrap ${className}`}
      onClick={(e) => { e.stopPropagation(); setExpanded(!expanded); }}
      title={expanded ? 'Click to collapse' : 'Click to expand'}
    >
      {expanded ? strValue : `${strValue.slice(0, TRUNCATE_LENGTH)}…`}
      {!expanded && (
        <span className="text-xs text-gray-500 ml-1">+{strValue.length - TRUNCATE_LENGTH}</span>
      )}
    </span>
  );
}

/**
 * Modal to display sample differences from validation results.
 * Handles both except_all and primary_key comparison modes.
 */
export function SampleDifferencesModal({ validation, onClose }) {
  const [maximized, setMaximized] = useState(false);
  
  if (!validation) return null;

  // Parse sample_differences if it's a JSON string (from PostgreSQL JSONB)
  let samples = validation.sample_differences;
  if (typeof samples === 'string') {
    try {
      samples = JSON.parse(samples);
    } catch (e) {
      console.error('Failed to parse sample_differences:', e);
      samples = null;
    }
  }
  
  const isPKMode = samples?.mode === 'primary_key';
  const isRowCountMismatch = samples?.mode === 'row_count_mismatch';
  const isExceptAllMode = Array.isArray(samples) && samples.length > 0;
  const isPKPending = validation.compare_mode === 'primary_key' && isExceptAllMode;
  const isExceptAllCountMismatch = validation.compare_mode === 'except_all' && !validation.row_count_match && !isExceptAllMode;
  const isPKCountPending = validation.compare_mode === 'primary_key' && !validation.row_count_match && !isRowCountMismatch;

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div 
        className={`bg-charcoal-500 border border-charcoal-200 rounded-lg p-6 overflow-hidden flex flex-col transition-all ${
          maximized 
            ? 'w-full h-full max-w-none max-h-none m-0 rounded-none' 
            : 'max-w-6xl w-full mx-4 max-h-[90vh]'
        }`}
        onClick={(e) => e.stopPropagation()}
      >
        
        {/* Header */}
        <div className="flex justify-between items-start mb-3">
          <div>
            <h3 className="text-lg font-bold text-rust-light">
              Sample Differences
            </h3>
            <p className="text-gray-400 text-xs mt-1">
              {validation.entity_name} • {validation.compare_mode} mode
              {validation.rows_different != null && ` • ${validation.rows_different.toLocaleString()} total differences`}
              {!validation.row_count_match && ' • Row count mismatch'}
            </p>
          </div>
          <div className="flex items-center gap-1">
            <button 
              onClick={(e) => { e.stopPropagation(); setMaximized(!maximized); }}
              className="text-gray-400 hover:text-gray-200 leading-none px-2 py-1"
              title={maximized ? 'Restore' : 'Maximize'}
            >
              {maximized ? (
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <rect x="8" y="8" width="12" height="12" strokeWidth="2" rx="1" />
                  <path d="M4 16V5a1 1 0 011-1h11" strokeWidth="2" />
                </svg>
              ) : (
                <svg className="w-5 h-5" fill="none" stroke="currentColor" strokeWidth="2" viewBox="0 0 24 24">
                  <path d="M4 14v6h6M20 10V4h-6M4 20L10 14M20 4L14 10" />
                </svg>
              )}
            </button>
            <button 
              onClick={onClose}
              className="text-gray-400 hover:text-gray-200 text-2xl leading-none px-2"
            >
              ×
            </button>
          </div>
        </div>
        
        {/* Content - Different for each mode */}
        <div className="flex-1 overflow-auto">
          {isPKMode && <PKModeView samples={samples} validation={validation} />}
          {isRowCountMismatch && <RowCountMismatchView samples={samples} validation={validation} />}
          {isPKPending && <PKPendingView samples={samples} validation={validation} />}
          {isExceptAllMode && !isPKPending && <ExceptAllModeView samples={samples} validation={validation} />}
          {isExceptAllCountMismatch && (
            <div className="p-4 bg-charcoal-400 border border-charcoal-300 rounded-lg">
              <p className="text-gray-300 mb-2">
                <span className="font-semibold">Row count mismatch detected</span>
              </p>
              <p className="text-gray-400 text-sm">
                Source: {validation.row_count_source?.toLocaleString()} rows • Target: {validation.row_count_target?.toLocaleString()} rows
              </p>
              <p className="text-gray-500 text-sm mt-3">
                Detailed row analysis is not available for <span className="font-mono text-gray-400">except_all</span> mode 
                because there are no primary keys defined to identify which specific rows are missing or extra.
              </p>
            </div>
          )}
          {isPKCountPending && (
            <div className="p-4 bg-charcoal-400 border border-charcoal-300 rounded-lg">
              <p className="text-gray-300 mb-2">
                <span className="font-semibold">Row count mismatch detected</span>
              </p>
              <p className="text-gray-400 text-sm">
                Source: {validation.row_count_source?.toLocaleString()} rows • Target: {validation.row_count_target?.toLocaleString()} rows
              </p>
              <p className="text-gray-500 text-sm mt-3">
                Analysis data may still be processing. Check the validation notebook for more details.
              </p>
            </div>
          )}
          {!isPKMode && !isRowCountMismatch && !isExceptAllMode && !isExceptAllCountMismatch && !isPKCountPending && (
            <p className="text-gray-400">No sample data available</p>
          )}
        </div>
        
        {/* Footer */}
        <div className="mt-4 pt-4 border-t border-charcoal-300 flex justify-end">
          <button
            onClick={onClose}
            className="px-4 py-2 bg-charcoal-600 text-gray-300 border border-charcoal-300 rounded hover:bg-charcoal-500 transition-all"
          >
            Close
          </button>
        </div>
        
      </div>
    </div>
  );
}

/**
 * Display for primary_key mode when PK analysis hasn't completed yet
 */
function PKPendingView({ samples, validation }) {
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

/**
 * Display for except_all mode - shows full mismatched rows
 */
function ExceptAllModeView({ samples, validation }) {
  if (!samples || samples.length === 0) {
    return <p className="text-gray-400">No sample data available</p>;
  }
  
  // Get all column names from first row
  const columns = Object.keys(samples[0]);
  const tableName = validation?.source_table || 'TABLE_NAME';
  
  return (
    <div>
      <div className="mb-3 p-2 bg-blue-900/20 border border-blue-700 rounded">
        <p className="text-blue-300 text-xs">
          <strong>Showing {samples.length} sample rows from source</strong> that don't have an exact match in target. 
          These are complete rows returned by the EXCEPT ALL operation.
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
                  <CopySqlButton tableName={tableName} row={row} />
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

/**
 * Display for primary_key mode - shows side-by-side comparison grouped by PK
 */
/**
 * Transposed summary table - columns as headers, stats as rows
 */
function SummaryTable({ summary, isPK }) {
  if (!summary || summary.length === 0) return null;
  
  // Fallback if isPK not provided
  const checkPK = isPK || (() => false);
  
  // Separate PK and non-PK columns, PKs first then preserve order for rest
  const pkSummary = summary.filter(s => checkPK(s.name));
  const nonPkSummary = summary.filter(s => !checkPK(s.name));
  const displaySummary = [...pkSummary, ...nonPkSummary].map(s => ({ ...s, is_pk: checkPK(s.name) }));
  
  if (displaySummary.length === 0) return null;
  
  const formatRangeOrCard = (col) => {
    if (col.type === 'string') {
      return `${col.cardinality} unique`;
    }
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
                  col.is_pk 
                    ? 'bg-purple-900/30 text-purple-300' 
                    : 'text-gray-300'
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
              <td 
                key={col.name}
                className={`px-3 py-1 border-l border-charcoal-300 ${
                  col.is_pk ? 'bg-purple-900/10' : ''
                } ${col.is_pk && idx === pkSummary.length - 1 ? 'border-r-2 border-r-purple-500' : ''}`}
              >
                <span className="text-gray-400">{col.type}</span>
              </td>
            ))}
          </tr>
          <tr className="border-t border-charcoal-300">
            <td className="px-2 py-1 text-gray-400 sticky left-0 bg-charcoal-500 z-10">Range</td>
            {displaySummary.map((col, idx) => (
              <td 
                key={col.name}
                className={`px-3 py-1 border-l border-charcoal-300 font-mono text-gray-200 ${
                  col.is_pk ? 'bg-purple-900/10' : ''
                } ${col.is_pk && idx === pkSummary.length - 1 ? 'border-r-2 border-r-purple-500' : ''}`}
              >
                {formatRangeOrCard(col)}
              </td>
            ))}
          </tr>
          <tr className="border-t border-charcoal-300">
            <td className="px-2 py-1 text-gray-400 sticky left-0 bg-charcoal-500 z-10">Nulls</td>
            {displaySummary.map((col, idx) => (
              <td 
                key={col.name}
                className={`px-3 py-1 border-l border-charcoal-300 ${
                  col.is_pk ? 'bg-purple-900/10' : ''
                } ${col.is_pk && idx === pkSummary.length - 1 ? 'border-r-2 border-r-purple-500' : ''} ${
                  col.nulls > 0 ? 'text-yellow-400' : 'text-gray-400'
                }`}
              >
                {col.nulls}
              </td>
            ))}
          </tr>
        </tbody>
      </table>
    </div>
  );
}

/**
 * Collapsible section for missing rows (missing_in_target or missing_in_source)
 */
function MissingRowsSection({ title, data, tableName, pkColumns = [], defaultExpanded = false }) {
  const [expanded, setExpanded] = useState(defaultExpanded);
  
  if (!data) return null;
  
  const { count, summary, samples } = data;
  
  // Case-insensitive PK matching
  const pkLower = new Set((pkColumns || []).map(pk => pk.toLowerCase()));
  const isPK = (col) => pkLower.has((col || '').toLowerCase());
  
  // Reorder columns: PKs first, then rest in original order
  const allColumns = samples?.[0] ? Object.keys(samples[0]) : [];
  const pkCols = allColumns.filter(isPK);
  const nonPkCols = allColumns.filter(c => !isPK(c));
  const columns = [...pkCols, ...nonPkCols];
  
  return (
    <div className="border border-charcoal-300 rounded-lg overflow-hidden">
      {/* Header */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-3 py-2 bg-charcoal-400 flex items-center justify-between hover:bg-charcoal-400/80 transition-colors"
      >
        <span className="text-sm font-semibold text-gray-200">
          {expanded ? '▼' : '▶'} {title} ({count.toLocaleString()} rows)
        </span>
      </button>
      
      {expanded && (
        <div className="p-3 space-y-3">
          {/* Summary */}
          {summary && summary.length > 0 && (
            <div>
              <p className="text-xs text-gray-400 mb-2">Summary (PK columns highlighted, scroll for more)</p>
              <SummaryTable summary={summary} isPK={isPK} />
            </div>
          )}
          
          {/* Samples */}
          {samples && samples.length > 0 && (
            <div>
              <p className="text-xs text-gray-400 mb-2">Samples ({samples.length} of {count.toLocaleString()})</p>
              <div className="overflow-x-auto">
                <table className="w-full border border-charcoal-300 rounded text-xs">
                  <thead className="bg-charcoal-400">
                    <tr>
                      <th className="px-2 py-1.5 w-14 border-r border-charcoal-300"></th>
                      {columns.map(col => (
                        <th 
                          key={col} 
                          className={`px-2 py-1.5 text-left font-semibold border-r border-charcoal-300 last:border-r-0 whitespace-nowrap ${
                            isPK(col) ? 'text-purple-300 bg-purple-900/20' : 'text-gray-300'
                          }`}
                        >
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {samples.map((row, idx) => (
                      <tr key={idx} className="border-t border-charcoal-300 hover:bg-charcoal-400/50">
                        <td className="px-2 py-1.5 border-r border-charcoal-300 text-center">
                          <CopySqlButton tableName={tableName} row={row} columnsToUse={pkColumns} />
                        </td>
                        {columns.map(col => (
                          <td 
                            key={col} 
                            className={`px-2 py-1.5 font-mono border-r border-charcoal-300 last:border-r-0 ${
                              isPK(col) ? 'bg-purple-900/10 text-purple-200' : 'text-gray-200'
                            }`}
                          >
                            <ExpandableCell value={row[col]} />
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
          
          {(!samples || samples.length === 0) && (
            <p className="text-gray-500 text-sm italic">No rows in this category</p>
          )}
        </div>
      )}
    </div>
  );
}

/**
 * Display for row count mismatch - shows missing in target and missing in source
 */
function RowCountMismatchView({ samples, validation }) {
  if (samples.skipped) {
    return (
      <div className="p-4 bg-yellow-900/20 border border-yellow-700 rounded">
        <p className="text-yellow-300 text-sm font-semibold">Analysis Skipped</p>
        <p className="text-yellow-200 text-xs mt-2">
          Source data was limited and source count ({validation.row_count_source?.toLocaleString()}) is less than 
          target count ({validation.row_count_target?.toLocaleString()}). Results would be unreliable.
        </p>
        {validation?.databricks_run_url && (
          <a 
            href={validation.databricks_run_url} 
            target="_blank" 
            rel="noopener noreferrer"
            className="inline-block mt-2 text-xs text-blue-400 hover:text-blue-300 underline"
          >
            View Notebook Run for details
          </a>
        )}
      </div>
    );
  }
  
  const { missing_in_target, missing_in_source } = samples;
  // Try validation.pk_columns first, fallback to samples.pk_columns if stored there
  const pkColumns = validation.pk_columns || samples.pk_columns || [];
  const sourceTable = validation.source_table || 'SOURCE_TABLE';
  const targetTable = validation.target_table || 'TARGET_TABLE';
  
  return (
    <div className="space-y-3">
      {/* Header info */}
      <div className="p-2 bg-orange-900/20 border border-orange-700 rounded">
        <p className="text-orange-300 text-xs">
          Row count mismatch: Source has {validation.row_count_source?.toLocaleString()} rows, 
          Target has {validation.row_count_target?.toLocaleString()} rows 
          (diff: {Math.abs((validation.row_count_source || 0) - (validation.row_count_target || 0)).toLocaleString()})
        </p>
      </div>
      
      {/* Missing in Target */}
      <MissingRowsSection
        title="Missing in Target"
        data={missing_in_target}
        tableName={sourceTable}
        pkColumns={pkColumns}
        defaultExpanded={true}
      />
      
      {/* Missing in Source (Extra in Target) */}
      <MissingRowsSection
        title="Missing in Source (Extra in Target)"
        data={missing_in_source}
        tableName={targetTable}
        pkColumns={pkColumns}
        defaultExpanded={missing_in_target?.count === 0}
      />
    </div>
  );
}

function PKModeView({ samples, validation }) {
  if (!samples?.samples || samples.samples.length === 0) {
    return (
      <div className="p-3 bg-yellow-900/20 border border-yellow-700 rounded">
        <p className="text-yellow-300 text-sm">
          <strong>No column-level differences found.</strong> PK analysis completed but no differing columns were detected.
        </p>
        <p className="text-yellow-200 text-xs mt-2">
          This may indicate the differences are in excluded columns or due to data type coercion. 
          Check the Databricks notebook run for detailed investigation.
        </p>
        {validation?.databricks_run_url && (
          <a 
            href={validation.databricks_run_url} 
            target="_blank" 
            rel="noopener noreferrer"
            className="inline-block mt-2 text-xs text-blue-400 hover:text-blue-300 underline"
          >
            Open Notebook Run →
          </a>
        )}
      </div>
    );
  }
  
  const { pk_columns, samples: pkSamples } = samples;
  
  return (
    <div className="space-y-3">
      <div className="p-2 bg-purple-900/20 border border-purple-700 rounded">
        <p className="text-purple-300 text-xs">
          <strong>Showing {pkSamples.length} records</strong> where row values differ between source and target. 
          Primary keys: <span className="font-mono text-rust-light">{pk_columns.join(', ')}</span>
        </p>
      </div>
      
      {pkSamples.map((sample, idx) => (
        <div key={idx} className="border border-charcoal-300 rounded-lg overflow-hidden">
          
          {/* PK Header */}
          <div className="bg-charcoal-400 px-3 py-2 border-b border-charcoal-300">
            <span className="text-gray-300 font-semibold text-xs">Record #{idx + 1} — </span>
            {Object.entries(sample.pk).map(([key, value], pkIdx) => (
              <span key={key}>
                <span className="text-gray-400 text-xs">{key}:</span>
                <span className="text-rust-light font-mono text-xs ml-1 mr-3">{value !== null ? String(value) : 'null'}</span>
                {pkIdx < Object.keys(sample.pk).length - 1 && <span className="text-gray-600">•</span>}
              </span>
            ))}
          </div>
          
          {/* Differences Table */}
          <table className="w-full">
            <thead className="bg-charcoal-400/50">
              <tr>
                <th className="px-3 py-1.5 text-left text-xs text-gray-300 w-1/4">Column</th>
                <th className="px-3 py-1.5 text-left text-xs text-blue-300 w-3/8">
                  <span className="flex items-center gap-1.5">
                    <span className="inline-block w-2 h-2 bg-blue-500 rounded"></span>
                    Source Value
                  </span>
                </th>
                <th className="px-3 py-1.5 text-left text-xs text-purple-300 w-3/8">
                  <span className="flex items-center gap-1.5">
                    <span className="inline-block w-2 h-2 bg-purple-500 rounded"></span>
                    Target Value
                  </span>
                </th>
              </tr>
            </thead>
            <tbody>
              {sample.differences.map((diff, diffIdx) => (
                <tr key={diffIdx} className="border-t border-charcoal-300">
                  <td className="px-3 py-1.5 text-xs font-mono text-gray-300 align-top">
                    {diff.column}
                  </td>
                  <td className="px-3 py-1.5 text-xs text-blue-200 bg-blue-900/10 align-top">
                    <div className="font-mono">
                      <ExpandableCell value={diff.source_value} />
                    </div>
                  </td>
                  <td className="px-3 py-1.5 text-xs text-purple-200 bg-purple-900/10 align-top">
                    <div className="font-mono">
                      <DiffHighlight source={diff.source_value} target={diff.target_value} />
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          
        </div>
      ))}
    </div>
  );
}
