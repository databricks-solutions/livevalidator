import React, { useState, useRef } from 'react';
import { TagList } from './TagBadge';
import { Checkbox } from './Checkbox';
import { parseArray } from '../utils/arrays';

// Error popover component
function ErrorPopover({ error, onClose }) {
  const [copied, setCopied] = useState(false);
  const ref = useRef(null);

  React.useEffect(() => {
    const handleClick = (e) => { if (ref.current && !ref.current.contains(e.target)) onClose(); };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [onClose]);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(error || 'Unknown error');
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  };

  return (
    <div ref={ref} className="fixed z-50 bg-charcoal-500 rounded-lg shadow-xl border border-orange-700/50 p-3 w-[500px] h-[300px] flex flex-col" style={{ top: '50%', left: '50%', transform: 'translate(-50%, -50%)' }}>
      <pre className="flex-1 text-sm text-gray-200 whitespace-pre-wrap break-words overflow-y-auto select-all m-0 mb-2 pr-2">
        {error || 'Unknown error'}
      </pre>
      <div className="flex gap-1.5 justify-end shrink-0">
        <button onClick={handleCopy} className="px-2 py-1 text-xs rounded bg-orange-600/80 text-white hover:bg-orange-500 transition-all">
          {copied ? '✓ Copied' : 'Copy'}
        </button>
        <button onClick={onClose} className="px-2 py-1 text-xs rounded bg-charcoal-300 text-gray-300 hover:bg-charcoal-200 hover:text-white transition-all">
          Close
        </button>
      </div>
    </div>
  );
}

// Copy icon SVG component
const CopyIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" fill="none" viewBox="0 0 16 16" aria-hidden="true">
    <path fill="currentColor" fillRule="evenodd" d="M1.75 1a.75.75 0 0 0-.75.75v8.5c0 .414.336.75.75.75H5v3.25c0 .414.336.75.75.75h8.5a.75.75 0 0 0 .75-.75v-8.5a.75.75 0 0 0-.75-.75H11V1.75a.75.75 0 0 0-.75-.75zM9.5 5V2.5h-7v7H5V5.75A.75.75 0 0 1 5.75 5zm-3 8.5v-7h7v7z" clipRule="evenodd"/>
  </svg>
);

/**
 * Reusable validation results table component
 */
export function ValidationResultsTable({
  data,
  onViewSample,
  loadingSampleId = null,
  onEntityClick,
  showCheckboxes = false,
  selectedIds = [],
  onToggleSelect,
  onToggleSelectAll,
  highlightId,
  highlightedRowRef,
  sortable = false,
  sortConfig,
  onSort,
  emptyMessage = "No results to display",
  maxHeight = null,
  fillHeight = false,
}) {
  const [errorModal, setErrorModal] = useState(null);
  const [copied, setCopied] = useState(false);


  // Copy table data to clipboard as TSV (paste into Excel/Sheets)
  const handleCopyToClipboard = async () => {
    const headers = ['Entity', 'Type', 'Tags', 'Status', 'Duration (s)', 'Source', 'Target', 'Row Count Source', 'Row Count Target', 'Differences', 'Diff %', 'Triggered'];
    const rows = data.map(v => [
      v.entity_name,
      v.entity_type === 'table' ? 'Table' : 'Query',
      parseArray(v._parsedTags || v.tags).join(', '),
      v.status,
      v.duration_seconds ?? '',
      v.source_system_name,
      v.target_system_name,
      v.row_count_source ?? '',
      v.row_count_target ?? '',
      v.rows_different ?? '',
      v.difference_pct ?? '',
      v.requested_at ? new Date(v.requested_at).toLocaleString() : ''
    ].join('\t'));
    const tsv = [headers.join('\t'), ...rows].join('\n');
    try {
      await navigator.clipboard.writeText(tsv);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy:', err);
      // Fallback: create a temporary textarea
      const textarea = document.createElement('textarea');
      textarea.value = tsv;
      textarea.style.position = 'fixed';
      textarea.style.opacity = '0';
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand('copy');
      document.body.removeChild(textarea);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const SortableHeader = ({ label, sortKey, className = "" }) => {
    if (!sortable) {
      return (
        <th className={`text-left px-2 py-1.5 text-sm text-gray-300 font-semibold ${className}`}>
          {label}
        </th>
      );
    }
    return (
      <th 
        className={`text-left px-2 py-1.5 text-sm text-gray-300 font-semibold cursor-pointer hover:bg-charcoal-300/30 transition-colors select-none ${className}`}
        onClick={() => onSort?.(sortKey)}
      >
        <div className="flex items-center gap-1">
          {label}
          {sortConfig?.key === sortKey && (
            <span className="text-rust-light">
              {sortConfig.direction === 'asc' ? '↑' : '↓'}
            </span>
          )}
        </div>
      </th>
    );
  };

  return (
    <div className={`relative ${fillHeight ? 'flex-1 min-h-0 flex flex-col' : ''}`} style={{ isolation: 'isolate' }}>
      {/* Floating copy button */}
      {data.length > 0 && (
        <button
          type="button"
          onClick={handleCopyToClipboard}
          className="absolute top-2 right-3 z-[100] p-1.5 text-gray-400 hover:text-white bg-charcoal-500 hover:bg-charcoal-400 rounded shadow-lg border border-charcoal-300 transition-colors cursor-pointer"
          title="Copy to clipboard"
        >
          {copied ? <span className="text-green-400 text-xs px-0.5">✓</span> : <CopyIcon />}
        </button>
      )}
      <div 
        className={`overflow-x-auto overflow-y-auto ${fillHeight ? 'flex-1 min-h-0' : ''}`} 
        style={maxHeight ? { maxHeight: `${maxHeight}px` } : undefined}
      >
      <table className="w-full min-w-[1200px]">
        <thead className="sticky top-0 bg-charcoal-400 border-b border-charcoal-200">
          <tr>
            {showCheckboxes && (
              <th className="px-2 py-1.5 w-12 text-center">
                <Checkbox
                  checked={selectedIds.length === data.length && data.length > 0}
                  onChange={onToggleSelectAll}
                  className="align-middle"
                />
              </th>
            )}
            <SortableHeader label="Entity" sortKey="entity_name" className="max-w-[500px]" />
            <SortableHeader label="Type" sortKey="entity_type" />
            <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold">Tags</th>
            <SortableHeader label="Status" sortKey="status" />
            <SortableHeader label="Duration" sortKey="duration" />
            <SortableHeader label="Source → Target" sortKey="systems" />
            <SortableHeader label="Row Counts" sortKey="row_counts" className="whitespace-nowrap" />
            <SortableHeader label="Diffs" sortKey="differences" className="whitespace-nowrap" />
            <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold w-16">Details</th>
            <SortableHeader label="Triggered" sortKey="requested_at" />
          </tr>
        </thead>
        <tbody>
          {data.length === 0 ? (
            <tr>
              <td colSpan={showCheckboxes ? 11 : 10} className="text-center p-8 text-gray-500 text-base">
                {emptyMessage}
              </td>
            </tr>
          ) : (
            data.map((v) => (
              <tr
                key={v.id}
                ref={v.id === highlightId ? highlightedRowRef : null}
                className={`border-b border-charcoal-300/30 hover:bg-charcoal-400/50 transition-colors ${
                  v.id === highlightId ? 'bg-rust-light/20 ring-2 ring-rust-light' : ''
                }`}
              >
                {showCheckboxes && (
                  <td className="px-2 py-1.5 text-center align-middle">
                    <Checkbox
                      checked={selectedIds.includes(v.id)}
                      onChange={() => onToggleSelect?.(v.id)}
                      className="align-middle"
                    />
                  </td>
                )}
                <td className="px-2 py-1.5 text-gray-200 font-medium text-sm max-w-[500px]" title={v.entity_name}>
                  {onEntityClick ? (
                    <button
                      onClick={() => onEntityClick(v.entity_type, v.entity_id)}
                      className="truncate overflow-hidden whitespace-nowrap [direction:rtl] text-left block w-full text-purple-400 hover:text-purple-300 hover:underline transition-colors cursor-pointer"
                      title={`Click to view ${v.entity_name} in ${v.entity_type === 'table' ? 'Tables' : 'Queries'}`}
                    >
                      <span className="[direction:ltr]">{v.entity_name}</span>
                    </button>
                  ) : (
                    <div className="truncate overflow-hidden whitespace-nowrap [direction:rtl] text-left">
                      <span className="[direction:ltr]">{v.entity_name}</span>
                    </div>
                  )}
                </td>
                <td className="px-2 py-1.5">
                  <span className={`px-1.5 py-0.5 text-sm rounded-full ${
                    v.entity_type === 'table'
                      ? 'bg-blue-900/40 text-blue-300 border border-blue-700'
                      : 'bg-purple-900/40 text-purple-300 border border-purple-700'
                  }`}>
                    {v.entity_type}
                  </span>
                </td>
                <td className="px-2 py-1.5">
                  <TagList tags={parseArray(v._parsedTags || v.tags)} maxVisible={4} />
                </td>
                <td className="px-2 py-1.5">
                  {v.status === 'succeeded' ? (
                    <span className="px-1.5 py-0.5 text-sm rounded-full bg-green-900/40 text-green-300 border border-green-700 whitespace-nowrap">
                      ✓ Success
                    </span>
                  ) : v.status === 'error' ? (
                    <button 
                      onClick={() => setErrorModal(v.error_message)}
                      className="px-1.5 py-0.5 text-sm rounded-full bg-orange-900/40 text-orange-300 border border-orange-700 whitespace-nowrap cursor-pointer hover:bg-orange-900/60 transition-colors"
                      title="Click to view error details"
                    >
                      ⚠ Error
                    </button>
                  ) : (
                    <span className="px-1.5 py-0.5 text-sm rounded-full bg-red-900/40 text-red-300 border border-red-700 whitespace-nowrap">
                      ✗ Failed
                    </span>
                  )}
                </td>
                <td className="px-2 py-1.5 text-gray-300 text-sm whitespace-nowrap">
                  {((v.duration_seconds || 0) / 60).toFixed(1)}m
                </td>
                <td className="px-2 py-1.5 text-sm text-gray-400 whitespace-nowrap">
                  {v.source_system_name} → {v.target_system_name}
                </td>
                <td className="px-2 py-1.5 text-sm whitespace-nowrap">
                  {v.status === 'error' || v.row_count_source == null ? (
                    <span className="text-gray-500">-</span>
                  ) : v.row_count_match ? (
                    <span className="text-green-400">✓ {v.row_count_source?.toLocaleString()}</span>
                  ) : loadingSampleId === v.id ? (
                    <span className="text-gray-400 text-xs">Loading...</span>
                  ) : (
                    <button
                      onClick={() => onViewSample?.(v)}
                      className="text-red-400 hover:text-red-300 underline decoration-dotted cursor-pointer transition-colors"
                      title="Click to view row count analysis"
                    >
                      {v.row_count_source?.toLocaleString()} ≠ {v.row_count_target?.toLocaleString()}
                    </button>
                  )}
                </td>
                <td className="px-2 py-1.5 text-sm whitespace-nowrap">
                  {v.rows_different == null ? (
                    <span className="text-gray-500">-</span>
                  ) : v.rows_different > 0 ? (
                    loadingSampleId === v.id ? (
                      <span className="text-gray-400 text-xs">Loading...</span>
                    ) : (
                      <button
                        onClick={() => onViewSample?.(v)}
                        className="text-red-400 font-medium hover:text-red-300 underline decoration-dotted cursor-pointer transition-colors"
                        title="Click to view sample differences"
                      >
                        {v.rows_different.toLocaleString()} ({v.difference_pct}%)
                      </button>
                    )
                  ) : (
                    <span className="text-green-400">0</span>
                  )}
                </td>
                <td className="px-2 py-1.5 text-sm whitespace-nowrap">
                  {v.databricks_run_url && (
                    <a
                      href={v.databricks_run_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-purple-400 hover:text-purple-300 underline"
                    >
                      View
                    </a>
                  )}
                </td>
                <td className="px-2 py-1.5 text-sm text-gray-400 whitespace-nowrap">
                  {new Date(v.requested_at).toLocaleString()}
                </td>
              </tr>
            ))
          )}
        </tbody>
      </table>
      </div>
      {errorModal !== null && <ErrorPopover error={errorModal} onClose={() => setErrorModal(null)} />}
    </div>
  );
}
