import React from 'react';
import { TagList } from './TagBadge';
import { Checkbox } from './Checkbox';

// Helper to safely parse tags
const parseTags = (tags) => {
  if (!tags) return [];
  if (Array.isArray(tags)) return tags;
  if (typeof tags === 'string') {
    try {
      const parsed = JSON.parse(tags);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
};

/**
 * Reusable validation results table component
 * 
 * @param {Object} props
 * @param {Array} props.data - Array of validation result objects
 * @param {Function} props.onViewSample - Callback when clicking diff count (receives validation object)
 * @param {Function} props.onEntityClick - Callback when clicking entity name (receives entityType, entityId)
 * @param {boolean} props.showCheckboxes - Whether to show row selection checkboxes
 * @param {Array} props.selectedIds - Array of selected row IDs (required if showCheckboxes)
 * @param {Function} props.onToggleSelect - Callback for toggling row selection (required if showCheckboxes)
 * @param {Function} props.onToggleSelectAll - Callback for toggling all rows (required if showCheckboxes)
 * @param {number} props.highlightId - ID of row to highlight
 * @param {React.Ref} props.highlightedRowRef - Ref for highlighted row
 * @param {boolean} props.sortable - Whether columns are sortable
 * @param {Object} props.sortConfig - Current sort config { key, direction }
 * @param {Function} props.onSort - Callback for sorting (receives column key)
 * @param {string} props.emptyMessage - Message to show when no data
 * @param {number} props.maxHeight - Max height in px (default 500)
 */
export function ValidationResultsTable({
  data,
  onViewSample,
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
  maxHeight = 500,
}) {
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
    <div className="overflow-x-auto" style={{ maxHeight: `${maxHeight}px` }}>
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
            <SortableHeader label="Triggered" sortKey="requested_at" />
            <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold w-16">Details</th>
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
                  <TagList tags={parseTags(v.tags)} maxVisible={4} />
                </td>
                <td className="px-2 py-1.5">
                  {v.status === 'succeeded' ? (
                    <span className="px-1.5 py-0.5 text-sm rounded-full bg-green-900/40 text-green-300 border border-green-700 whitespace-nowrap">
                      ✓ Success
                    </span>
                  ) : v.status === 'error' ? (
                    <span 
                      className="px-1.5 py-0.5 text-sm rounded-full bg-orange-900/40 text-orange-300 border border-orange-700 whitespace-nowrap cursor-help"
                      title={v.error_message || 'Unknown error'}
                    >
                      ⚠ Error
                    </span>
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
                    <button
                      onClick={() => onViewSample?.(v)}
                      className="text-red-400 font-medium hover:text-red-300 underline decoration-dotted cursor-pointer transition-colors"
                      title="Click to view sample differences"
                    >
                      {v.rows_different.toLocaleString()} ({v.difference_pct}%)
                    </button>
                  ) : (
                    <span className="text-green-400">0</span>
                  )}
                </td>
                <td className="px-2 py-1.5 text-sm text-gray-400 whitespace-nowrap">
                  {new Date(v.requested_at).toLocaleString()}
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
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
