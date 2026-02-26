import React, { useMemo, useState } from 'react';

const COLS = ['level', 'catalog_name', 'schema_name', 'object_name', 'object_type', 'parent_name'];

/**
 * Safely coerce lineage into an array of objects regardless of how it arrives.
 */
function normalizeLineage(raw) {
  if (!raw) return [];
  let parsed = raw;
  if (typeof parsed === 'string') {
    try { parsed = JSON.parse(parsed); } catch { return []; }
  }
  if (parsed && !Array.isArray(parsed) && Array.isArray(parsed.items)) {
    return parsed.items;
  }
  if (Array.isArray(parsed)) return parsed;
  if (typeof parsed === 'object') return [parsed];
  return [];
}

/**
 * Build a lookup key from schema + table for matching against configured tables.
 */
function tableKey(schema, table) {
  return `${(schema || '').trim().toLowerCase()}.${(table || '').trim().toLowerCase()}`;
}

/**
 * Format a timestamp into a human-friendly relative string.
 */
function formatRelativeTime(timestamp) {
  if (!timestamp) return null;
  const date = new Date(timestamp);
  if (isNaN(date.getTime())) return null;
  const now = new Date();
  const diffMs = now - date;
  const diffMins = Math.floor(diffMs / 60000);
  if (diffMins < 1) return 'just now';
  if (diffMins < 60) return `${diffMins}m ago`;
  const diffHours = Math.floor(diffMins / 60);
  if (diffHours < 24) return `${diffHours}h ago`;
  const diffDays = Math.floor(diffHours / 24);
  if (diffDays < 7) return `${diffDays}d ago`;
  return date.toLocaleDateString();
}

/** Status badge colors */
const STATUS_COLORS = {
  succeeded: 'text-green-300',
  failed: 'text-red-300',
  error: 'text-orange-300',
  running: 'text-blue-300',
};

/** Object type badge styles */
const TYPE_BADGE = {
  TABLE:             { cls: 'bg-blue-900/40 text-blue-300 border-blue-700', label: 'Table' },
  VIEW:              { cls: 'bg-purple-900/40 text-purple-300 border-purple-700', label: 'View' },
  MATERIALIZED_VIEW: { cls: 'bg-teal-900/40 text-teal-300 border-teal-700', label: 'Mat. View' },
  STREAMING_TABLE:   { cls: 'bg-cyan-900/40 text-cyan-300 border-cyan-700', label: 'Stream' },
};
const DEFAULT_TYPE_BADGE = { cls: 'bg-gray-900/40 text-gray-400 border-gray-600', label: 'Unknown' };

export function TypeBadge({ type }) {
  const badge = TYPE_BADGE[(type || '').toUpperCase()] || DEFAULT_TYPE_BADGE;
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 text-[10px] font-medium rounded-full border ${badge.cls}`}>
      {badge.label}
    </span>
  );
}

/**
 * Modal that displays upstream lineage for a table/view.
 * Shows configuration status, last run info, and allows configuring & running all.
 *
 * Props:
 *   configuredTables - array of table objects from /api/tables (passed from App.jsx via tbl.data)
 *   onConfigureTable - callback({ schema_name, object_name, catalog_name }) to open TableModal
 *   onRunAll         - callback(tableIds[]) to bulk-trigger validation for configured lineage tables
 */
export function LineageModal({
  open,
  onClose,
  entityName,
  lineage: rawLineage,
  configuredTables = [],
  onConfigureTable,
  onRunAll,
  onNavigateToEntity,
  srcSystemKind = '',
  tgtSystemKind = '',
  srcSystemName = '',
  tgtSystemName = '',
  lineageSystem = null,
  entityTableType = '',
}) {
  const lineage = useMemo(() => normalizeLineage(rawLineage), [rawLineage]);
  const [runningAll, setRunningAll] = useState(false);

  const lineageSourceNote = useMemo(() => {
    if (lineageSystem === 'source') {
      return { text: `Lineage queried from source system (${srcSystemName || srcSystemKind || 'Unknown'})`, color: 'text-blue-400' };
    }
    if (lineageSystem === 'target') {
      return { text: `Lineage queried from target system (${tgtSystemName || tgtSystemKind || 'Unknown'})`, color: 'text-blue-400' };
    }
    return { text: 'Lineage source system not recorded — refresh lineage to update', color: 'text-gray-400' };
  }, [lineageSystem, srcSystemName, tgtSystemName, srcSystemKind, tgtSystemKind]);

  // Build a Map of configured table keys (both src and tgt) for fast lookup
  const configuredKeys = useMemo(() => {
    const keys = new Map();
    for (const t of configuredTables) {
      const srcKey = tableKey(t.src_schema, t.src_table);
      if (!keys.has(srcKey)) keys.set(srcKey, t);
      const tgtKey = tableKey(t.tgt_schema, t.tgt_table);
      if (!keys.has(tgtKey)) keys.set(tgtKey, t);
    }
    return keys;
  }, [configuredTables]);

  // For each lineage row, determine if it's configured and attach the configured table info
  const enrichedLineage = useMemo(() => {
    return lineage.map(row => {
      const key = tableKey(row.schema_name, row.object_name);
      const match = configuredKeys.get(key);
      return { ...row, _configured: !!match, _configuredTable: match || null };
    });
  }, [lineage, configuredKeys]);

  const allConfigured = lineage.length > 0 && enrichedLineage.every(r => r._configured);
  const configuredCount = enrichedLineage.filter(r => r._configured).length;

  // Collect unique table IDs for the "Run All" action
  const configuredTableIds = useMemo(() => {
    const ids = new Set();
    for (const row of enrichedLineage) {
      if (row._configuredTable?.id) ids.add(row._configuredTable.id);
    }
    return Array.from(ids);
  }, [enrichedLineage]);

  const handleRunAll = async () => {
    if (!onRunAll || configuredTableIds.length === 0) return;
    setRunningAll(true);
    try {
      await onRunAll(configuredTableIds);
    } finally {
      setRunningAll(false);
    }
  };

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60" onClick={onClose}>
      <div
        className="bg-charcoal-500 border border-charcoal-200 rounded-lg shadow-xl max-w-6xl w-full max-h-[85vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between p-3 border-b border-charcoal-200">
          <div className="flex items-center gap-3">
            <h3 className="text-lg font-semibold text-gray-100">
              Lineage: {entityName || 'Table/View'}
            </h3>
            {entityTableType && (
              <span className="inline-flex items-center gap-1.5">
                <span className="text-xs text-gray-500">Object Type:</span>
                <TypeBadge type={entityTableType} />
              </span>
            )}
            {lineage.length > 0 && (
              <span className="text-xs text-gray-400">
                {configuredCount}/{lineage.length} tracked
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            {allConfigured && onRunAll && (
              <button
                onClick={handleRunAll}
                disabled={runningAll}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium bg-green-900/50 text-green-200 border border-green-700/60 hover:bg-green-800/60 hover:border-green-600 transition-colors cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed"
                title="Run validation for all upstream tables"
              >
                {runningAll ? (
                  <>
                    <span className="inline-block w-3.5 h-3.5 border-2 border-green-300 border-t-transparent rounded-full animate-spin"></span>
                    Triggering…
                  </>
                ) : (
                  <>▶ Run All ({configuredTableIds.length})</>
                )}
              </button>
            )}
            <button
              type="button"
              onClick={onClose}
              className="text-gray-400 hover:text-white transition-colors p-1"
              aria-label="Close"
            >
              ✕
            </button>
          </div>
        </div>

        {/* Lineage source note */}
        <div className={`px-3 py-1.5 text-xs ${lineageSourceNote.color} bg-charcoal-400/50 border-b border-charcoal-200 flex items-center gap-1.5`}>
          <span>ℹ</span> {lineageSourceNote.text}
        </div>

        {/* Table */}
        <div className="p-3 overflow-auto flex-1 min-h-0">
          {lineage.length === 0 ? (
            <p className="text-gray-500">No upstream lineage found.</p>
          ) : (
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-charcoal-400">
                <tr>
                  {COLS.map((c) => (
                    <th key={c} className="text-left px-2 py-1.5 text-gray-300 font-semibold capitalize">
                      {c.replace(/_/g, ' ')}
                    </th>
                  ))}
                  <th className="text-left px-2 py-1.5 text-gray-300 font-semibold">Tracking?</th>
                  <th className="text-left px-2 py-1.5 text-gray-300 font-semibold">Last Run</th>
                </tr>
              </thead>
              <tbody>
                {enrichedLineage.map((row, i) => {
                  const ct = row._configuredTable;
                  const lastRunTime = ct?.last_run_timestamp;
                  const lastRunStatus = ct?.last_run_status;
                  const relTime = formatRelativeTime(lastRunTime);

                  return (
                    <tr key={i} className="border-b border-charcoal-300/30 hover:bg-charcoal-400/50">
                      {COLS.map((col) => (
                        <td key={col} className="px-2 py-1.5 text-gray-200">
                          {col === 'object_type' && (row[col] || row['table_type'])
                            ? <TypeBadge type={row[col] || row['table_type']} />
                            : row[col] != null ? String(row[col]) : '—'}
                        </td>
                      ))}
                      {/* Status */}
                      <td className="px-2 py-1.5">
                        {row._configured ? (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-green-900/40 text-green-300 border border-green-700/50">
                            ✓ Added
                          </span>
                        ) : (
                          <button
                            onClick={() => {
                              if (onConfigureTable) {
                                onConfigureTable({
                                  schema_name: row.schema_name || '',
                                  object_name: row.object_name || '',
                                  catalog_name: row.catalog_name || '',
                                });
                              }
                            }}
                            className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-purple-900/40 text-purple-300 border border-purple-700/50 hover:bg-purple-800/60 hover:border-purple-600 transition-colors cursor-pointer"
                            title={`Add ${row.schema_name}.${row.object_name} for validation`}
                          >
                            + Add Table
                          </button>
                        )}
                      </td>
                      {/* Last Run */}
                      <td className="px-2 py-1.5">
                        {row._configured ? (
                          relTime ? (
                            onNavigateToEntity ? (
                              <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  onNavigateToEntity(ct);
                                }}
                                className={`text-xs font-medium ${STATUS_COLORS[lastRunStatus] || 'text-gray-400'} hover:underline cursor-pointer bg-transparent border-0 p-0`}
                                title={`${lastRunStatus} — ${new Date(lastRunTime).toLocaleString()} · Click to view analysis`}
                              >
                                {lastRunStatus === 'succeeded' ? '✓' : lastRunStatus === 'failed' ? '✗' : lastRunStatus === 'error' ? '⚠' : '●'}{' '}
                                {relTime} →
                              </button>
                            ) : (
                              <span
                                className={`text-xs font-medium ${STATUS_COLORS[lastRunStatus] || 'text-gray-400'}`}
                                title={`${lastRunStatus} — ${new Date(lastRunTime).toLocaleString()}`}
                              >
                                {lastRunStatus === 'succeeded' ? '✓' : lastRunStatus === 'failed' ? '✗' : lastRunStatus === 'error' ? '⚠' : '●'}{' '}
                                {relTime}
                              </span>
                            )
                          ) : (
                            <span className="text-xs text-gray-500">Never run</span>
                          )
                        ) : (
                          <span className="text-xs text-gray-600">—</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>

        {/* Footer legend */}
        {lineage.length > 0 && (
          <div className="px-3 py-2 border-t border-charcoal-200 flex items-center justify-between">
            <div className="flex items-center gap-4 text-xs text-gray-400 flex-wrap">
              <span className="flex items-center gap-1">
                <span className="inline-block w-2 h-2 rounded-full bg-green-500"></span> Added for validation
              </span>
              <span className="flex items-center gap-1">
                <span className="inline-block w-2 h-2 rounded-full bg-purple-500"></span> Click to add table
              </span>
              <span className="text-gray-600 mx-1">|</span>
              <TypeBadge type="TABLE" />
              <TypeBadge type="VIEW" />
              <TypeBadge type="MATERIALIZED_VIEW" />
              <TypeBadge type="STREAMING_TABLE" />
            </div>
            {!allConfigured && configuredCount > 0 && onRunAll && (
              <button
                onClick={handleRunAll}
                disabled={runningAll}
                className="inline-flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-medium bg-charcoal-400 text-gray-300 border border-charcoal-200 hover:bg-charcoal-300 transition-colors cursor-pointer disabled:opacity-50"
                title="Run validation for configured tables only"
              >
                {runningAll ? 'Triggering…' : `▶ Run Configured (${configuredTableIds.length})`}
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
