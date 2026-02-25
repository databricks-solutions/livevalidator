import React, { useState, useMemo, useEffect } from 'react';
import { ErrorBox } from '../components/ErrorBox';
import { TagList } from '../components/TagBadge';
import { LineageModal } from '../components/modals/LineageModal';
import { SampleDifferencesModal } from '../components/modals/SampleDifferencesModal';

function parseLineageData(raw) {
  if (!raw) return { items: null, entityObjectType: null };
  let parsed = raw;
  if (typeof parsed === 'string') {
    try { parsed = JSON.parse(parsed); } catch { return { items: null, entityObjectType: null }; }
  }
  if (Array.isArray(parsed)) return { items: parsed, entityObjectType: null };
  if (parsed && typeof parsed === 'object' && Array.isArray(parsed.items)) {
    return { items: parsed.items, entityObjectType: parsed.entity_object_type || null };
  }
  if (typeof parsed === 'object') return { items: [parsed], entityObjectType: null };
  return { items: null, entityObjectType: null };
}

function parseTags(tags) {
  if (!tags) return [];
  if (Array.isArray(tags)) return tags;
  if (typeof tags === 'string') {
    try {
      const parsed = JSON.parse(tags);
      return Array.isArray(parsed) ? parsed : [];
    } catch { return []; }
  }
  return [];
}

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

const STATUS_BADGE = {
  succeeded: { cls: 'bg-green-900/40 text-green-300 border-green-700', icon: '✓', label: 'Success' },
  failed:    { cls: 'bg-red-900/40 text-red-300 border-red-700', icon: '✗', label: 'Failed' },
  error:     { cls: 'bg-orange-900/40 text-orange-300 border-orange-700', icon: '⚠', label: 'Error' },
};

// ─── Entity Detail Page ───────────────────────────────────────────────

function EntityDetailView({ entity, tables, systems, schedules, onBack, onConfigureTable, onRunAllLineage, onRefresh, onTrigger, onEditTable, onEditQuery, onSelectEntity }) {
  const [runs, setRuns] = useState(null);
  const [runsLoading, setRunsLoading] = useState(true);
  const [selectedRunId, setSelectedRunId] = useState(null);
  const [selectedRunDetail, setSelectedRunDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [sampleModal, setSampleModal] = useState(null);
  const [lineageModal, setLineageModal] = useState({ open: false, lineage: [], lineageSystem: null });
  const [fetchingLineage, setFetchingLineage] = useState(false);
  const [triggerRunning, setTriggerRunning] = useState(false);
  const [toast, setToast] = useState(null);

  const entityType = entity._entityType === 'table' ? 'table' : 'compare_query';
  const apiPath = entity._entityType === 'table' ? 'tables' : 'queries';
  const srcIsDatabricks = entity._srcSystemKind === 'Databricks';
  const tgtIsDatabricks = entity._tgtSystemKind === 'Databricks';
  const bothDatabricks = srcIsDatabricks && tgtIsDatabricks;
  const canFetchLineage = srcIsDatabricks || tgtIsDatabricks;
  const { items: parsedLineage, entityObjectType } = parseLineageData(entity.lineage);
  const [showSystemPicker, setShowSystemPicker] = useState(false);

  // Fetch all runs for this entity
  useEffect(() => {
    let cancelled = false;
    setRunsLoading(true);
    fetch(`/api/validation-history?entity_type=${entityType}&entity_id=${entity.id}&days_back=7&limit=100`)
      .then(r => r.json())
      .then(data => {
        if (!cancelled) {
          setRuns(data || []);
          setRunsLoading(false);
          // Auto-select latest run
          if (data && data.length > 0) {
            setSelectedRunId(data[0].id);
          }
        }
      })
      .catch(() => {
        if (!cancelled) { setRuns([]); setRunsLoading(false); }
      });
    return () => { cancelled = true; };
  }, [entity.id, entityType]);

  // Fetch detail when selected run changes
  useEffect(() => {
    if (!selectedRunId) { setSelectedRunDetail(null); return; }
    let cancelled = false;
    setDetailLoading(true);
    fetch(`/api/validation-history/${selectedRunId}`)
      .then(r => r.json())
      .then(detail => {
        if (!cancelled) {
          setSelectedRunDetail(detail);
          setDetailLoading(false);
        }
      })
      .catch(() => {
        if (!cancelled) { setSelectedRunDetail(null); setDetailLoading(false); }
      });
    return () => { cancelled = true; };
  }, [selectedRunId]);

  const showToast = (message, type = 'info') => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 5000);
  };

  const requestFetchLineage = () => {
    if (bothDatabricks) {
      setShowSystemPicker(true);
    } else if (srcIsDatabricks) {
      doFetchLineage('source');
    } else if (tgtIsDatabricks) {
      doFetchLineage('target');
    } else {
      showToast(`Neither ${entity._srcSystemName} nor ${entity._tgtSystemName} are Databricks, cannot fetch lineage.`, 'error');
    }
  };

  const doFetchLineage = async (system) => {
    setShowSystemPicker(false);
    const systemName = system === 'source' ? entity._srcSystemName : entity._tgtSystemName;
    showToast(`Fetching lineage from ${systemName}…`);
    setFetchingLineage(true);
    try {
      const startRes = await fetch(`/api/${apiPath}/${entity.id}/fetch-lineage?system=${system}`, { method: 'POST' });
      const startData = await startRes.json().catch(() => ({}));
      if (!startRes.ok) {
        showToast(startData.detail || 'Failed to start lineage fetch', 'error');
        return;
      }
      const deadline = Date.now() + 120000;
      while (Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 3000));
        const detailRes = await fetch(`/api/${apiPath}/${entity.id}`);
        if (!detailRes.ok) break;
        const detail = await detailRes.json();
        const { items: parsed } = parseLineageData(detail.lineage);
        if (parsed) {
          setLineageModal({ open: true, lineage: parsed, lineageSystem: system });
          onRefresh?.();
          return;
        }
      }
      showToast('Lineage is still being fetched. Check back in a minute.', 'warning');
    } catch (e) {
      showToast(e.message || 'Failed to fetch lineage', 'error');
    } finally {
      setFetchingLineage(false);
    }
  };

  const handleViewLineage = () => {
    if (parsedLineage) {
      const guessedSystem = srcIsDatabricks ? 'source' : tgtIsDatabricks ? 'target' : null;
      setLineageModal({ open: true, lineage: parsedLineage, lineageSystem: guessedSystem });
    }
  };

  const handleTriggerValidation = async () => {
    if (!onTrigger) return;
    setTriggerRunning(true);
    try {
      await onTrigger(entityType, entity.id);
    } finally {
      setTriggerRunning(false);
    }
  };

  const handleEditConfig = () => {
    if (entity._entityType === 'table') {
      onEditTable?.(entity);
    } else {
      onEditQuery?.(entity);
    }
  };

  const scheduleName = useMemo(() => {
    if (!entity.schedule_id || !schedules) return null;
    const s = schedules.find(s => s.id === entity.schedule_id);
    return s ? s.name : null;
  }, [entity.schedule_id, schedules]);

  return (
    <>
      {/* Back button + Entity header */}
      <div className="mb-4">
        <button
          onClick={onBack}
          className="text-gray-400 hover:text-gray-200 text-lg font-semibold mb-3 inline-flex items-center gap-2 px-4 py-2 rounded-md hover:bg-charcoal-400 transition-colors"
        >
          ← Back to list
        </button>
        <div className="flex items-start justify-between">
          <div>
            <div className="flex items-center gap-3 mb-1">
              <h2 className="text-2xl font-bold text-gray-100">{entity.name}</h2>
              <span className={`px-1.5 py-0.5 text-xs rounded-full ${
                entity._entityType === 'table'
                  ? 'bg-blue-900/40 text-blue-300 border border-blue-700'
                  : 'bg-purple-900/40 text-purple-300 border border-purple-700'
              }`}>
                {entity._entityType === 'table' ? 'Table' : 'Query'}
              </span>
              {entity._parsedTags && entity._parsedTags.length > 0 && (
                <TagList tags={entity._parsedTags} maxVisible={5} />
              )}
            </div>
            {entity._entityType === 'table' ? (
              <div className="flex items-center gap-2 mt-1">
                <span className="inline-flex items-center gap-1.5">
                  <span className="text-[10px] text-gray-500 uppercase tracking-wider">Src</span>
                  <span className="text-sm font-mono text-blue-300">{entity.src_schema || '—'}.{entity.src_table || '—'}</span>
                  <span className="text-xs text-gray-500">({entity._srcSystemName}{entity._srcSystemKind ? ` · ${entity._srcSystemKind}` : ''})</span>
                </span>
                <span className="text-gray-600 mx-1">→</span>
                <span className="inline-flex items-center gap-1.5">
                  <span className="text-[10px] text-gray-500 uppercase tracking-wider">Tgt</span>
                  <span className="text-sm font-mono text-green-300">{entity.tgt_schema || '—'}.{entity.tgt_table || '—'}</span>
                  <span className="text-xs text-gray-500">({entity._tgtSystemName}{entity._tgtSystemKind ? ` · ${entity._tgtSystemKind}` : ''})</span>
                </span>
              </div>
            ) : (
              <div className="flex items-center gap-1.5 mt-1">
                <span className="text-[10px] text-gray-500 uppercase tracking-wider">SQL</span>
                <span className="text-sm font-mono text-gray-400 truncate max-w-lg" title={entity.sql}>{(entity.sql || '—').substring(0, 120)}</span>
              </div>
            )}
          </div>
          <div className="flex items-center gap-2 flex-shrink-0">
            <button
              onClick={handleEditConfig}
              className="px-3 py-1.5 text-sm bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500 transition-colors font-medium inline-flex items-center gap-1.5"
            >
              ✎ Edit Configuration
            </button>
            <button
              onClick={handleTriggerValidation}
              disabled={triggerRunning}
              className="px-3 py-1.5 text-sm bg-green-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-green-500 transition-colors font-medium inline-flex items-center gap-1.5 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {triggerRunning ? 'Triggering…' : '▶ Run Validation'}
            </button>
          </div>
        </div>
      </div>

      {/* Configuration Summary — single line */}
      <div className="bg-charcoal-500 border border-charcoal-200 rounded-lg px-4 py-2.5 mb-5 flex items-center gap-3 flex-wrap text-xs">
        {entity._entityType === 'table' ? (
          <>
            <span className="inline-flex items-center gap-1.5 bg-charcoal-400 rounded px-2 py-1">
              <span className="text-gray-500 uppercase tracking-wider text-[10px]">Src</span>
              <span className="text-gray-200 font-mono">{entity.src_schema || '—'}.{entity.src_table || '—'}</span>
            </span>
            <span className="text-charcoal-200">→</span>
            <span className="inline-flex items-center gap-1.5 bg-charcoal-400 rounded px-2 py-1">
              <span className="text-gray-500 uppercase tracking-wider text-[10px]">Tgt</span>
              <span className="text-gray-200 font-mono">{entity.tgt_schema || '—'}.{entity.tgt_table || '—'}</span>
            </span>
          </>
        ) : (
          <span className="inline-flex items-center gap-1.5 bg-charcoal-400 rounded px-2 py-1">
            <span className="text-gray-500 uppercase tracking-wider text-[10px]">SQL</span>
            <span className="text-gray-200 font-mono truncate max-w-xs" title={entity.sql}>{(entity.sql || '—').substring(0, 80)}</span>
          </span>
        )}
        <span className="w-px h-4 bg-charcoal-200" />
        <span className="inline-flex items-center gap-1.5 bg-charcoal-400 rounded px-2 py-1">
          <span className="text-gray-500 uppercase tracking-wider text-[10px]">Mode</span>
          <span className="text-gray-200">{entity.compare_mode || '—'}</span>
        </span>
        {entity.pk_columns && entity.pk_columns.length > 0 && (
          <span className="inline-flex items-center gap-1.5 bg-charcoal-400 rounded px-2 py-1">
            <span className="text-gray-500 uppercase tracking-wider text-[10px]">PK</span>
            <span className="text-gray-200 font-mono">{Array.isArray(entity.pk_columns) ? entity.pk_columns.join(', ') : entity.pk_columns}</span>
          </span>
        )}
        {scheduleName && (
          <span className="inline-flex items-center gap-1.5 bg-charcoal-400 rounded px-2 py-1">
            <span className="text-gray-500 uppercase tracking-wider text-[10px]">Schedule</span>
            <span className="text-gray-200">{scheduleName}</span>
          </span>
        )}
        <span className={`inline-flex items-center px-2 py-1 rounded text-[10px] font-semibold uppercase tracking-wider ${
          entity.is_active === false
            ? 'bg-red-900/30 text-red-400 border border-red-800'
            : 'bg-green-900/30 text-green-400 border border-green-800'
        }`}>
          {entity.is_active === false ? 'Inactive' : 'Active'}
        </span>
        <button
          onClick={handleEditConfig}
          className="ml-auto text-xs text-purple-400 hover:text-purple-300 transition-colors cursor-pointer inline-flex items-center gap-1"
        >
          ✎ Edit
        </button>
      </div>

      {/* Lineage + PK Discovery — side by side */}
      {(() => {
        const showPK = entity._entityType === 'table' && entity.compare_mode === 'except_all' && (!entity.pk_columns || (Array.isArray(entity.pk_columns) ? entity.pk_columns.length === 0 : !entity.pk_columns));
        const lineageSection = entity._entityType === 'table' ? (
          <div className={`bg-charcoal-500 border border-charcoal-200 border-l-4 border-l-rust rounded-lg p-3 ${showPK ? 'flex-1 min-w-0' : 'w-full'}`}>
            <div className="flex items-center justify-between flex-wrap gap-2">
              <div className="flex items-center gap-3">
                <h3 className="text-sm font-semibold text-gray-200">Upstream Lineage</h3>
                {fetchingLineage ? (
                  <span className="text-xs text-yellow-400 flex items-center gap-1.5">
                    <span className="inline-block w-3 h-3 border-2 border-yellow-400 border-t-transparent rounded-full animate-spin"></span>
                    Fetching lineage…
                  </span>
                ) : parsedLineage ? (
                  <span className="text-xs text-green-400">{parsedLineage.length} upstream table{parsedLineage.length !== 1 ? 's' : ''} found</span>
                ) : (
                  <span className="text-xs text-gray-500">Not fetched yet</span>
                )}
              </div>
              <div className="flex items-center gap-2">
                {parsedLineage && !fetchingLineage && (
                  <button
                    onClick={handleViewLineage}
                    className="px-3 py-1.5 rounded text-xs font-medium bg-green-900/40 text-green-300 border border-green-700/50 hover:bg-green-800/60 transition-colors cursor-pointer"
                  >
                    View Lineage
                  </button>
                )}
                {fetchingLineage ? (
                  <span className="text-gray-400 text-xs px-3 py-1.5">Fetching...</span>
                ) : (
                  <div className="relative">
                    <button
                      onClick={requestFetchLineage}
                      className={`px-3 py-1.5 rounded text-xs font-medium transition-colors cursor-pointer ${
                        canFetchLineage
                          ? parsedLineage
                            ? 'bg-charcoal-400 text-gray-300 border border-charcoal-200 hover:bg-charcoal-300'
                            : 'bg-purple-900/40 text-purple-300 border border-purple-700/50 hover:bg-purple-800/60'
                          : 'bg-charcoal-400 text-gray-500 border border-charcoal-300'
                      }`}
                    >
                      {parsedLineage ? '↻ Refresh Lineage' : 'Fetch Lineage'}
                    </button>
                    {showSystemPicker && (
                      <div className="absolute right-0 top-full mt-1 z-20 bg-charcoal-500 border border-charcoal-200 rounded-lg shadow-xl p-3 w-64">
                        <p className="text-xs text-gray-300 mb-2 font-medium">Both systems are Databricks. Fetch lineage from:</p>
                        <div className="flex flex-col gap-1.5">
                          <button
                            onClick={() => doFetchLineage('source')}
                            className="text-left px-3 py-2 rounded text-xs font-medium bg-charcoal-400 text-gray-200 border border-charcoal-300 hover:bg-charcoal-300 hover:border-purple-600 transition-colors cursor-pointer"
                          >
                            <span className="text-gray-500 text-[10px] uppercase tracking-wider">Source</span>
                            <span className="block font-mono text-gray-200">{entity._srcSystemName} / {entity.src_schema}.{entity.src_table}</span>
                          </button>
                          <button
                            onClick={() => doFetchLineage('target')}
                            className="text-left px-3 py-2 rounded text-xs font-medium bg-charcoal-400 text-gray-200 border border-charcoal-300 hover:bg-charcoal-300 hover:border-purple-600 transition-colors cursor-pointer"
                          >
                            <span className="text-gray-500 text-[10px] uppercase tracking-wider">Target</span>
                            <span className="block font-mono text-gray-200">{entity._tgtSystemName} / {entity.tgt_schema}.{entity.tgt_table}</span>
                          </button>
                        </div>
                        <button
                          onClick={() => setShowSystemPicker(false)}
                          className="mt-2 text-xs text-gray-500 hover:text-gray-300 cursor-pointer"
                        >
                          Cancel
                        </button>
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
          </div>
        ) : (
          <div className="bg-charcoal-500 border border-charcoal-200 border-l-4 border-l-rust rounded-lg p-3 flex-1 min-w-0">
            <h3 className="text-sm font-semibold text-gray-200 mb-1">Upstream Lineage</h3>
            <p className="text-xs text-gray-500">Lineage is available only for tables at this time.</p>
          </div>
        );

        const pkSection = showPK ? (
          <div className="bg-charcoal-500 border border-charcoal-200 border-l-4 border-l-rust rounded-lg p-3 flex-1 min-w-0">
            <div className="flex items-center justify-between flex-wrap gap-2">
              <div className="flex items-center gap-3">
                <h3 className="text-sm font-semibold text-gray-200">Primary Key Discovery</h3>
                <span className="text-xs text-gray-500">No PK columns configured</span>
              </div>
              <button
                disabled
                className="px-3 py-1.5 rounded text-xs font-medium bg-charcoal-400 text-gray-500 border border-charcoal-300 cursor-not-allowed"
                title="Coming soon — will trigger a job to discover primary keys and update the entity configuration"
              >
                Discover PK (coming soon)
              </button>
            </div>
            <p className="text-xs text-gray-500 mt-2">
              Uses <span className="font-mono text-gray-400">except_all</span> mode. PK discovery can switch to <span className="font-mono text-gray-400">primary_key</span> for more precise diffs.
            </p>
          </div>
        ) : null;

        return (
          <div className={`mb-5 ${showPK ? 'flex gap-4' : ''}`}>
            {lineageSection}
            {pkSection}
          </div>
        );
      })()}

      {/* Run History Section */}
      <div className="bg-charcoal-500 border border-charcoal-200 border-l-4 border-l-rust rounded-lg flex flex-col" style={{ minHeight: 'calc(100vh - 480px)' }}>
        <div className="p-3 border-b border-charcoal-200 bg-charcoal-400 flex items-center justify-between">
          <h3 className="text-sm font-semibold text-gray-200">
            Validation Runs (Last 7 Days)
            {runs && <span className="text-gray-400 font-normal ml-2">— {runs.length} run{runs.length !== 1 ? 's' : ''}</span>}
          </h3>
        </div>

        {runsLoading ? (
          <div className="p-6 text-gray-400 text-sm">Loading run history...</div>
        ) : runs.length === 0 ? (
          <div className="p-6 text-gray-500 text-sm">No validation runs in the last 7 days.</div>
        ) : (
          <div className="flex flex-1 min-h-0">
            {/* Left: Run list */}
            <div className="w-72 flex-shrink-0 border-r border-charcoal-200 overflow-y-auto">
              {runs.map((run) => {
                const badge = STATUS_BADGE[run.status] || STATUS_BADGE.error;
                const isSelected = selectedRunId === run.id;
                return (
                  <div
                    key={run.id}
                    onClick={() => setSelectedRunId(run.id)}
                    className={`px-3 py-2.5 border-b border-charcoal-300/30 cursor-pointer transition-colors ${
                      isSelected
                        ? 'bg-charcoal-400 border-l-2 border-l-rust'
                        : 'hover:bg-charcoal-400/50 border-l-2 border-l-transparent'
                    }`}
                  >
                    <div className="flex items-center gap-2 mb-1">
                      <span className={`px-1.5 py-0.5 text-[10px] rounded-full border ${badge.cls}`}>
                        {badge.icon} {badge.label}
                      </span>
                      <span className="text-gray-400 text-[10px] ml-auto">
                        {formatRelativeTime(run.requested_at)}
                      </span>
                    </div>
                    <div className="text-xs text-gray-300">
                      {new Date(run.requested_at).toLocaleString()}
                    </div>
                    <div className="flex items-center gap-2 mt-1 text-[10px]">
                      <span className="text-gray-500">
                        {run.source_system_name} → {run.target_system_name}
                      </span>
                      {run.duration_seconds != null && (
                        <span className="text-gray-500">{(run.duration_seconds / 60).toFixed(1)}m</span>
                      )}
                    </div>
                    {/* Quick stats */}
                    <div className="flex items-center gap-3 mt-1 text-[10px]">
                      {run.row_count_source != null && (
                        run.row_count_match ? (
                          <span className="text-green-400">Rows: ✓ {run.row_count_source?.toLocaleString()}</span>
                        ) : (
                          <span className="text-red-400">
                            {run.row_count_source?.toLocaleString()} ≠ {run.row_count_target?.toLocaleString()}
                          </span>
                        )
                      )}
                      {run.rows_different != null && run.rows_different > 0 && (
                        <span className="text-red-400 font-medium">
                          {run.rows_different.toLocaleString()} diffs
                        </span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>

            {/* Right: Selected run detail */}
            <div className="flex-1 overflow-y-auto">
              {detailLoading ? (
                <div className="p-6 text-gray-400 text-sm">Loading run details...</div>
              ) : !selectedRunDetail ? (
                <div className="p-6 text-gray-500 text-sm">Select a run from the list to view details.</div>
              ) : (
                <div className="p-4 space-y-4">
                  {/* Run summary: header + metrics */}
                  <div className="bg-charcoal-600 border border-charcoal-300 rounded-lg p-4 space-y-3">
                    <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Run summary</h4>
                    <div className="flex items-center gap-3 flex-wrap">
                      {(() => {
                        const badge = STATUS_BADGE[selectedRunDetail.status] || STATUS_BADGE.error;
                        return (
                          <span className={`px-2 py-1 text-xs rounded-full border ${badge.cls}`}>
                            {badge.icon} {badge.label}
                          </span>
                        );
                      })()}
                      <span className="text-gray-300 text-sm">
                        {new Date(selectedRunDetail.requested_at).toLocaleString()}
                      </span>
                      <span className="text-gray-500 text-xs">
                        {selectedRunDetail.source_system_name} → {selectedRunDetail.target_system_name}
                      </span>
                      {selectedRunDetail.databricks_run_url && (
                        <a
                          href={selectedRunDetail.databricks_run_url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-purple-400 hover:text-purple-300 underline text-xs ml-auto"
                        >
                          View Notebook →
                        </a>
                      )}
                    </div>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                    <div className="bg-charcoal-600 border border-charcoal-300 rounded p-2.5">
                      <div className="text-gray-500 text-[10px] uppercase tracking-wider mb-0.5">Compare Mode</div>
                      <div className="text-gray-200 text-sm font-medium">{selectedRunDetail.compare_mode}</div>
                    </div>
                    <div className="bg-charcoal-600 border border-charcoal-300 rounded p-2.5">
                      <div className="text-gray-500 text-[10px] uppercase tracking-wider mb-0.5">Duration</div>
                      <div className="text-gray-200 text-sm font-medium">
                        {selectedRunDetail.duration_seconds != null ? `${(selectedRunDetail.duration_seconds / 60).toFixed(1)} min` : '-'}
                      </div>
                    </div>
                    <div className={`border rounded p-2.5 ${
                      selectedRunDetail.row_count_match === false
                        ? 'bg-red-900/20 border-red-700'
                        : selectedRunDetail.row_count_match === true
                        ? 'bg-green-900/20 border-green-700'
                        : 'bg-charcoal-600 border-charcoal-300'
                    }`}>
                      <div className="text-gray-500 text-[10px] uppercase tracking-wider mb-0.5">Row Count (Src / Tgt)</div>
                      <div className={`text-sm font-medium ${
                        selectedRunDetail.row_count_match === false ? 'text-red-300' :
                        selectedRunDetail.row_count_match === true ? 'text-green-300' : 'text-gray-200'
                      }`}>
                        {selectedRunDetail.row_count_source?.toLocaleString() ?? '-'} / {selectedRunDetail.row_count_target?.toLocaleString() ?? '-'}
                      </div>
                    </div>
                    <div className={`border rounded p-2.5 ${
                      (selectedRunDetail.rows_different || 0) > 0
                        ? 'bg-red-900/20 border-red-700'
                        : selectedRunDetail.rows_different === 0
                        ? 'bg-green-900/20 border-green-700'
                        : 'bg-charcoal-600 border-charcoal-300'
                    }`}>
                      <div className="text-gray-500 text-[10px] uppercase tracking-wider mb-0.5">Differences</div>
                      <div className={`text-sm font-medium ${
                        (selectedRunDetail.rows_different || 0) > 0 ? 'text-red-300' :
                        selectedRunDetail.rows_different === 0 ? 'text-green-300' : 'text-gray-200'
                      }`}>
                        {selectedRunDetail.rows_different != null ? `${selectedRunDetail.rows_different.toLocaleString()} (${selectedRunDetail.difference_pct}%)` : '-'}
                      </div>
                    </div>
                  </div>
                  </div>

                  {/* PK columns */}
                  {selectedRunDetail.pk_columns && selectedRunDetail.pk_columns.length > 0 && (
                    <div className="text-xs">
                      <span className="text-gray-500">PK Columns: </span>
                      <span className="text-rust-light font-mono">{selectedRunDetail.pk_columns.join(', ')}</span>
                    </div>
                  )}

                  {/* Error message */}
                  {selectedRunDetail.error_message && (
                    <div className="p-3 bg-orange-900/20 border border-orange-700 rounded">
                      <span className="text-orange-400 text-xs font-semibold">Error: </span>
                      <span className="text-orange-200 text-xs">{selectedRunDetail.error_message}</span>
                    </div>
                  )}

                  {/* Mismatch analysis: show when there are differences or (non-success and we have difference data) */}
                  {((selectedRunDetail.rows_different || 0) > 0 || selectedRunDetail.row_count_match === false || (selectedRunDetail.status !== 'succeeded' && selectedRunDetail.sample_differences)) && (
                    <div>
                      <button
                        onClick={() => setSampleModal(selectedRunDetail)}
                        className="px-4 py-2 rounded text-sm font-medium bg-rust/80 text-white border border-rust-light hover:bg-rust transition-colors cursor-pointer"
                      >
                        View Mismatch Analysis
                      </button>
                    </div>
                  )}

                  {/* Schema match */}
                  {selectedRunDetail.schema_match !== null && selectedRunDetail.schema_match !== undefined && (
                    <div className="text-xs">
                      <span className="text-gray-500">Schema Match: </span>
                      <span className={selectedRunDetail.schema_match ? 'text-green-300' : 'text-red-300'}>
                        {selectedRunDetail.schema_match ? '✓ Yes' : '✗ No'}
                      </span>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Lineage Modal */}
      <LineageModal
        open={lineageModal.open}
        onClose={() => setLineageModal((p) => ({ ...p, open: false }))}
        entityName={entity.name}
        lineage={lineageModal.lineage}
        configuredTables={tables || []}
        onConfigureTable={onConfigureTable}
        onRunAll={onRunAllLineage}
        onNavigateToEntity={onSelectEntity ? (configuredTable) => {
          const srcSys = (systems || []).find(s => s.id === configuredTable.src_system_id);
          const tgtSys = (systems || []).find(s => s.id === configuredTable.tgt_system_id);
          setLineageModal(p => ({ ...p, open: false }));
          onSelectEntity({
            ...configuredTable,
            _entityType: 'table',
            _srcSystemName: srcSys?.name || `System #${configuredTable.src_system_id}`,
            _tgtSystemName: tgtSys?.name || `System #${configuredTable.tgt_system_id}`,
            _srcSystemKind: srcSys?.kind || '',
            _tgtSystemKind: tgtSys?.kind || '',
            _parsedTags: parseTags(configuredTable.tags),
          });
        } : undefined}
        srcSystemKind={entity._srcSystemKind}
        tgtSystemKind={entity._tgtSystemKind}
        srcSystemName={entity._srcSystemName}
        tgtSystemName={entity._tgtSystemName}
        lineageSystem={lineageModal.lineageSystem}
        entityTableType={entityObjectType || ''}
      />

      {/* Sample Differences Modal */}
      {sampleModal && (
        <SampleDifferencesModal
          validation={sampleModal}
          onClose={() => setSampleModal(null)}
        />
      )}

      {/* Toast */}
      {toast && (
        <div className={`fixed top-4 right-4 z-50 max-w-md rounded-lg shadow-2xl border-2 px-4 py-3 flex items-start gap-3 animate-slide-in ${
          toast.type === 'error'
            ? 'bg-red-900/95 border-red-600 text-red-100'
            : toast.type === 'warning'
            ? 'bg-yellow-900/95 border-yellow-600 text-yellow-100'
            : 'bg-blue-900/95 border-blue-600 text-blue-100'
        }`}>
          <span className="flex-1 text-sm">{toast.message}</span>
          <button onClick={() => setToast(null)} className="text-gray-300 hover:text-white flex-shrink-0">✕</button>
        </div>
      )}
    </>
  );
}

// ─── Entity List (main table) ─────────────────────────────────────────

export function AnalysisView({
  tables,
  queries,
  systems,
  schedules,
  tablesLoading,
  queriesLoading,
  tablesError,
  queriesError,
  onClearTablesError,
  onClearQueriesError,
  onRefresh,
  onConfigureTable,
  onRunAllLineage,
  onTrigger,
  onEditTable,
  onEditQuery,
}) {
  const [filterText, setFilterText] = useState('');
  const [filterType, setFilterType] = useState('');
  const [selectedEntity, setSelectedEntity] = useState(null);

  // Keep selectedEntity in sync with latest data after edits/refreshes
  useEffect(() => {
    if (!selectedEntity) return;
    const source = selectedEntity._entityType === 'table' ? tables : queries;
    const fresh = (source || []).find(e => e.id === selectedEntity.id);
    if (fresh && fresh !== selectedEntity) {
      const srcSys = (systems || []).find(s => s.id === fresh.src_system_id);
      const tgtSys = (systems || []).find(s => s.id === fresh.tgt_system_id);
      setSelectedEntity({
        ...fresh,
        _entityType: selectedEntity._entityType,
        _srcSystemName: srcSys?.name || `System #${fresh.src_system_id}`,
        _tgtSystemName: tgtSys?.name || `System #${fresh.tgt_system_id}`,
        _srcSystemKind: srcSys?.kind || '',
        _tgtSystemKind: tgtSys?.kind || '',
        _parsedTags: parseTags(fresh.tags),
      });
    }
  }, [tables, queries, systems]);

  const entities = useMemo(() => {
    const result = [];
    for (const t of (tables || [])) {
      const srcSys = (systems || []).find(s => s.id === t.src_system_id);
      const tgtSys = (systems || []).find(s => s.id === t.tgt_system_id);
      result.push({
        ...t,
        _entityType: 'table',
        _srcSystemName: srcSys?.name || `System #${t.src_system_id}`,
        _tgtSystemName: tgtSys?.name || `System #${t.tgt_system_id}`,
        _srcSystemKind: srcSys?.kind || '',
        _tgtSystemKind: tgtSys?.kind || '',
        _parsedTags: parseTags(t.tags),
      });
    }
    for (const q of (queries || [])) {
      const srcSys = (systems || []).find(s => s.id === q.src_system_id);
      const tgtSys = (systems || []).find(s => s.id === q.tgt_system_id);
      result.push({
        ...q,
        _entityType: 'query',
        _srcSystemName: srcSys?.name || `System #${q.src_system_id}`,
        _tgtSystemName: tgtSys?.name || `System #${q.tgt_system_id}`,
        _srcSystemKind: srcSys?.kind || '',
        _tgtSystemKind: tgtSys?.kind || '',
        _parsedTags: parseTags(q.tags),
      });
    }
    return result;
  }, [tables, queries, systems]);

  const filtered = useMemo(() => {
    let result = entities.filter(e => e.last_run_status === 'failed');
    if (filterText) {
      const term = filterText.toLowerCase();
      result = result.filter(e => e.name.toLowerCase().includes(term));
    }
    if (filterType) {
      result = result.filter(e => e._entityType === filterType);
    }
    return result.sort((a, b) => a.name.localeCompare(b.name));
  }, [entities, filterText, filterType]);

  // If an entity is selected, show the detail view
  if (selectedEntity) {
    return (
      <EntityDetailView
        entity={selectedEntity}
        tables={tables}
        systems={systems}
        schedules={schedules}
        onBack={() => setSelectedEntity(null)}
        onConfigureTable={onConfigureTable}
        onRunAllLineage={onRunAllLineage}
        onRefresh={onRefresh}
        onTrigger={onTrigger}
        onEditTable={onEditTable}
        onEditQuery={onEditQuery}
        onSelectEntity={setSelectedEntity}
      />
    );
  }

  const loading = tablesLoading || queriesLoading;
  const error = tablesError || queriesError;

  return (
    <>
      {error && error.action !== 'setup_required' && (
        <ErrorBox message={error.message} onClose={tablesError ? onClearTablesError : onClearQueriesError} />
      )}

      <div className="mb-4">
        <h2 className="text-3xl font-bold text-rust-light mb-1">Analysis</h2>
        <p className="text-gray-400 text-base">Showing tables and queries whose latest validation run failed. Click any row to investigate.</p>
      </div>

      {loading ? (
        <p className="text-gray-400">Loading...</p>
      ) : (
        <div
          className="bg-charcoal-500 border border-charcoal-200 rounded-lg flex flex-col"
          style={{ minHeight: filtered.length >= 8 ? 'calc(100vh - 350px)' : undefined }}
        >
          {/* Filters */}
          <div className="p-2 bg-charcoal-400 border-b border-charcoal-200">
            <div className="flex items-center gap-2">
              <span className="text-gray-300 text-sm font-semibold">Filters:</span>
              <input
                type="text"
                placeholder="Search by name..."
                value={filterText}
                onChange={(e) => setFilterText(e.target.value)}
                className="px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light flex-1 max-w-xs"
              />
              <select
                value={filterType}
                onChange={(e) => setFilterType(e.target.value)}
                className="px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light cursor-pointer"
              >
                <option value="">All Types</option>
                <option value="table">Tables</option>
                <option value="query">Queries</option>
              </select>
              {(filterText || filterType) && (
                <button
                  onClick={() => { setFilterText(''); setFilterType(''); }}
                  className="px-2 py-1 text-sm rounded bg-red-900/40 text-red-300 border border-red-700 hover:bg-red-900/60 transition-all"
                >
                  Clear
                </button>
              )}
            </div>
          </div>

          {/* Table */}
          <div className="flex-1 min-h-0 overflow-auto">
            <table className="w-full min-w-[1100px]">
              <thead className="sticky top-0 bg-charcoal-400 border-b border-charcoal-200 z-10">
                <tr>
                  <th className="text-left px-3 py-2 text-sm text-gray-300 font-semibold">Display name</th>
                  <th className="text-left px-3 py-2 text-sm text-gray-300 font-semibold">Configure Type</th>
                  <th className="text-left px-3 py-2 text-sm text-gray-300 font-semibold">Tags</th>
                  <th className="text-left px-3 py-2 text-sm text-gray-300 font-semibold">Source system</th>
                  <th className="text-left px-3 py-2 text-sm text-gray-300 font-semibold">Target system</th>
                  <th className="text-left px-3 py-2 text-sm text-gray-300 font-semibold">Source object</th>
                  <th className="text-left px-3 py-2 text-sm text-gray-300 font-semibold">Target object</th>
                  <th className="text-left px-3 py-2 text-sm text-gray-300 font-semibold">Last run</th>
                </tr>
                <tr>
                  <td colSpan={8} className="px-3 py-1 text-xs text-gray-500 bg-charcoal-400 border-b border-charcoal-200">
                    Click a row to open run history and lineage.
                  </td>
                </tr>
              </thead>
              <tbody>
                {filtered.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="text-center p-8 text-gray-500 text-base">
                      {entities.length === 0
                        ? 'No tables or queries configured yet.'
                        : 'No results match the current filters.'}
                    </td>
                  </tr>
                ) : (
                  filtered.map((entity) => {
                    const loadKey = `${entity._entityType}-${entity.id}`;
                    const sourceObject = entity._entityType === 'table'
                      ? `${entity.src_schema || ''}.${entity.src_table || ''}`.replace(/^\./, '') || '—'
                      : (entity.sql || '').substring(0, 60) + ((entity.sql || '').length > 60 ? '...' : '') || '—';
                    const targetObject = entity._entityType === 'table'
                      ? `${entity.tgt_schema || ''}.${entity.tgt_table || ''}`.replace(/^\./, '') || '—'
                      : '—';
                    const lastRunTime = entity.last_run_timestamp
                      ? new Date(entity.last_run_timestamp).toLocaleString()
                      : null;
                    const lastRunStatus = entity.last_run_status;

                    return (
                      <tr
                        key={loadKey}
                        onClick={() => setSelectedEntity(entity)}
                        className="border-b border-charcoal-300/30 even:bg-charcoal-500/50 hover:bg-charcoal-400 transition-colors cursor-pointer"
                      >
                        <td className="px-3 py-2 text-gray-200 font-medium text-sm max-w-[220px]">
                          <div className="truncate" title={entity.name}>{entity.name}</div>
                        </td>
                        <td className="px-3 py-2">
                          <span className={`px-1.5 py-0.5 text-xs rounded-full ${
                            entity._entityType === 'table'
                              ? 'bg-blue-900/40 text-blue-300 border border-blue-700'
                              : 'bg-purple-900/40 text-purple-300 border border-purple-700'
                          }`}>
                            {entity._entityType === 'table' ? 'Table' : 'Query'}
                          </span>
                        </td>
                        <td className="px-3 py-2">
                          <TagList tags={entity._parsedTags} maxVisible={3} />
                        </td>
                        <td className="px-3 py-2 text-sm text-gray-400 whitespace-nowrap">
                          {entity._srcSystemName}
                        </td>
                        <td className="px-3 py-2 text-sm text-gray-400 whitespace-nowrap">
                          {entity._tgtSystemName}
                        </td>
                        <td className="px-3 py-2 text-sm text-gray-400 max-w-[180px] font-mono">
                          <div className="truncate" title={sourceObject}>{sourceObject}</div>
                        </td>
                        <td className="px-3 py-2 text-sm text-gray-400 max-w-[180px] font-mono">
                          <div className="truncate" title={targetObject}>{targetObject}</div>
                        </td>
                        <td className="px-3 py-2 text-sm whitespace-nowrap">
                          {lastRunTime ? (
                            <span className={`text-xs font-medium ${
                              lastRunStatus === 'succeeded' ? 'text-green-300' :
                              lastRunStatus === 'failed' ? 'text-red-300' :
                              lastRunStatus === 'error' ? 'text-orange-300' :
                              'text-gray-400'
                            }`}>
                              {lastRunStatus === 'succeeded' ? '✓' : lastRunStatus === 'failed' ? '✗' : lastRunStatus === 'error' ? '⚠' : '●'}{' '}
                              {lastRunTime}
                            </span>
                          ) : (
                            <span className="text-xs text-gray-500">Never run</span>
                          )}
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </>
  );
}
