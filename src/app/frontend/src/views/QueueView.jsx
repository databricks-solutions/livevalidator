import React, { useState, useMemo, useRef, useEffect } from 'react';
import { ErrorBox } from '../components/ErrorBox';
import { Checkbox } from '../components/Checkbox';
import { TagList, TagBadge } from '../components/TagBadge';
import { apiCall, triggerService } from '../services/api';

export function QueueView({ 
  triggers, 
  queueStats, 
  onRefresh,
  showNotification 
}) {
  const [filterText, setFilterText] = useState('');
  const [filterStatus, setFilterStatus] = useState('');
  const [filterTags, setFilterTags] = useState([]);
  const [tagInput, setTagInput] = useState('');
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedSuggestionIndex, setSelectedSuggestionIndex] = useState(0);
  const [selectedIds, setSelectedIds] = useState(new Set());
  const [showCancelConfirm, setShowCancelConfirm] = useState(false);
  const [launching, setLaunching] = useState(new Set());
  const [repairing, setRepairing] = useState(new Set());
  const tagInputRef = useRef(null);
  const inputElementRef = useRef(null);

  const isStale = (trigger) => {
    if (trigger.status !== 'running' || !trigger.started_at) return false;
    const startedAt = new Date(trigger.started_at);
    const hourAgo = Date.now() - 60 * 60 * 1000;
    return startedAt.getTime() < hourAgo;
  };

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

  // Get all unique tags from triggers
  const allTags = useMemo(() => {
    const tagSet = new Set();
    triggers.data.forEach(t => parseTags(t.entity_tags).forEach(tag => tagSet.add(tag)));
    return Array.from(tagSet).sort();
  }, [triggers.data]);

  // Filter triggers
  const filteredData = useMemo(() => {
    let result = triggers.data || [];
    
    if (filterText) {
      const search = filterText.toLowerCase();
      result = result.filter(t => 
        (t.entity_name || '').toLowerCase().includes(search) ||
        (t.requested_by || '').toLowerCase().includes(search)
      );
    }
    
    if (filterStatus) {
      result = result.filter(t => t.status === filterStatus);
    }
    
    if (filterTags.length > 0) {
      result = result.filter(t => {
        const tags = parseTags(t.entity_tags);
        return filterTags.every(ft => tags.includes(ft));
      });
    }
    
    return result;
  }, [triggers.data, filterText, filterStatus, filterTags]);

  // Selection handlers
  const handleSelectAll = (checked) => {
    if (checked) {
      setSelectedIds(new Set(filteredData.map(t => t.id)));
    } else {
      setSelectedIds(new Set());
    }
  };

  const handleSelectRow = (id, checked) => {
    const newSelected = new Set(selectedIds);
    if (checked) {
      newSelected.add(id);
    } else {
      newSelected.delete(id);
    }
    setSelectedIds(newSelected);
  };

  // Actions
  const handleCancel = async (triggerId) => {
    if (confirm('Cancel this validation?')) {
      try {
        await apiCall('DELETE', `/api/triggers/${triggerId}`);
        onRefresh();
      } catch (e) {
        alert('Failed to cancel: ' + e.message);
      }
    }
  };

  const handleLaunch = async (triggerId) => {
    setLaunching(prev => new Set([...prev, triggerId]));
    try {
      const result = await triggerService.launch(triggerId);
      if (result.launched) {
        onRefresh();
      } else {
        alert(`Could not launch: ${result.reason}`);
      }
    } catch (e) {
      alert('Failed to launch: ' + e.message);
    } finally {
      setLaunching(prev => {
        const next = new Set(prev);
        next.delete(triggerId);
        return next;
      });
    }
  };

  const handleBulkLaunch = async () => {
    const queuedIds = Array.from(selectedIds).filter(id => {
      const t = triggers.data.find(tr => tr.id === id);
      return t && t.status === 'queued';
    });
    
    if (queuedIds.length === 0) {
      alert('No queued triggers selected');
      return;
    }
    
    setLaunching(prev => new Set([...prev, ...queuedIds]));
    try {
      const result = await triggerService.bulkLaunch(queuedIds);
      const launched = result.results.filter(r => r.launched).length;
      const failed = result.results.filter(r => !r.launched);
      
      if (failed.length > 0) {
        const reasons = [...new Set(failed.map(f => f.reason))].join(', ');
        alert(`Launched ${launched}/${queuedIds.length}. Some failed: ${reasons}`);
      }
      
      onRefresh();
      setSelectedIds(new Set());
    } catch (e) {
      alert('Bulk launch failed: ' + e.message);
    } finally {
      setLaunching(new Set());
    }
  };

  const handleBulkCancel = async () => {
    try {
      await triggerService.bulkCancel(Array.from(selectedIds));
      onRefresh();
      setSelectedIds(new Set());
      setShowCancelConfirm(false);
    } catch (e) {
      alert('Bulk cancel failed: ' + e.message);
    }
  };

  const handleBulkRepair = async () => {
    // Get failed trigger IDs from selected
    const failedIds = Array.from(selectedIds).filter(id => {
      const t = triggers.data.find(tr => tr.id === id);
      return t && t.databricks_run_status?.failed && t.databricks_run_id;
    });
    
    if (failedIds.length === 0) {
      showNotification?.('No failed triggers selected', 'error');
      return;
    }
    
    setRepairing(prev => new Set([...prev, ...failedIds]));
    try {
      const result = await triggerService.bulkRepair(failedIds);
      const repaired = result.results.filter(r => r.repaired).length;
      const failed = result.results.filter(r => !r.repaired);
      
      if (failed.length > 0) {
        const reasons = [...new Set(failed.map(f => f.reason))].slice(0, 3).join(', ');
        showNotification?.(`Repaired ${repaired}/${failedIds.length}. Some failed: ${reasons}`, 'warning');
      } else {
        showNotification?.(`Repaired ${repaired} trigger(s)`, 'success');
      }
      
      onRefresh();
      setSelectedIds(new Set());
    } catch (e) {
      showNotification?.('Bulk repair failed: ' + e.message, 'error');
    } finally {
      setRepairing(new Set());
    }
  };

  const handleRepair = async (triggerId) => {
    setRepairing(prev => new Set([...prev, triggerId]));
    try {
      const result = await triggerService.repair(triggerId);
      if (result.repaired) {
        showNotification?.('Repair initiated - failed tasks will be re-executed', 'success');
        onRefresh();
      } else {
        showNotification?.(`Repair failed: ${result.reason}`, 'error');
      }
    } catch (e) {
      showNotification?.(`Repair error: ${e.message}`, 'error');
    } finally {
      setRepairing(prev => {
        const next = new Set(prev);
        next.delete(triggerId);
        return next;
      });
    }
  };

  // Tag filter handlers
  const addTagFilter = (tag) => {
    if (tag && !filterTags.includes(tag)) {
      setFilterTags(prev => [...prev, tag]);
    }
    setTagInput('');
    setShowSuggestions(false);
    setSelectedSuggestionIndex(0);
  };

  const removeTagFilter = (tag) => {
    setFilterTags(prev => prev.filter(t => t !== tag));
  };

  const tagSuggestions = useMemo(() => {
    if (!tagInput.trim()) return [];
    const input = tagInput.toLowerCase();
    return allTags
      .filter(tag => tag.toLowerCase().includes(input) && !filterTags.includes(tag))
      .slice(0, 10);
  }, [tagInput, allTags, filterTags]);

  useEffect(() => {
    setSelectedSuggestionIndex(0);
  }, [tagSuggestions]);

  const handleTagKeyDown = (e) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSelectedSuggestionIndex(prev => Math.min(prev + 1, tagSuggestions.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setSelectedSuggestionIndex(prev => Math.max(prev - 1, 0));
    } else if (e.key === 'Enter') {
      e.preventDefault();
      if (tagSuggestions.length > 0) {
        addTagFilter(tagSuggestions[selectedSuggestionIndex]);
      }
    } else if (e.key === 'Escape') {
      setShowSuggestions(false);
      setTagInput('');
    } else if (e.key === 'Backspace' && !tagInput && filterTags.length > 0) {
      removeTagFilter(filterTags[filterTags.length - 1]);
    }
  };

  useEffect(() => {
    const handleClickOutside = (e) => {
      if (tagInputRef.current && !tagInputRef.current.contains(e.target)) {
        setShowSuggestions(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const allFilteredSelected = filteredData.length > 0 && filteredData.every(t => selectedIds.has(t.id));
  const someSelected = selectedIds.size > 0;
  const selectedQueuedCount = Array.from(selectedIds).filter(id => {
    const t = triggers.data.find(tr => tr.id === id);
    return t && t.status === 'queued';
  }).length;

  const selectedFailedCount = Array.from(selectedIds).filter(id => {
    const t = triggers.data.find(tr => tr.id === id);
    return t && t.databricks_run_status?.failed && t.databricks_run_id;
  }).length;

  return (
    <>
      {triggers.error && triggers.error.action !== "setup_required" && <ErrorBox message={triggers.error.message} onClose={triggers.clearError} />}
      
      <div className="mb-4">
        <h2 className="text-3xl font-bold text-rust-light mb-1">Validation Queue</h2>
        <p className="text-gray-400 text-base">Active validation jobs in the queue</p>
      </div>

      {/* Queue Stats */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
        <div className="bg-blue-900/20 border border-blue-700 rounded-lg p-3">
          <div className="text-blue-400 text-sm mb-1">Queued</div>
          <div className="text-2xl font-bold text-blue-300">
            {queueStats.data?.active?.queued || 0}
          </div>
        </div>
        <div className="bg-orange-900/20 border border-orange-700 rounded-lg p-3 animate-pulse">
          <div className="text-orange-400 text-sm mb-1">Running</div>
          <div className="text-2xl font-bold text-orange-300">
            {queueStats.data?.active?.running || 0}
          </div>
        </div>
        <div className="bg-green-900/20 border border-green-700 rounded-lg p-3">
          <div className="text-green-400 text-sm mb-1">Completed (1h)</div>
          <div className="text-2xl font-bold text-green-300">
            {queueStats.data?.recent_1h?.succeeded || 0}
          </div>
        </div>
      </div>

      <div className="mb-3 flex gap-2">
        <button onClick={onRefresh} className="px-3 py-2 text-base bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500 transition-colors font-medium ml-auto">Refresh</button>
      </div>

      {triggers.loading ? <p className="text-gray-400">Loading…</p> : (
        <div className="bg-charcoal-500 border border-charcoal-200 rounded-lg overflow-hidden">
          {/* Filter and Bulk Actions Bar */}
          <div className="p-2 bg-charcoal-400 border-b border-charcoal-200">
            <div className="flex gap-2 items-center">
              {/* Name Filter */}
              <input
                type="text"
                placeholder="Filter by name..."
                value={filterText}
                onChange={(e) => setFilterText(e.target.value)}
                className="w-64 px-3 py-1.5 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light"
              />
              
              {/* Status Filter */}
              <select
                value={filterStatus}
                onChange={(e) => setFilterStatus(e.target.value)}
                className="w-32 px-2 py-1.5 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light cursor-pointer"
              >
                <option value="">All Status</option>
                <option value="queued">Queued</option>
                <option value="running">Running</option>
              </select>
              
              {/* Tag Filter */}
              <div className="relative w-40" ref={tagInputRef}>
                <div 
                  className="flex flex-wrap gap-1 items-center px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded min-h-[34px] cursor-text focus-within:border-rust-light"
                  onClick={() => inputElementRef.current?.focus()}
                >
                  {filterTags.map(tag => (
                    <TagBadge key={tag} tag={tag} onRemove={() => removeTagFilter(tag)} />
                  ))}
                  <input
                    ref={inputElementRef}
                    type="text"
                    placeholder={filterTags.length === 0 ? "Tags..." : ""}
                    value={tagInput}
                    onChange={(e) => {
                      setTagInput(e.target.value);
                      setShowSuggestions(true);
                    }}
                    onKeyDown={handleTagKeyDown}
                    onFocus={() => setShowSuggestions(true)}
                    className="flex-1 min-w-[40px] bg-transparent border-0 text-gray-200 text-sm focus:outline-none placeholder-gray-500"
                  />
                </div>
                {showSuggestions && tagSuggestions.length > 0 && (
                  <div className="absolute z-10 w-full mt-1 bg-charcoal-600 border border-charcoal-300 rounded shadow-lg max-h-48 overflow-y-auto">
                    {tagSuggestions.map((tag, idx) => (
                      <button
                        key={tag}
                        onClick={() => addTagFilter(tag)}
                        className={`w-full text-left px-3 py-2 text-sm transition-colors ${
                          idx === selectedSuggestionIndex
                            ? 'bg-purple-600 text-white'
                            : 'text-gray-200 hover:bg-charcoal-500'
                        }`}
                      >
                        {tag}
                      </button>
                    ))}
                  </div>
                )}
              </div>

              {/* Bulk Actions */}
              <div className="ml-auto flex items-center gap-1.5">
                {someSelected ? (
                  <>
                    <span className="text-purple-300 font-medium text-sm whitespace-nowrap mr-1">{selectedIds.size} selected</span>
                    {selectedQueuedCount > 0 && (
                      <button 
                        onClick={handleBulkLaunch}
                        className="px-2 py-1 text-sm bg-green-600 text-gray-100 rounded hover:bg-green-500 transition-colors whitespace-nowrap"
                        title={`Launch ${selectedQueuedCount} queued triggers`}
                      >
                        Launch ({selectedQueuedCount})
                      </button>
                    )}
                    {selectedFailedCount > 0 && (
                      <button 
                        onClick={handleBulkRepair}
                        className="px-2 py-1 text-sm bg-yellow-600 text-gray-100 rounded hover:bg-yellow-500 transition-colors whitespace-nowrap"
                        title={`Repair ${selectedFailedCount} failed triggers`}
                      >
                        Repair ({selectedFailedCount})
                      </button>
                    )}
                    <button 
                      onClick={() => setShowCancelConfirm(true)}
                      className="px-2 py-1 text-sm bg-red-600 text-gray-100 rounded hover:bg-red-500 transition-colors"
                    >
                      Cancel
                    </button>
                    <button 
                      onClick={() => setSelectedIds(new Set())}
                      className="px-2 py-1 text-sm bg-charcoal-600 text-gray-300 rounded hover:bg-charcoal-500 transition-colors whitespace-nowrap"
                    >
                      Clear
                    </button>
                  </>
                ) : (
                  <span className="text-gray-500 text-sm">Select items for bulk actions</span>
                )}
              </div>
            </div>
          </div>

          {/* Cancel confirmation modal */}
          {showCancelConfirm && (
            <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={() => setShowCancelConfirm(false)}>
              <div className="bg-charcoal-500 border border-red-700 rounded-lg p-4 max-w-md" onClick={e => e.stopPropagation()}>
                <h3 className="text-xl font-bold text-red-400 mb-2">Confirm Cancel</h3>
                <p className="text-gray-300 mb-4">
                  Are you sure you want to cancel <strong>{selectedIds.size}</strong> trigger{selectedIds.size !== 1 ? 's' : ''}?
                </p>
                <div className="flex gap-2 justify-end">
                  <button 
                    onClick={() => setShowCancelConfirm(false)}
                    className="px-3 py-1.5 bg-charcoal-600 text-gray-200 rounded hover:bg-charcoal-500"
                  >
                    No, Keep
                  </button>
                  <button 
                    onClick={handleBulkCancel}
                    className="px-3 py-1.5 bg-red-600 text-white rounded hover:bg-red-500"
                  >
                    Yes, Cancel {selectedIds.size}
                  </button>
                </div>
              </div>
            </div>
          )}

          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-charcoal-400 border-b border-charcoal-200">
                <tr>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold w-10">
                    <Checkbox
                      checked={allFilteredSelected}
                      onChange={(e) => handleSelectAll(e.target.checked)}
                    />
                  </th>
                  <th className="text-center px-2 py-1.5 text-sm text-gray-300 font-semibold w-24">Status</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold">Name</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold w-24">Type</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold w-40">Requested By</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold w-36">Queued At</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold">Tags</th>
                  <th className="text-center px-2 py-1.5 text-sm text-gray-300 font-semibold">Run</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold w-24">Actions</th>
                </tr>
              </thead>
              <tbody>
                {filteredData.length === 0 ? (
                  <tr>
                    <td colSpan={9} className="px-4 py-8 text-center text-gray-500">
                      Queue is empty. No active validation jobs.
                    </td>
                  </tr>
                ) : (
                  filteredData.map((trigger) => {
                    const isSelected = selectedIds.has(trigger.id);
                    const isLaunching = launching.has(trigger.id);
                    const stale = isStale(trigger);
                    const runStatus = trigger.databricks_run_status;
                    const runFailed = runStatus?.failed;
                    const runDone = runStatus?.done;
                    
                    return (
                      <tr 
                        key={trigger.id}
                        className={`border-b border-charcoal-300/30 hover:bg-charcoal-400/50 transition-colors ${
                          isSelected ? 'bg-purple-900/20' : ''
                        } ${runFailed ? 'bg-red-900/20' : ''} ${trigger.status === 'running' && !runDone ? 'animate-pulse' : ''}`}
                      >
                        <td className="px-2 py-1.5 text-sm">
                          <Checkbox
                            checked={isSelected}
                            onChange={(e) => handleSelectRow(trigger.id, e.target.checked)}
                          />
                        </td>
                        <td className="px-2 py-1.5 text-center">
                          {runFailed ? (
                            <span 
                              className="px-2 py-0.5 text-xs font-medium rounded-full bg-red-600 text-white"
                              title={runStatus?.state_message || runStatus?.result_state || 'Run failed'}
                            >
                              FAILED
                            </span>
                          ) : (
                            <span className={`px-2 py-0.5 text-xs font-medium rounded-full ${
                              trigger.status === 'running'
                                ? 'bg-orange-600 text-white'
                                : 'bg-blue-600 text-white'
                            }`}>
                              {trigger.status === 'running' ? 'RUNNING' : 'QUEUED'}
                            </span>
                          )}
                          {stale && !runFailed && (
                            <span title="Long-running or stale" className="ml-1 text-yellow-400">⚠</span>
                          )}
                        </td>
                        <td className="px-2 py-1.5 text-sm text-gray-100">
                          {trigger.entity_name || `${trigger.entity_type} #${trigger.entity_id}`}
                        </td>
                        <td className="px-2 py-1.5">
                          <span className={`px-2 py-0.5 text-xs rounded-full ${
                            trigger.entity_type === 'table'
                              ? 'bg-blue-900/40 text-blue-300 border border-blue-700'
                              : 'bg-purple-900/40 text-purple-300 border border-purple-700'
                          }`}>
                            {trigger.entity_type}
                          </span>
                        </td>
                        <td className="px-2 py-1.5 text-sm text-gray-300">{trigger.requested_by}</td>
                        <td className="px-2 py-1.5 text-sm text-gray-300">
                          {new Date(trigger.requested_at).toLocaleString()}
                        </td>
                        <td className="px-2 py-1.5">
                          <TagList tags={parseTags(trigger.entity_tags)} maxVisible={2} />
                        </td>
                        <td className="px-2 py-1.5 text-center whitespace-nowrap">
                          {trigger.databricks_run_url ? (
                            <a
                              href={trigger.databricks_run_url}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="text-orange-400 hover:text-orange-300 underline text-sm"
                            >
                              View Run
                            </a>
                          ) : (
                            <span className="text-gray-500 text-sm">—</span>
                          )}
                        </td>
                        <td className="px-2 py-1.5 whitespace-nowrap">
                          {trigger.status === 'queued' && (
                            <button 
                              onClick={() => handleLaunch(trigger.id)}
                              disabled={isLaunching}
                              className={`px-1.5 py-0.5 text-sm bg-green-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-green-500 mr-1 ${isLaunching ? 'opacity-50' : ''}`}
                            >
                              {isLaunching ? '...' : '▶'}
                            </button>
                          )}
                          {runFailed && trigger.databricks_run_id && (
                            <button 
                              onClick={() => handleRepair(trigger.id)}
                              disabled={repairing.has(trigger.id)}
                              className={`px-2 py-0.5 text-sm bg-yellow-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-yellow-500 mr-1 ${repairing.has(trigger.id) ? 'opacity-50' : ''}`}
                              title={`Repair: ${runStatus?.result_state || 'failed'}`}
                            >
                              {repairing.has(trigger.id) ? '...' : 'Repair'}
                            </button>
                          )}
                          <button 
                            onClick={() => handleCancel(trigger.id)}
                            className="px-1.5 py-0.5 text-sm bg-red-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-red-500"
                          >
                            ✕
                          </button>
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
