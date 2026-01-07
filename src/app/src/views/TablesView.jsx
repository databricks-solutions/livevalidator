import React, { useState, useMemo, useRef, useEffect } from 'react';
import { ErrorBox } from '../components/ErrorBox';
import { TagList, TagBadge } from '../components/TagBadge';
import { BulkTagModal } from '../components/TagInput';
import { Checkbox } from '../components/Checkbox';

export function TablesView({ 
  data, 
  loading, 
  error, 
  systems,
  schedules,
  bindings,
  onEdit, 
  onDelete, 
  onTrigger,
  onUploadCSV,
  onClearError,
  renderCell,
  onNavigateToResult,
  onRefresh
}) {
  const [filterText, setFilterText] = useState('');
  const [filterTags, setFilterTags] = useState([]);
  const [tagInput, setTagInput] = useState('');
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedSuggestionIndex, setSelectedSuggestionIndex] = useState(0);
  const [selectedIds, setSelectedIds] = useState(new Set());
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [showBulkTagModal, setShowBulkTagModal] = useState(false);
  const [bulkTagMode, setBulkTagMode] = useState('add');
  const tagInputRef = useRef(null);
  const inputElementRef = useRef(null);

  // Helper to safely parse tags (handle JSON strings from backend)
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

  // Get all unique tags from data
  const allTags = useMemo(() => {
    const tagSet = new Set();
    data.forEach(row => {
      parseTags(row.tags).forEach(tag => tagSet.add(tag));
    });
    return Array.from(tagSet).sort();
  }, [data]);

  // Filter data based on search text and tags
  const filteredData = useMemo(() => {
    let result = data;
    
    // Apply text filter
    if (filterText) {
      const search = filterText.toLowerCase();
      result = result.filter(row => {
        const srcTable = `${row.src_schema}.${row.src_table}`.toLowerCase();
        const tgtTable = `${row.tgt_schema}.${row.tgt_table}`.toLowerCase();
        const name = row.name?.toLowerCase() || '';
        return srcTable.includes(search) || tgtTable.includes(search) || name.includes(search);
      });
    }
    
    // Apply tag filter (AND logic - must have all selected tags)
    if (filterTags.length > 0) {
      result = result.filter(row => {
        const rowTags = parseTags(row.tags);
        return filterTags.every(filterTag => rowTags.includes(filterTag));
      });
    }
    
    return result;
  }, [data, filterText, filterTags]);

  // Handle select all (only filtered items)
  const handleSelectAll = (checked) => {
    if (checked) {
      setSelectedIds(new Set(filteredData.map(row => row.id)));
    } else {
      setSelectedIds(new Set());
    }
  };

  // Handle individual row selection
  const handleSelectRow = (id, checked) => {
    const newSelected = new Set(selectedIds);
    if (checked) {
      newSelected.add(id);
    } else {
      newSelected.delete(id);
    }
    setSelectedIds(newSelected);
  };

  // Bulk actions
  const handleBulkTrigger = () => {
    selectedIds.forEach(id => onTrigger('table', id));
    setSelectedIds(new Set());
  };

  const handleBulkToggleActive = async (isActive) => {
    try {
      const promises = Array.from(selectedIds).map(async (id) => {
        const row = data.find(r => r.id === id);
        if (row) {
          await fetch(`/api/tables/${id}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              ...row,
              is_active: isActive,
              version: row.version
            })
          });
        }
      });
      await Promise.all(promises);
      if (onRefresh) onRefresh();
      setSelectedIds(new Set());
    } catch (err) {
      console.error('Error toggling active state:', err);
    }
  };

  const handleBulkDelete = () => {
    selectedIds.forEach(id => onDelete('tables', id, true)); // Skip browser confirm - we already showed our modal
    setSelectedIds(new Set());
    setShowDeleteConfirm(false);
  };

  const handleBulkTagSubmit = async (tags) => {
    try {
      const entityIds = Array.from(selectedIds);
      if (bulkTagMode === 'add') {
        await fetch('/api/tags/entity/bulk-add', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ entity_type: 'table', entity_ids: entityIds, tags })
        });
      } else {
        await fetch('/api/tags/entity/bulk-remove', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ entity_type: 'table', entity_ids: entityIds, tags })
        });
      }
      if (onRefresh) onRefresh();
      setSelectedIds(new Set());
    } catch (err) {
      console.error('Error updating tags:', err);
    }
  };

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

  // Filter suggestions based on input
  const tagSuggestions = useMemo(() => {
    if (!tagInput.trim()) return [];
    const input = tagInput.toLowerCase();
    return allTags
      .filter(tag => tag.toLowerCase().includes(input) && !filterTags.includes(tag))
      .slice(0, 10);
  }, [tagInput, allTags, filterTags]);

  // Reset selected index when suggestions change
  useEffect(() => {
    setSelectedSuggestionIndex(0);
  }, [tagSuggestions]);

  const handleTagKeyDown = (e) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSelectedSuggestionIndex(prev => 
        Math.min(prev + 1, tagSuggestions.length - 1)
      );
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
      // Remove last tag when backspace is pressed on empty input
      removeTagFilter(filterTags[filterTags.length - 1]);
    }
  };

  // Close suggestions when clicking outside
  useEffect(() => {
    const handleClickOutside = (e) => {
      if (tagInputRef.current && !tagInputRef.current.contains(e.target)) {
        setShowSuggestions(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const allFilteredSelected = filteredData.length > 0 && filteredData.every(row => selectedIds.has(row.id));
  const someSelected = selectedIds.size > 0;

  return (
    <>
      {error && error.action !== "setup_required" && <ErrorBox message={error.message} onClose={onClearError} />}
      
      <div className="mb-4">
        <h2 className="text-3xl font-bold text-rust-light mb-1">Tables</h2>
        <p className="text-gray-400 text-base">Manage table-to-table validation configurations</p>
      </div>
      
      <div className="mb-3 flex gap-2">
        <button onClick={() => onEdit({})} className="px-3 py-2 text-base bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500 transition-colors font-medium">+ Add Table</button>
        <button onClick={onUploadCSV} className="px-3 py-2 text-base bg-rust text-gray-100 border-0 rounded-md cursor-pointer hover:bg-rust-light transition-colors font-medium">📂 Upload CSV</button>
      </div>

      {loading ? <p className="text-gray-400">Loading…</p> : (
        <div className="bg-charcoal-500 border border-charcoal-200 rounded-lg overflow-hidden">
          {/* Filter and Bulk Actions Bar */}
          <div className="p-2 bg-charcoal-400 border-b border-charcoal-200">
            <div className="flex gap-2">
              {/* Name/Table Filter - 1/3 width */}
              <input
                type="text"
                placeholder="Filter by name or table..."
                value={filterText}
                onChange={(e) => setFilterText(e.target.value)}
                className="w-1/3 px-3 py-2 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light"
              />
              
              {/* Tag Filter - 1/3 width */}
              <div className="relative w-1/3" ref={tagInputRef}>
                <div 
                  className="flex flex-wrap gap-1 items-center px-2 py-1.5 bg-charcoal-600 border border-charcoal-300 rounded min-h-[38px] cursor-text focus-within:border-rust-light"
                  onClick={() => inputElementRef.current?.focus()}
                >
                  {filterTags.map(tag => (
                    <TagBadge key={tag} tag={tag} onRemove={() => removeTagFilter(tag)} />
                  ))}
                  <input
                    ref={inputElementRef}
                    type="text"
                    placeholder={filterTags.length === 0 ? "Filter by tags..." : ""}
                    value={tagInput}
                    onChange={(e) => {
                      setTagInput(e.target.value);
                      setShowSuggestions(true);
                    }}
                    onKeyDown={handleTagKeyDown}
                    onFocus={() => setShowSuggestions(true)}
                    className="flex-1 min-w-[80px] bg-transparent border-0 text-gray-200 text-sm focus:outline-none placeholder-gray-500"
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

              {/* Bulk Actions - 1/3 width */}
              <div className="w-1/3 flex items-center gap-1.5">
                {someSelected ? (
                  <>
                    <span className="text-purple-300 font-medium text-sm whitespace-nowrap mr-1">{selectedIds.size} selected</span>
                    <button 
                      onClick={handleBulkTrigger}
                      className="px-2 py-1 text-sm bg-green-600 text-gray-100 rounded hover:bg-green-500 transition-colors"
                      title="Run selected"
                    >
                      ▶️
                    </button>
                    <button 
                      onClick={() => { setBulkTagMode('add'); setShowBulkTagModal(true); }}
                      className="px-2 py-1 text-sm bg-teal-600 text-gray-100 rounded hover:bg-teal-500 transition-colors whitespace-nowrap"
                    >
                      + Tags
                    </button>
                    <button 
                      onClick={() => { setBulkTagMode('remove'); setShowBulkTagModal(true); }}
                      className="px-2 py-1 text-sm bg-orange-600 text-gray-100 rounded hover:bg-orange-500 transition-colors whitespace-nowrap"
                    >
                      − Tags
                    </button>
                    <button 
                      onClick={() => handleBulkToggleActive(true)}
                      className="px-2 py-1 text-sm bg-blue-600 text-gray-100 rounded hover:bg-blue-500 transition-colors whitespace-nowrap"
                    >
                      Enable
                    </button>
                    <button 
                      onClick={() => handleBulkToggleActive(false)}
                      className="px-2 py-1 text-sm bg-gray-600 text-gray-100 rounded hover:bg-gray-500 transition-colors whitespace-nowrap"
                    >
                      Disable
                    </button>
                    <button 
                      onClick={() => setShowDeleteConfirm(true)}
                      className="px-2 py-1 text-sm bg-red-600 text-gray-100 rounded hover:bg-red-500 transition-colors"
                    >
                      🗑️
                    </button>
                    <button 
                      onClick={() => setSelectedIds(new Set())}
                      className="px-2 py-1 text-sm bg-charcoal-600 text-gray-300 rounded hover:bg-charcoal-500 transition-colors ml-auto whitespace-nowrap"
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

      {/* Delete confirmation modal */}
      {showDeleteConfirm && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={() => setShowDeleteConfirm(false)}>
          <div className="bg-charcoal-500 border border-red-700 rounded-lg p-4 max-w-md" onClick={e => e.stopPropagation()}>
            <h3 className="text-xl font-bold text-red-400 mb-2">⚠️ Confirm Delete</h3>
            <p className="text-gray-300 mb-4">
              Are you sure you want to delete <strong>{selectedIds.size}</strong> table{selectedIds.size !== 1 ? 's' : ''}? This action cannot be undone.
            </p>
            <div className="flex gap-2 justify-end">
              <button 
                onClick={() => setShowDeleteConfirm(false)}
                className="px-3 py-1.5 bg-charcoal-600 text-gray-200 rounded hover:bg-charcoal-500"
              >
                Cancel
              </button>
              <button 
                onClick={handleBulkDelete}
                className="px-3 py-1.5 bg-red-600 text-white rounded hover:bg-red-500"
              >
                Delete {selectedIds.size} item{selectedIds.size !== 1 ? 's' : ''}
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
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold max-w-xs">Table</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold w-32">Last Run</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold w-40">Source</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold w-40">Target</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold">Compare Mode</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold">PK Columns</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold">Schedules</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold">Tags</th>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold">Actions</th>
                </tr>
              </thead>
              <tbody>
              {filteredData.map(row => {
                const entityBindings = bindings[`dataset_${row.id}`] || [];
                const scheduleNames = entityBindings.map(b => schedules.find(s => s.id === b.schedule_id)?.name).filter(Boolean).join(', ');
                const srcTable = `${row.src_schema}.${row.src_table}`;
                const tgtTable = `${row.tgt_schema}.${row.tgt_table}`;
                const tablesMatch = srcTable === tgtTable;
                
                const isSelected = selectedIds.has(row.id);
                
                return (
                  <tr 
                    key={row.id} 
                    className={`border-b border-charcoal-300/30 hover:bg-charcoal-400/50 transition-colors ${isSelected ? 'bg-purple-900/20' : ''}`}
                  >
                    <td className="px-2 py-1 text-sm">
                      <Checkbox
                        checked={isSelected}
                        onChange={(e) => handleSelectRow(row.id, e.target.checked)}
                      />
                    </td>
                    <td className="px-2 py-1 text-sm">
                      <div className="flex flex-col gap-0.5">
                        {tablesMatch ? (
                          <span className="text-gray-100 whitespace-nowrap">{srcTable}</span>
                        ) : (
                          <div className="flex flex-col text-gray-100">
                            <span className="whitespace-nowrap">src: {srcTable}</span>
                            <span className="whitespace-nowrap">tgt: {tgtTable}</span>
                          </div>
                        )}
                        <span className="text-gray-500 text-xs whitespace-nowrap">{row.name}</span>
                      </div>
                    </td>
                    <td className="px-2 py-1">
                      {row.last_run_status === 'succeeded' ? (
                        <button
                          onClick={() => onNavigateToResult(row.last_run_id)}
                          className="px-1.5 py-0.5 text-sm rounded-full bg-green-900/40 text-green-300 border border-green-700 whitespace-nowrap hover:bg-green-900/60 transition-colors"
                          title={`Last run: ${new Date(row.last_run_timestamp).toLocaleString()}`}
                        >
                          ✓ Success
                        </button>
                      ) : row.last_run_status === 'failed' ? (
                        <button
                          onClick={() => onNavigateToResult(row.last_run_id)}
                          className="px-1.5 py-0.5 text-sm rounded-full bg-red-900/40 text-red-300 border border-red-700 whitespace-nowrap hover:bg-red-900/60 transition-colors"
                          title={`Last run: ${new Date(row.last_run_timestamp).toLocaleString()}`}
                        >
                          ✗ Failed
                        </button>
                      ) : (
                        <span className="px-1.5 py-0.5 text-sm rounded-full bg-gray-900/40 text-gray-500 border border-gray-700 whitespace-nowrap">
                          No recent
                        </span>
                      )}
                    </td>
                    <td className="px-2 py-1 text-gray-100 text-sm w-40">{renderCell('tables', row, 'src_system_id', systems)}</td>
                    <td className="px-2 py-1 text-gray-100 text-sm w-40">{renderCell('tables', row, 'tgt_system_id', systems)}</td>
                    <td className="px-2 py-1 text-gray-300 text-sm whitespace-nowrap">{row.compare_mode}</td>
                    <td className="px-2 py-1 text-gray-300 text-sm">{row.pk_columns?.join(', ') || '-'}</td>
                    <td className="px-2 py-1 text-purple-400 text-sm">{scheduleNames || '-'}</td>
                    <td className="px-2 py-1">
                      <TagList tags={parseTags(row.tags)} maxVisible={3} />
                    </td>
                    <td className="px-2 py-1 whitespace-nowrap">
                      <button onClick={() => onEdit(row)} className="px-1.5 py-0.5 text-sm bg-purple-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-purple-500 mr-1">Edit</button>
                      <button onClick={() => onDelete('tables', row.id)} className="px-1.5 py-0.5 text-sm bg-red-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-red-500 mr-1">Del</button>
                      <button onClick={() => onTrigger('table', row.id)} className="px-1.5 py-0.5 text-sm bg-green-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-green-500">▶️</button>
                    </td>
                  </tr>
                );
              })}
              </tbody>
            </table>
          </div>
        </div>
      )}
      
      {/* Bulk Tag Modal */}
      <BulkTagModal
        isOpen={showBulkTagModal}
        onClose={() => setShowBulkTagModal(false)}
        mode={bulkTagMode}
        onSubmit={handleBulkTagSubmit}
        entityCount={selectedIds.size}
      />
    </>
  );
}
