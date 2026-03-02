import React, { useState, useMemo, useRef, useEffect } from 'react';
import { ErrorBox } from './ErrorBox';
import { TagList, TagBadge } from './TagBadge';
import { BulkTagModal } from './TagInput';
import { Checkbox } from './Checkbox';
import { useTagFilter } from '../hooks/useTagFilter';
import { useSelection } from '../hooks/useSelection';
import { parseArray } from '../utils/arrays';

/**
 * Shared list view component for Tables and Queries.
 * Config-driven to handle differences between entity types.
 */
export function EntityListView({
  // Data
  data,
  loading,
  error,
  systems,
  // Callbacks
  onEdit,
  onDelete,
  onTrigger,
  onBulkTrigger,
  onUploadCSV,
  onClearError,
  renderCell,
  onNavigateToResult,
  onRefresh,
  highlightEntityId,
  onClearEntityHighlight,
  // Configuration
  config
}) {
  const {
    entityType,        // 'table' or 'query'
    entityTypePlural,  // 'tables' or 'queries'
    title,
    subtitle,
    addButtonLabel,
    triggerAction,     // 'table' or 'compare_query'
    apiEndpoint,       // '/api/tables' or '/api/queries'
    filterFn,          // Custom filter function
    exportHeaders,
    exportRowFn,
    exportFilename,
    columns,           // Column definitions
    renderRow,         // Custom row renderer
    deleteConfirmText, // Function to generate delete text
  } = config;

  const [filterText, setFilterText] = useState('');
  const [filterStatus, setFilterStatus] = useState('');
  const { selectedIds, handleSelectAll, handleSelectRow, clearSelection, selectedArray } = useSelection();
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [showBulkTagModal, setShowBulkTagModal] = useState(false);
  const [bulkTagMode, setBulkTagMode] = useState('add');
  const highlightedRowRef = useRef(null);

  useEffect(() => {
    if (highlightEntityId && highlightedRowRef.current) {
      highlightedRowRef.current.scrollIntoView({ behavior: 'smooth', block: 'center' });
      const timer = setTimeout(() => {
        if (onClearEntityHighlight) onClearEntityHighlight();
      }, 3000);
      return () => clearTimeout(timer);
    }
  }, [highlightEntityId, onClearEntityHighlight]);

  const allTags = useMemo(() => {
    const tagSet = new Set();
    data.forEach(row => parseArray(row.tags).forEach(tag => tagSet.add(tag)));
    return Array.from(tagSet).sort();
  }, [data]);

  const {
    filterTags, tagInput, setTagInput, showSuggestions, setShowSuggestions,
    selectedSuggestionIndex, tagInputRef, inputElementRef, tagSuggestions,
    addTagFilter, removeTagFilter, handleTagKeyDown, filterByTags,
  } = useTagFilter(allTags);

  const filteredData = useMemo(() => {
    let result = data;
    if (filterText) {
      result = result.filter(row => filterFn(row, filterText.toLowerCase()));
    }
    if (filterStatus) {
      result = filterStatus === 'none'
        ? result.filter(row => !row.last_run_status)
        : result.filter(row => row.last_run_status === filterStatus);
    }
    result = filterByTags(result, row => parseArray(row.tags));
    result.sort((a, b) => (a.is_active === b.is_active ? 0 : a.is_active ? -1 : 1));
    return result;
  }, [data, filterText, filterStatus, filterByTags, filterFn]);

  const handleBulkTrigger = () => {
    if (onBulkTrigger) {
      onBulkTrigger(selectedArray);
    } else {
      selectedIds.forEach(id => onTrigger(triggerAction, id));
    }
    clearSelection();
  };

  const handleBulkToggleActive = async (isActive) => {
    try {
      const promises = selectedArray.map(async (id) => {
        const row = data.find(r => r.id === id);
        if (row) {
          const response = await fetch(`${apiEndpoint}/${id}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ is_active: isActive, version: row.version })
          });
          if (!response.ok) console.error(`Failed to update ${entityType} ${id}`);
        }
      });
      await Promise.all(promises);
      if (onRefresh) onRefresh();
      clearSelection();
    } catch (err) {
      console.error('Error toggling active state:', err);
    }
  };

  const handleBulkDelete = () => {
    selectedIds.forEach(id => onDelete(entityTypePlural, id, true));
    clearSelection();
    setShowDeleteConfirm(false);
  };

  const handleBulkTagSubmit = async (tags) => {
    try {
      const endpoint = bulkTagMode === 'add' ? '/api/tags/entity/bulk-add' : '/api/tags/entity/bulk-remove';
      await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ entity_type: entityType, entity_ids: selectedArray, tags })
      });
      if (onRefresh) onRefresh();
      clearSelection();
    } catch (err) {
      console.error('Error updating tags:', err);
    }
  };

  const allFilteredSelected = filteredData.length > 0 && filteredData.every(row => selectedIds.has(row.id));
  const someSelected = selectedIds.size > 0;

  const handleExportCSV = () => {
    const rowsToExport = someSelected ? data.filter(row => selectedIds.has(row.id)) : data;
    if (rowsToExport.length === 0) return;

    const escapeCSV = (val) => {
      if (val === null || val === undefined) return '';
      const str = String(val);
      return (str.includes(',') || str.includes('"') || str.includes('\n')) ? `"${str.replace(/"/g, '""')}"` : str;
    };

    const rows = rowsToExport.map(row => exportRowFn(row, systems).map(escapeCSV).join(','));
    const csv = [exportHeaders.join(','), ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const now = new Date();
    const timestamp = `${now.getFullYear()}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}${String(now.getHours()).padStart(2,'0')}${String(now.getMinutes()).padStart(2,'0')}`;
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `${exportFilename}_${timestamp}.csv`;
    a.click();
  };

  return (
    <>
      {error && error.action !== "setup_required" && <ErrorBox message={error.message} onClose={onClearError} />}
      
      <div className="mb-4">
        <h2 className="text-3xl font-bold text-rust-light mb-1">{title}</h2>
        <p className="text-gray-400 text-base">{subtitle}</p>
      </div>
      
      <div className="mb-3 flex gap-2">
        <button onClick={() => onEdit({})} className="px-3 py-2 text-base bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500 transition-colors font-medium">{addButtonLabel}</button>
        <button onClick={onUploadCSV} className="px-3 py-2 text-base bg-rust text-gray-100 border-0 rounded-md cursor-pointer hover:bg-rust-light transition-colors font-medium">Upload CSV</button>
        <button onClick={handleExportCSV} className="px-3 py-2 text-base bg-blue-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-blue-500 transition-colors font-medium">
          {someSelected ? `Export Selected (${selectedIds.size})` : 'Export CSV'}
        </button>
        <button onClick={onRefresh} className="px-3 py-2 text-base bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500 transition-colors font-medium ml-auto">Refresh</button>
      </div>

      {loading ? <p className="text-gray-400">Loading…</p> : (
        <div className="bg-charcoal-500 border border-charcoal-200 rounded-lg overflow-hidden">
          {/* Filter and Bulk Actions Bar */}
          <div className="p-2 bg-charcoal-400 border-b border-charcoal-200">
            <div className="flex gap-2 items-center">
              <input
                type="text"
                placeholder="Filter by name..."
                value={filterText}
                onChange={(e) => setFilterText(e.target.value)}
                className="w-[36rem] px-3 py-1.5 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light"
              />
              
              <select
                value={filterStatus}
                onChange={(e) => setFilterStatus(e.target.value)}
                className="w-36 px-2 py-1.5 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light cursor-pointer"
              >
                <option value="">All Statuses</option>
                <option value="succeeded">Succeeded</option>
                <option value="failed">Failed</option>
                <option value="error">Error</option>
                <option value="none">No runs</option>
              </select>
              
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
                    onChange={(e) => { setTagInput(e.target.value); setShowSuggestions(true); }}
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
                        className={`w-full text-left px-3 py-2 text-sm transition-colors ${idx === selectedSuggestionIndex ? 'bg-purple-600 text-white' : 'text-gray-200 hover:bg-charcoal-500'}`}
                      >
                        {tag}
                      </button>
                    ))}
                  </div>
                )}
              </div>

              <div className="ml-auto flex items-center gap-1.5">
                {someSelected ? (
                  <>
                    <span className="text-purple-300 font-medium text-sm whitespace-nowrap mr-1">{selectedIds.size} selected</span>
                    <button onClick={handleBulkTrigger} className="px-2 py-1 text-sm bg-green-600 text-gray-100 rounded hover:bg-green-500 transition-colors" title="Run selected">▶️</button>
                    <button onClick={() => { setBulkTagMode('add'); setShowBulkTagModal(true); }} className="px-2 py-1 text-sm bg-teal-600 text-gray-100 rounded hover:bg-teal-500 transition-colors whitespace-nowrap">+ Tags</button>
                    <button onClick={() => { setBulkTagMode('remove'); setShowBulkTagModal(true); }} className="px-2 py-1 text-sm bg-orange-600 text-gray-100 rounded hover:bg-orange-500 transition-colors whitespace-nowrap">− Tags</button>
                    <button onClick={() => handleBulkToggleActive(true)} className="px-2 py-1 text-sm bg-blue-600 text-gray-100 rounded hover:bg-blue-500 transition-colors whitespace-nowrap">Enable</button>
                    <button onClick={() => handleBulkToggleActive(false)} className="px-2 py-1 text-sm bg-gray-600 text-gray-100 rounded hover:bg-gray-500 transition-colors whitespace-nowrap">Disable</button>
                    <button onClick={() => setShowDeleteConfirm(true)} className="px-2 py-1 text-sm bg-red-600 text-gray-100 rounded hover:bg-red-500 transition-colors">🗑️</button>
                    <button onClick={() => clearSelection()} className="px-2 py-1 text-sm bg-charcoal-600 text-gray-300 rounded hover:bg-charcoal-500 transition-colors ml-auto whitespace-nowrap">Clear</button>
                  </>
                ) : (
                  <span className="text-gray-500 text-sm">Select items for bulk actions</span>
                )}
              </div>
            </div>
          </div>

          {showDeleteConfirm && (
            <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={() => setShowDeleteConfirm(false)}>
              <div className="bg-charcoal-500 border border-red-700 rounded-lg p-4 max-w-md" onClick={e => e.stopPropagation()}>
                <h3 className="text-xl font-bold text-red-400 mb-2">⚠️ Confirm Delete</h3>
                <p className="text-gray-300 mb-4">{deleteConfirmText(selectedIds.size)}</p>
                <div className="flex gap-2 justify-end">
                  <button onClick={() => setShowDeleteConfirm(false)} className="px-3 py-1.5 bg-charcoal-600 text-gray-200 rounded hover:bg-charcoal-500">Cancel</button>
                  <button onClick={handleBulkDelete} className="px-3 py-1.5 bg-red-600 text-white rounded hover:bg-red-500">Delete {selectedIds.size} item{selectedIds.size !== 1 ? 's' : ''}</button>
                </div>
              </div>
            </div>
          )}

          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-charcoal-400 border-b border-charcoal-200">
                <tr>
                  <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold w-10">
                    <Checkbox checked={allFilteredSelected} onChange={(e) => handleSelectAll(e.target.checked, filteredData)} />
                  </th>
                  {columns.map(col => (
                    <th key={col.key} className={`text-${col.align || 'left'} px-2 py-1.5 text-sm text-gray-300 font-semibold ${col.className || ''}`} style={col.style}>
                      {col.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filteredData.map(row => renderRow({
                  row,
                  isSelected: selectedIds.has(row.id),
                  isHighlighted: row.id === highlightEntityId,
                  highlightedRowRef,
                  handleSelectRow,
                  onEdit,
                  onDelete,
                  onTrigger,
                  onNavigateToResult,
                  renderCell,
                  systems,
                  entityTypePlural,
                  triggerAction,
                }))}
              </tbody>
            </table>
          </div>
        </div>
      )}
      
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
