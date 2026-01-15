import React, { useState, useMemo, useEffect, useRef } from 'react';
import { ErrorBox } from '../components/ErrorBox';
import { TagList, TagBadge } from '../components/TagBadge';
import { Checkbox } from '../components/Checkbox';
import { SampleDifferencesModal } from '../components/modals/SampleDifferencesModal';
import { validationService } from '../services/api';

export function ValidationResultsView({ data, loading, error, onClearError, highlightId, onClearHighlight, onRefresh }) {
  const [sortConfig, setSortConfig] = useState({ key: 'requested_at', direction: 'desc' });
  const [filters, setFilters] = useState({
    entity_name: '',
    entity_type: '',
    status: '',
    system_pair: '',
  });
  const [filterTags, setFilterTags] = useState([]);
  const [tagInput, setTagInput] = useState('');
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedSuggestionIndex, setSelectedSuggestionIndex] = useState(0);
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');
  const [activePreset, setActivePreset] = useState('');
  const [selectedIds, setSelectedIds] = useState([]);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [selectedSample, setSelectedSample] = useState(null);
  const highlightedRowRef = useRef(null);
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

  // Scroll to and highlight the row when highlightId changes
  useEffect(() => {
    if (highlightId && highlightedRowRef.current) {
      highlightedRowRef.current.scrollIntoView({ behavior: 'smooth', block: 'center' });
      
      // Clear highlight after 3 seconds
      const timer = setTimeout(() => {
        if (onClearHighlight) onClearHighlight();
      }, 3000);
      
      return () => clearTimeout(timer);
    }
  }, [highlightId, onClearHighlight]);

  const handleSort = (key) => {
    setSortConfig(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'asc' ? 'desc' : 'asc'
    }));
  };

  const handleFilterChange = (key, value) => {
    setFilters(prev => ({ ...prev, [key]: value }));
  };

  const handlePresetClick = (preset) => {
    const now = new Date();
    let from = new Date();

    switch (preset) {
      case '1h':
        from.setHours(now.getHours() - 1);
        break;
      case '3h':
        from.setHours(now.getHours() - 3);
        break;
      case '6h':
        from.setHours(now.getHours() - 6);
        break;
      case '12h':
        from.setHours(now.getHours() - 12);
        break;
      case '24h':
        from.setHours(now.getHours() - 24);
        break;
      case '7d':
        from.setDate(now.getDate() - 7);
        break;
      default:
        return;
    }

    // Format for datetime-local input
    const formatDateTime = (date) => {
      const year = date.getFullYear();
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const day = String(date.getDate()).padStart(2, '0');
      const hours = String(date.getHours()).padStart(2, '0');
      const minutes = String(date.getMinutes()).padStart(2, '0');
      return `${year}-${month}-${day}T${hours}:${minutes}`;
    };

    setDateFrom(formatDateTime(from));
    setDateTo(formatDateTime(now));
    setActivePreset(preset);
  };

  const handleDateFromChange = (value) => {
    setDateFrom(value);
    setActivePreset('');
  };

  const handleDateToChange = (value) => {
    setDateTo(value);
    setActivePreset('');
  };

  const clearDateFilters = () => {
    setDateFrom('');
    setDateTo('');
    setActivePreset('');
  };

  const hasActiveFilters = filters.entity_name || filters.entity_type || filters.status || filters.system_pair || filterTags.length > 0 || dateFrom || dateTo;

  const clearAllFilters = () => {
    setFilters({
      entity_name: '',
      entity_type: '',
      status: '',
      system_pair: '',
    });
    setFilterTags([]);
    clearDateFilters();
  };

  // Checkbox selection handlers
  const toggleSelectAll = () => {
    if (selectedIds.length === filteredAndSortedData.length) {
      setSelectedIds([]);
    } else {
      setSelectedIds(filteredAndSortedData.map(v => v.id));
    }
  };

  const toggleSelectRow = (id) => {
    setSelectedIds(prev => 
      prev.includes(id) ? prev.filter(i => i !== id) : [...prev, id]
    );
  };

  const handleDeleteSelected = async () => {
    setIsDeleting(true);
    try {
      await validationService.deleteMultiple(selectedIds);
      setSelectedIds([]);
      setShowDeleteConfirm(false);
      if (onRefresh) await onRefresh();
    } catch (err) {
      alert(`Failed to delete records: ${err.message}`);
    } finally {
      setIsDeleting(false);
    }
  };

  // Extract unique system pairs and tags from data
  const availableSystemPairs = useMemo(() => {
    const pairsSet = new Set();
    data.forEach(v => {
      if (v.source_system_name && v.target_system_name) {
        pairsSet.add(`${v.source_system_name} → ${v.target_system_name}`);
      }
    });
    return Array.from(pairsSet).sort();
  }, [data]);

  const allTags = useMemo(() => {
    const tagSet = new Set();
    data.forEach(row => {
      parseTags(row.tags).forEach(tag => tagSet.add(tag));
    });
    return Array.from(tagSet).sort();
  }, [data]);

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

  const filteredAndSortedData = useMemo(() => {
    let result = [...data];

    // Apply date filters
    if (dateFrom) {
      const fromTime = new Date(dateFrom).getTime();
      result = result.filter(v => new Date(v.requested_at).getTime() >= fromTime);
    }
    if (dateTo) {
      const toTime = new Date(dateTo).getTime();
      result = result.filter(v => new Date(v.requested_at).getTime() <= toTime);
    }

    // Apply filters
    if (filters.entity_name) {
      result = result.filter(v => 
        v.entity_name.toLowerCase().includes(filters.entity_name.toLowerCase())
      );
    }
    if (filters.entity_type) {
      result = result.filter(v => v.entity_type === filters.entity_type);
    }
    if (filters.status) {
      result = result.filter(v => v.status === filters.status);
    }
    if (filters.system_pair) {
      result = result.filter(v => 
        `${v.source_system_name} → ${v.target_system_name}` === filters.system_pair
      );
    }
    
    // Apply tag filter (AND logic - must have all selected tags)
    if (filterTags.length > 0) {
      result = result.filter(v => {
        const rowTags = parseTags(v.tags);
        return filterTags.every(filterTag => rowTags.includes(filterTag));
      });
    }

    // Apply sorting
    result.sort((a, b) => {
      let aVal, bVal;

      switch (sortConfig.key) {
        case 'entity_name':
          aVal = a.entity_name.toLowerCase();
          bVal = b.entity_name.toLowerCase();
          break;
        case 'entity_type':
          aVal = a.entity_type;
          bVal = b.entity_type;
          break;
        case 'status':
          aVal = a.status;
          bVal = b.status;
          break;
        case 'duration':
          aVal = a.duration_seconds || 0;
          bVal = b.duration_seconds || 0;
          break;
        case 'systems':
          aVal = `${a.source_system_name} ${a.target_system_name}`.toLowerCase();
          bVal = `${b.source_system_name} ${b.target_system_name}`.toLowerCase();
          break;
        case 'row_counts':
          aVal = a.row_count_source || 0;
          bVal = b.row_count_source || 0;
          break;
        case 'differences':
          aVal = a.rows_different || 0;
          bVal = b.rows_different || 0;
          break;
        case 'requested_at':
          aVal = new Date(a.requested_at).getTime();
          bVal = new Date(b.requested_at).getTime();
          break;
        default:
          return 0;
      }

      if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
      if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
      return 0;
    });

    return result;
  }, [data, filters, filterTags, sortConfig, dateFrom, dateTo]);

  const SortableHeader = ({ label, sortKey, className = "" }) => (
    <th 
      className={`text-left px-2 py-1.5 text-sm text-gray-300 font-semibold cursor-pointer hover:bg-charcoal-300/30 transition-colors select-none ${className}`}
      onClick={() => handleSort(sortKey)}
    >
      <div className="flex items-center gap-1">
        {label}
        {sortConfig.key === sortKey && (
          <span className="text-rust-light">
            {sortConfig.direction === 'asc' ? '↑' : '↓'}
          </span>
        )}
      </div>
    </th>
  );

  return (
    <>
      {error && error.action !== "setup_required" && <ErrorBox message={error.message} onClose={onClearError} />}
      <div className="mb-4 flex items-start justify-between">
        <div>
          <h2 className="text-3xl font-bold text-rust-light mb-1">🎯 Validation Results</h2>
          <p className="text-gray-400 text-base">Recent validation history for the last 7 days</p>
        </div>
        {selectedIds.length > 0 && (
          <button
            onClick={() => setShowDeleteConfirm(true)}
            className="px-4 py-2 bg-red-900/40 text-red-300 border border-red-700 rounded-lg hover:bg-red-900/60 transition-all font-medium"
          >
            Delete Selected ({selectedIds.length})
          </button>
        )}
      </div>
      
      {/* Summary Stats */}
      <div className="grid grid-cols-1 md:grid-cols-5 gap-3 mb-4">
        <div className="bg-charcoal-500 border border-charcoal-200 rounded-lg p-2.5">
          <div className="text-gray-400 text-sm mb-0.5">Total Validations</div>
          <div className="text-3xl font-bold text-gray-100">{filteredAndSortedData.length}</div>
          {filteredAndSortedData.length > 9500 && (
            <div className="mt-2 px-2 py-1 bg-red-900/40 border border-red-700 rounded text-red-300 text-xs">
              Results pane almost full ({filteredAndSortedData.length}/10000). Delete unneeded records
            </div>
          )}
        </div>
        <div className="bg-green-900/20 border border-green-700 rounded-lg p-2.5">
          <div className="text-green-400 text-sm mb-0.5">✓ Succeeded</div>
          <div className="text-3xl font-bold text-green-300">
            {filteredAndSortedData.filter(v => v.status === 'succeeded').length}
          </div>
        </div>
        <div className="bg-red-900/20 border border-red-700 rounded-lg p-2.5">
          <div className="text-red-400 text-sm mb-0.5">✗ Failed</div>
          <div className="text-3xl font-bold text-red-300">
            {filteredAndSortedData.filter(v => v.status === 'failed').length}
          </div>
        </div>
        <div className="bg-orange-900/20 border border-orange-700 rounded-lg p-2.5">
          <div className="text-orange-400 text-sm mb-0.5">⚠ Errors</div>
          <div className="text-3xl font-bold text-orange-300">
            {filteredAndSortedData.filter(v => v.status === 'error').length}
          </div>
        </div>
        <div className="bg-purple-900/20 border border-purple-700 rounded-lg p-2.5">
          <div className="text-purple-400 text-sm mb-0.5">Avg Duration</div>
          <div className="text-3xl font-bold text-purple-300">
            {filteredAndSortedData.length > 0 
              ? ((filteredAndSortedData.reduce((sum, v) => sum + (v.duration_seconds || 0), 0) / filteredAndSortedData.length) / 60).toFixed(1)
              : 0}m
          </div>
        </div>
      </div>

      {loading ? <p className="text-gray-400">Loading…</p> : (
        <div className="bg-charcoal-500 border border-charcoal-200 rounded-lg">
          {/* Date Range Filter */}
          <div className="p-2 bg-charcoal-400 border-b border-charcoal-200">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-gray-300 text-sm font-semibold">Time:</span>
              {['1h', '3h', '6h', '12h', '24h', '7d'].map(preset => (
                <button
                  key={preset}
                  onClick={() => handlePresetClick(preset)}
                  className={`px-2 py-1 text-sm rounded transition-all ${
                    activePreset === preset
                      ? 'bg-rust-light text-white border border-rust-light shadow-sm'
                      : 'bg-charcoal-600 text-gray-300 border border-charcoal-300 hover:border-rust-light/50 hover:bg-charcoal-500'
                  }`}
                >
                  {preset}
                </button>
              ))}
              {(dateFrom || dateTo) && (
                <button
                  onClick={clearDateFilters}
                  className="px-2 py-1 text-sm rounded bg-red-900/40 text-red-300 border border-red-700 hover:bg-red-900/60 transition-all"
                >
                  Clear
                </button>
              )}
              <div className="flex items-center gap-1 ml-auto">
                <label className="text-gray-400 text-sm">From:</label>
                <input
                  type="datetime-local"
                  value={dateFrom}
                  onChange={(e) => handleDateFromChange(e.target.value)}
                  className="px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light"
                />
              </div>
              <div className="flex items-center gap-1">
                <label className="text-gray-400 text-sm">To:</label>
                <input
                  type="datetime-local"
                  value={dateTo}
                  onChange={(e) => handleDateToChange(e.target.value)}
                  className="px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light"
                />
              </div>
            </div>
          </div>

          {/* Filters */}
          <div className="p-2 bg-charcoal-400 border-b border-charcoal-200">
            <div className="flex justify-between items-center mb-2 min-h-[24px]">
              <span className="text-gray-300 text-sm font-semibold">Filters:</span>
              {hasActiveFilters && (
                <button
                  onClick={clearAllFilters}
                  className="px-2 py-0.5 text-xs rounded bg-red-900/40 text-red-300 border border-red-700 hover:bg-red-900/60 transition-all"
                >
                  Clear All
                </button>
              )}
            </div>
            <div className="grid grid-cols-1 md:grid-cols-5 gap-2">
            <input
              type="text"
              placeholder="Filter by entity..."
              value={filters.entity_name}
              onChange={(e) => handleFilterChange('entity_name', e.target.value)}
              className="px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light"
            />
            
            {/* Tag Filter with Autocomplete */}
            <div className="relative" ref={tagInputRef}>
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
                  placeholder={filterTags.length === 0 ? "Filter by tags..." : ""}
                  value={tagInput}
                  onChange={(e) => {
                    setTagInput(e.target.value);
                    setShowSuggestions(true);
                  }}
                  onKeyDown={handleTagKeyDown}
                  onFocus={() => setShowSuggestions(true)}
                  className="flex-1 min-w-[60px] bg-transparent border-0 text-gray-200 text-sm focus:outline-none placeholder-gray-500"
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

            <select
              value={filters.entity_type}
              onChange={(e) => handleFilterChange('entity_type', e.target.value)}
              className="px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light hover:border-charcoal-100 transition-colors cursor-pointer"
            >
              <option value="">All Types</option>
              <option value="table">Table</option>
              <option value="query">Query</option>
            </select>
            <select
              value={filters.status}
              onChange={(e) => handleFilterChange('status', e.target.value)}
              className="px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light hover:border-charcoal-100 transition-colors cursor-pointer"
            >
              <option value="">All Statuses+</option>
              <option value="succeeded">Succeeded</option>
              <option value="failed">Failed</option>
              <option value="error">Error</option>
            </select>
            <select
              value={filters.system_pair}
              onChange={(e) => handleFilterChange('system_pair', e.target.value)}
              className="px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light hover:border-charcoal-100 transition-colors cursor-pointer"
            >
              <option value="">All System Pairs</option>
              {availableSystemPairs.map(pair => (
                <option key={pair} value={pair}>
                  {pair}
                </option>
              ))}
            </select>
            </div>
          </div>

          <div className="overflow-x-auto">
          <table className="w-full min-w-[1400px]">

            <thead className="bg-charcoal-400 border-b border-charcoal-200">
              <tr>
                <th className="px-2 py-1.5 w-12 text-center">
                  <Checkbox
                    checked={selectedIds.length === filteredAndSortedData.length && filteredAndSortedData.length > 0}
                    onChange={toggleSelectAll}
                    className="align-middle"
                  />
                </th>
                <SortableHeader label="Entity" sortKey="entity_name" className="px-2 py-1.5 text-left max-w-[500px]" />
                <SortableHeader label="Type" sortKey="entity_type" className="px-2 py-1.5 text-left" />
                <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold">Tags</th>
                <SortableHeader label="Status" sortKey="status" className="px-2 py-1.5 text-left" />
                <SortableHeader label="Duration" sortKey="duration" className="px-2 py-1.5 text-left" />
                <SortableHeader label="Source → Target" sortKey="systems" className="px-2 py-1.5 text-left" />
                <SortableHeader label="Row Counts" sortKey="row_counts" className="px-2 py-1.5 text-left whitespace-nowrap" />
                <SortableHeader label="Diffs" sortKey="differences" className="px-2 py-1.5 text-left whitespace-nowrap" />
                <SortableHeader label="Triggered" sortKey="requested_at" className="px-2 py-1.5 text-left" />
                <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold w-16">Details</th>
              </tr>
            </thead>

            <tbody>
              {filteredAndSortedData.length === 0 ? (
                <tr>
                  <td colSpan={11} className="text-center p-8 text-gray-500 text-base">
                    {data.length === 0
                      ? "No validation history yet. Run a validation from Tables or Queries!"
                      : "No results match the current filters."}
                  </td>
                </tr>
              ) : (
                filteredAndSortedData.map((v) => (
                  <tr
                    key={v.id}
                    ref={v.id === highlightId ? highlightedRowRef : null}
                    className={`border-b border-charcoal-300/30 hover:bg-charcoal-400/50 transition-colors ${
                      v.id === highlightId ? 'bg-rust-light/20 ring-2 ring-rust-light' : ''
                    }`}
                  >
                    <td className="px-2 py-1.5 text-center align-middle">
                      <Checkbox
                        checked={selectedIds.includes(v.id)}
                        onChange={() => toggleSelectRow(v.id)}
                        className="align-middle"
                      />
                    </td>
                    <td className="px-2 py-1.5 text-gray-200 font-medium text-sm max-w-[500px]" title={v.entity_name}>
                      <div className="truncate overflow-hidden whitespace-nowrap [direction:rtl] text-left">
                        <span className="[direction:ltr]">{v.entity_name}</span>
                      </div>
                    </td>

                    <td className="px-2 py-1.5">
                      <span
                        className={`px-1.5 py-0.5 text-sm rounded-full ${
                          v.entity_type === 'table'
                            ? 'bg-blue-900/40 text-blue-300 border border-blue-700'
                            : 'bg-purple-900/40 text-purple-300 border border-purple-700'
                        }`}
                      >
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
                      {v.row_count_match ? (
                        <span className="text-green-400">✓ {v.row_count_source?.toLocaleString()}</span>
                      ) : (
                        <span className="text-red-400">
                          {v.row_count_source?.toLocaleString()} ≠ {v.row_count_target?.toLocaleString()}
                        </span>
                      )}
                    </td>

                    <td className="px-2 py-1.5 text-sm whitespace-nowrap">
                      {v.rows_different == null ? (
                        <span className="text-gray-500">-</span>
                      ) : v.rows_different > 0 ? (
                        <button
                          onClick={() => setSelectedSample(v)}
                          className="text-rust-light font-medium hover:text-rust-lighter underline decoration-dotted cursor-pointer transition-colors"
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
                          className="text-purple-4 00 hover:text-purple-300 underline"
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
        </div>
      )}

      {/* Delete Confirmation Dialog */}
      {showDeleteConfirm && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-charcoal-500 border border-charcoal-200 rounded-lg p-6 max-w-md w-full mx-4">
            <h3 className="text-xl font-bold text-rust-light mb-3">Confirm Deletion</h3>
            <p className="text-gray-300 mb-6">
              Are you sure you want to delete <span className="font-bold text-rust-light">{selectedIds.length}</span> validation record{selectedIds.length !== 1 ? 's' : ''}? This action cannot be undone.
            </p>
            <div className="flex gap-3 justify-end">
              <button
                onClick={() => setShowDeleteConfirm(false)}
                disabled={isDeleting}
                className="px-4 py-2 bg-charcoal-600 text-gray-300 border border-charcoal-300 rounded hover:bg-charcoal-500 transition-all disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                onClick={handleDeleteSelected}
                disabled={isDeleting}
                className="px-4 py-2 bg-red-900/40 text-red-300 border border-red-700 rounded hover:bg-red-900/60 transition-all disabled:opacity-50 font-medium"
              >
                {isDeleting ? 'Deleting...' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Sample Differences Modal */}
      {selectedSample && (
        <SampleDifferencesModal 
          validation={selectedSample} 
          onClose={() => setSelectedSample(null)} 
        />
      )}
    </>
  );
}
