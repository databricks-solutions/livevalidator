import React, { useState, useMemo, useEffect, useRef, useCallback } from 'react';
import { ErrorBox } from '../components/ErrorBox';
import { TagBadge } from '../components/TagBadge';
import { SampleDifferencesModal } from '../components/modals/SampleDifferencesModal';
import { ValidationResultsTable } from '../components/ValidationResultsTable';
import { validationService } from '../services/api';

// Debounce hook
function useDebounce(value, delay) {
  const [debouncedValue, setDebouncedValue] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedValue(value), delay);
    return () => clearTimeout(timer);
  }, [value, delay]);
  return debouncedValue;
}

export function ValidationResultsView({ highlightId, onClearHighlight, onNavigateToEntity }) {
  // Pagination state
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(100);
  
  // Filter input states (immediate)
  const [entityNameInput, setEntityNameInput] = useState('');
  const [filterTags, setFilterTags] = useState([]);
  const [entityType, setEntityType] = useState('');
  const [status, setStatus] = useState('');
  const [sourceSystem, setSourceSystem] = useState('');
  const [targetSystem, setTargetSystem] = useState('');
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');
  const [activePreset, setActivePreset] = useState('7d');
  
  // Sorting state
  const [sortConfig, setSortConfig] = useState({ key: 'requested_at', direction: 'desc' });
  
  // Debounced filter values (for API calls)
  const debouncedEntityName = useDebounce(entityNameInput, 300);
  
  // Tag input state
  const [tagInput, setTagInput] = useState('');
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedSuggestionIndex, setSelectedSuggestionIndex] = useState(0);
  const tagInputRef = useRef(null);
  const inputElementRef = useRef(null);
  
  // Data state
  const [data, setData] = useState([]);
  const [totalCount, setTotalCount] = useState(0);
  const [stats, setStats] = useState({ total: 0, succeeded: 0, failed: 0, errors: 0 });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  // Selection state
  const [selectedIds, setSelectedIds] = useState([]);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  
  // Sample modal state
  const [selectedSample, setSelectedSample] = useState(null);
  const [loadingSampleId, setLoadingSampleId] = useState(null);
  
  // Highlight ref
  const highlightedRowRef = useRef(null);
  
  // Systems list for dropdowns (fetched once)
  const [availableSystems, setAvailableSystems] = useState([]);
  const [allTags, setAllTags] = useState([]);

  // Fetch systems list for filter dropdowns
  useEffect(() => {
    fetch('/api/systems')
      .then(r => r.json())
      .then(systems => setAvailableSystems(systems.map(s => s.name)))
      .catch(() => {});
    fetch('/api/tags')
      .then(r => r.json())
      .then(tags => setAllTags(tags.map(t => t.name)))
      .catch(() => {});
  }, []);

  // Calculate date range based on preset
  const getDateRange = useCallback(() => {
    if (dateFrom || dateTo) {
      return { from: dateFrom, to: dateTo };
    }
    if (!activePreset) {
      return { from: '', to: '' };
    }
    
    const now = new Date();
    let from = new Date();
    
    switch (activePreset) {
      case '1h': from.setHours(now.getHours() - 1); break;
      case '3h': from.setHours(now.getHours() - 3); break;
      case '6h': from.setHours(now.getHours() - 6); break;
      case '12h': from.setHours(now.getHours() - 12); break;
      case '24h': from.setHours(now.getHours() - 24); break;
      case '7d': from.setDate(now.getDate() - 7); break;
      case '30d': from.setDate(now.getDate() - 30); break;
      default: return { from: '', to: '' };
    }
    
    return { from: from.toISOString(), to: now.toISOString() };
  }, [activePreset, dateFrom, dateTo]);

  // Build query string from filters
  const queryString = useMemo(() => {
    const params = new URLSearchParams();
    params.set('limit', pageSize);
    params.set('offset', page * pageSize);
    params.set('sort_by', sortConfig.key);
    params.set('sort_dir', sortConfig.direction);
    
    if (debouncedEntityName) params.set('entity_name', debouncedEntityName);
    if (entityType) params.set('entity_type', entityType);
    if (status) params.set('status', status);
    if (sourceSystem) params.set('source_system', sourceSystem);
    if (targetSystem) params.set('target_system', targetSystem);
    if (filterTags.length > 0) params.set('tags', filterTags.join(','));
    
    const { from, to } = getDateRange();
    if (from) params.set('date_from', from);
    if (to) params.set('date_to', to);
    
    return params.toString();
  }, [page, pageSize, sortConfig, debouncedEntityName, entityType, status, sourceSystem, targetSystem, filterTags, getDateRange]);

  // Fetch data when query changes
  useEffect(() => {
    let cancelled = false;
    const fetchData = async () => {
      setLoading(true);
      try {
        const res = await fetch(`/api/validation-history?${queryString}`);
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        const result = await res.json();
        if (!cancelled) {
          setData(result.data || []);
          setTotalCount(result.total || 0);
          setStats(result.stats || { total: 0, succeeded: 0, failed: 0, errors: 0 });
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError({ message: err.message });
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    fetchData();
    return () => { cancelled = true; };
  }, [queryString]);

  // Auto-refresh every 30s when on first page with no filters
  useEffect(() => {
    const isDefaultView = page === 0 && !debouncedEntityName && !entityType && !status && 
                          !sourceSystem && !targetSystem && filterTags.length === 0;
    if (!isDefaultView) return;
    
    const interval = setInterval(() => {
      fetch(`/api/validation-history?${queryString}`)
        .then(r => r.json())
        .then(result => {
          setData(result.data || []);
          setTotalCount(result.total || 0);
          setStats(result.stats || { total: 0, succeeded: 0, failed: 0, errors: 0 });
        })
        .catch(() => {});
    }, 30000);
    
    return () => clearInterval(interval);
  }, [queryString, page, debouncedEntityName, entityType, status, sourceSystem, targetSystem, filterTags]);

  // Scroll to highlighted row
  useEffect(() => {
    if (highlightId && highlightedRowRef.current) {
      highlightedRowRef.current.scrollIntoView({ behavior: 'smooth', block: 'center' });
      const timer = setTimeout(() => onClearHighlight?.(), 3000);
      return () => clearTimeout(timer);
    }
  }, [highlightId, onClearHighlight]);

  // Handlers
  const handleSort = (key) => {
    setSortConfig(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'asc' ? 'desc' : 'asc'
    }));
    setPage(0);
  };

  const handlePresetClick = (preset) => {
    setDateFrom('');
    setDateTo('');
    setActivePreset(preset);
    setPage(0);
  };

  const handleDateFromChange = (value) => {
    setDateFrom(value);
    setActivePreset('');
    setPage(0);
  };

  const handleDateToChange = (value) => {
    setDateTo(value);
    setActivePreset('');
    setPage(0);
  };

  const clearDateFilters = () => {
    setDateFrom('');
    setDateTo('');
    setActivePreset('');
    setPage(0);
  };

  const handleViewSample = async (validation) => {
    setLoadingSampleId(validation.id);
    try {
      const res = await fetch(`/api/validation-history/${validation.id}`);
      if (res.ok) setSelectedSample(await res.json());
    } catch (e) {
      console.error('Failed to fetch validation details:', e);
    } finally {
      setLoadingSampleId(null);
    }
  };

  // Tag handlers
  const addTagFilter = (tag) => {
    if (tag && !filterTags.includes(tag)) {
      setFilterTags(prev => [...prev, tag]);
      setPage(0);
    }
    setTagInput('');
    setShowSuggestions(false);
    setSelectedSuggestionIndex(0);
  };

  const removeTagFilter = (tag) => {
    setFilterTags(prev => prev.filter(t => t !== tag));
    setPage(0);
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
      if (tagSuggestions.length > 0) addTagFilter(tagSuggestions[selectedSuggestionIndex]);
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

  // Selection handlers
  const toggleSelectAll = () => {
    if (selectedIds.length === data.length) {
      setSelectedIds([]);
    } else {
      setSelectedIds(data.map(v => v.id));
    }
  };

  const toggleSelectRow = (id) => {
    setSelectedIds(prev => prev.includes(id) ? prev.filter(i => i !== id) : [...prev, id]);
  };

  const handleDeleteSelected = async () => {
    setIsDeleting(true);
    try {
      await validationService.deleteMultiple(selectedIds);
      setSelectedIds([]);
      setShowDeleteConfirm(false);
      // Refresh data
      const res = await fetch(`/api/validation-history?${queryString}`);
      const result = await res.json();
      setData(result.data || []);
      setTotalCount(result.total || 0);
    } catch (err) {
      alert(`Failed to delete records: ${err.message}`);
    } finally {
      setIsDeleting(false);
    }
  };

  const hasActiveFilters = entityNameInput || entityType || status || sourceSystem || targetSystem || filterTags.length > 0 || dateFrom || dateTo;

  const clearAllFilters = () => {
    setEntityNameInput('');
    setEntityType('');
    setStatus('');
    setSourceSystem('');
    setTargetSystem('');
    setFilterTags([]);
    setDateFrom('');
    setDateTo('');
    setActivePreset('7d');
    setPage(0);
  };

  // Summary stats from API (accurate totals for all filtered results)
  const summaryStats = stats;

  // Pagination calculations
  const totalPages = Math.ceil(totalCount / pageSize);
  const startRecord = page * pageSize + 1;
  const endRecord = Math.min((page + 1) * pageSize, totalCount);

  // Get current date range label
  const getDateRangeLabel = () => {
    if (dateFrom || dateTo) return 'Custom range';
    if (activePreset === '7d') return 'last 7 days';
    if (activePreset === '30d') return 'last 30 days';
    if (activePreset) return `last ${activePreset}`;
    return 'all time';
  };

  return (
    <>
      {error && <ErrorBox message={error.message} onClose={() => setError(null)} />}
      <div className="mb-4 flex items-start justify-between">
        <h2 className="text-3xl font-bold text-rust-light">Validation Results</h2>
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
      <div className="grid grid-cols-1 md:grid-cols-4 gap-3 mb-4">
        <div className="bg-charcoal-500 border border-charcoal-200 rounded-lg p-2.5">
          <div className="flex items-center justify-between">
            <span className="text-gray-400 text-sm">Total</span>
            <span className="text-xs px-1.5 py-0.5 bg-charcoal-400 text-gray-300 rounded">{getDateRangeLabel()}</span>
          </div>
          <div className="text-3xl font-bold text-gray-100">{summaryStats.total.toLocaleString()}</div>
        </div>
        <div className="bg-green-900/20 border border-green-700 rounded-lg p-2.5">
          <div className="text-green-400 text-sm mb-0.5">✓ Succeeded</div>
          <div className="text-3xl font-bold text-green-300">{summaryStats.succeeded.toLocaleString()}</div>
        </div>
        <div className="bg-red-900/20 border border-red-700 rounded-lg p-2.5">
          <div className="text-red-400 text-sm mb-0.5">✗ Failed</div>
          <div className="text-3xl font-bold text-red-300">{summaryStats.failed.toLocaleString()}</div>
        </div>
        <div className="bg-orange-900/20 border border-orange-700 rounded-lg p-2.5">
          <div className="text-orange-400 text-sm mb-0.5">⚠ Errors</div>
          <div className="text-3xl font-bold text-orange-300">{summaryStats.errors.toLocaleString()}</div>
        </div>
      </div>

      <div className="bg-charcoal-500 border border-charcoal-200 rounded-lg flex flex-col" style={{ minHeight: 'calc(100vh - 380px)' }}>
        {/* Date Range Filter */}
        <div className="p-2 bg-charcoal-400 border-b border-charcoal-200">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-gray-300 text-sm font-semibold">Time:</span>
            {['1h', '3h', '6h', '12h', '24h', '7d', '30d'].map(preset => (
              <button
                key={preset}
                onClick={() => handlePresetClick(preset)}
                className={`px-2 py-1 text-sm rounded transition-all ${
                  activePreset === preset && !dateFrom && !dateTo
                    ? 'bg-rust-light text-white border border-rust-light shadow-sm'
                    : 'bg-charcoal-600 text-gray-300 border border-charcoal-300 hover:border-rust-light/50 hover:bg-charcoal-500'
                }`}
              >
                {preset}
              </button>
            ))}
            {(dateFrom || dateTo || activePreset) && (
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
          <div className="grid grid-cols-1 md:grid-cols-6 gap-2">
            <input
              type="text"
              placeholder="Filter by entity..."
              value={entityNameInput}
              onChange={(e) => { setEntityNameInput(e.target.value); setPage(0); }}
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
                  onChange={(e) => { setTagInput(e.target.value); setShowSuggestions(true); }}
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
              value={entityType}
              onChange={(e) => { setEntityType(e.target.value); setPage(0); }}
              className="px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light hover:border-charcoal-100 transition-colors cursor-pointer"
            >
              <option value="">All Types</option>
              <option value="table">Table</option>
              <option value="compare_query">Query</option>
            </select>
            <select
              value={status}
              onChange={(e) => { setStatus(e.target.value); setPage(0); }}
              className="px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light hover:border-charcoal-100 transition-colors cursor-pointer"
            >
              <option value="">All Statuses</option>
              <option value="succeeded">Succeeded</option>
              <option value="failed">Failed</option>
              <option value="error">Error</option>
            </select>
            <select
              value={sourceSystem}
              onChange={(e) => { setSourceSystem(e.target.value); setPage(0); }}
              className="px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light hover:border-charcoal-100 transition-colors cursor-pointer"
            >
              <option value="">All Sources</option>
              {availableSystems.map(sys => (
                <option key={sys} value={sys}>{sys}</option>
              ))}
            </select>
            <select
              value={targetSystem}
              onChange={(e) => { setTargetSystem(e.target.value); setPage(0); }}
              className="px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light hover:border-charcoal-100 transition-colors cursor-pointer"
            >
              <option value="">All Targets</option>
              {availableSystems.map(sys => (
                <option key={sys} value={sys}>{sys}</option>
              ))}
            </select>
          </div>
        </div>

        {/* Results Table */}
        {loading && data.length === 0 ? (
          <div className="flex-1 flex items-center justify-center text-gray-400">Loading...</div>
        ) : (
          <ValidationResultsTable
            data={data}
            onViewSample={handleViewSample}
            loadingSampleId={loadingSampleId}
            onEntityClick={onNavigateToEntity}
            showCheckboxes={true}
            selectedIds={selectedIds}
            onToggleSelect={toggleSelectRow}
            onToggleSelectAll={toggleSelectAll}
            highlightId={highlightId}
            highlightedRowRef={highlightedRowRef}
            sortable={true}
            sortConfig={sortConfig}
            onSort={handleSort}
            emptyMessage={totalCount === 0 
              ? "No validation history yet. Run a validation from Tables or Queries!" 
              : "No results match the current filters."}
            fillHeight={true}
          />
        )}

        {/* Pagination Controls */}
        {totalCount > 0 && (
          <div className="flex items-center justify-between p-3 border-t border-charcoal-200 bg-charcoal-400">
            <span className="text-gray-400 text-sm">
              Showing {startRecord.toLocaleString()}-{endRecord.toLocaleString()} of {totalCount.toLocaleString()}
            </span>
            
            <div className="flex items-center gap-2">
              <button
                onClick={() => setPage(0)}
                disabled={page === 0}
                className="px-2 py-1 text-sm rounded bg-charcoal-600 text-gray-300 border border-charcoal-300 hover:bg-charcoal-500 disabled:opacity-40 disabled:cursor-not-allowed transition-all"
              >
                First
              </button>
              <button
                onClick={() => setPage(p => p - 1)}
                disabled={page === 0}
                className="px-2 py-1 text-sm rounded bg-charcoal-600 text-gray-300 border border-charcoal-300 hover:bg-charcoal-500 disabled:opacity-40 disabled:cursor-not-allowed transition-all"
              >
                Prev
              </button>
              
              <span className="px-3 py-1 text-gray-300 text-sm">
                Page {page + 1} of {totalPages}
              </span>
              
              <button
                onClick={() => setPage(p => p + 1)}
                disabled={page >= totalPages - 1}
                className="px-2 py-1 text-sm rounded bg-charcoal-600 text-gray-300 border border-charcoal-300 hover:bg-charcoal-500 disabled:opacity-40 disabled:cursor-not-allowed transition-all"
              >
                Next
              </button>
              <button
                onClick={() => setPage(totalPages - 1)}
                disabled={page >= totalPages - 1}
                className="px-2 py-1 text-sm rounded bg-charcoal-600 text-gray-300 border border-charcoal-300 hover:bg-charcoal-500 disabled:opacity-40 disabled:cursor-not-allowed transition-all"
              >
                Last
              </button>
            </div>

            <select
              value={pageSize}
              onChange={(e) => { setPageSize(Number(e.target.value)); setPage(0); }}
              className="px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light cursor-pointer"
            >
              <option value={50}>50 / page</option>
              <option value={100}>100 / page</option>
              <option value={250}>250 / page</option>
              <option value={500}>500 / page</option>
              <option value={1000}>1000 / page</option>
            </select>
          </div>
        )}
      </div>

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
