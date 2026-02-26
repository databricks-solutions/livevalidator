import React from 'react';
import { TagBadge } from './TagBadge';

export function ResultFilterBar({
  // Entity name filter
  entityName = '',
  onEntityNameChange,
  // Tag filter (from useTagFilter hook)
  filterTags = [],
  tagInput = '',
  onTagInputChange,
  showSuggestions = false,
  onShowSuggestionsChange,
  tagSuggestions = [],
  selectedSuggestionIndex = 0,
  onAddTag,
  onRemoveTag,
  onTagKeyDown,
  tagInputRef,
  inputElementRef,
  // Type filter
  entityType = '',
  onEntityTypeChange,
  typeOptions = [
    { value: '', label: 'All Types' },
    { value: 'table', label: 'Tables' },
    { value: 'query', label: 'Queries' },
  ],
  // Status filter
  showStatusFilter = false,
  status = '',
  onStatusChange,
  // Source/Target system filters
  showSystemFilters = false,
  sourceSystem = '',
  onSourceSystemChange,
  targetSystem = '',
  onTargetSystemChange,
  availableSystems = [],
  // Clear all
  hasActiveFilters = false,
  onClearAll,
}) {
  const selectClass = "px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light hover:border-charcoal-100 transition-colors cursor-pointer";
  const inputClass = "px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light";

  return (
    <div className="p-2 bg-charcoal-400 border-b border-charcoal-200 space-y-2">
      <div className="flex items-center gap-2">
        <span className="text-gray-300 text-sm font-semibold">Filters:</span>
        {hasActiveFilters && (
          <button
            onClick={onClearAll}
            className="px-2 py-0.5 text-xs rounded bg-red-900/40 text-red-300 border border-red-700 hover:bg-red-900/60 transition-all"
          >
            Clear All
          </button>
        )}
      </div>
      <div className="grid grid-cols-1 md:grid-cols-6 gap-2">
        {/* Entity name filter */}
        <input
          type="text"
          placeholder="Filter by entity..."
          value={entityName}
          onChange={(e) => onEntityNameChange?.(e.target.value)}
          className={inputClass}
        />

        {/* Tag filter with autocomplete */}
        <div className="relative" ref={tagInputRef}>
          <div 
            className="flex flex-wrap gap-1 items-center px-2 py-1 bg-charcoal-600 border border-charcoal-300 rounded min-h-[34px] cursor-text focus-within:border-rust-light"
            onClick={() => inputElementRef?.current?.focus()}
          >
            {filterTags.map(tag => (
              <TagBadge key={tag} tag={tag} onRemove={() => onRemoveTag?.(tag)} />
            ))}
            <input
              ref={inputElementRef}
              type="text"
              placeholder={filterTags.length === 0 ? "Filter by tags..." : ""}
              value={tagInput}
              onChange={(e) => { onTagInputChange?.(e.target.value); onShowSuggestionsChange?.(true); }}
              onKeyDown={onTagKeyDown}
              onFocus={() => onShowSuggestionsChange?.(true)}
              className="flex-1 min-w-[60px] bg-transparent border-0 text-gray-200 text-sm focus:outline-none placeholder-gray-500"
            />
          </div>
          {showSuggestions && tagSuggestions.length > 0 && (
            <div className="absolute z-10 w-full mt-1 bg-charcoal-600 border border-charcoal-300 rounded shadow-lg max-h-48 overflow-y-auto">
              {tagSuggestions.map((tag, idx) => (
                <button
                  key={tag}
                  onClick={() => onAddTag?.(tag)}
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

        {/* Type filter */}
        <select value={entityType} onChange={(e) => onEntityTypeChange?.(e.target.value)} className={selectClass}>
          {typeOptions.map(opt => (
            <option key={opt.value} value={opt.value}>{opt.label}</option>
          ))}
        </select>

        {/* Status filter (optional) */}
        {showStatusFilter && (
          <select value={status} onChange={(e) => onStatusChange?.(e.target.value)} className={selectClass}>
            <option value="">All Statuses</option>
            <option value="succeeded">Succeeded</option>
            <option value="failed">Failed</option>
            <option value="error">Error</option>
          </select>
        )}

        {/* Source system filter (optional) */}
        {showSystemFilters && (
          <select value={sourceSystem} onChange={(e) => onSourceSystemChange?.(e.target.value)} className={selectClass}>
            <option value="">All Sources</option>
            {availableSystems.map(sys => (
              <option key={sys} value={sys}>{sys}</option>
            ))}
          </select>
        )}

        {/* Target system filter (optional) */}
        {showSystemFilters && (
          <select value={targetSystem} onChange={(e) => onTargetSystemChange?.(e.target.value)} className={selectClass}>
            <option value="">All Targets</option>
            {availableSystems.map(sys => (
              <option key={sys} value={sys}>{sys}</option>
            ))}
          </select>
        )}
      </div>
    </div>
  );
}
