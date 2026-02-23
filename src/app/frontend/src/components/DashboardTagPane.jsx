import React, { useState } from 'react';
import { TagOverflowModal } from './modals/TagOverflowModal';

const TAG_COLORS = [
  { bg: 'bg-blue-900/60', text: 'text-blue-300', border: 'border-blue-500' },
  { bg: 'bg-purple-900/60', text: 'text-purple-300', border: 'border-purple-500' },
  { bg: 'bg-pink-900/60', text: 'text-pink-300', border: 'border-pink-500' },
  { bg: 'bg-red-900/60', text: 'text-red-300', border: 'border-red-500' },
  { bg: 'bg-orange-900/60', text: 'text-orange-300', border: 'border-orange-500' },
  { bg: 'bg-amber-900/60', text: 'text-amber-300', border: 'border-amber-500' },
  { bg: 'bg-yellow-900/60', text: 'text-yellow-300', border: 'border-yellow-500' },
  { bg: 'bg-lime-900/60', text: 'text-lime-300', border: 'border-lime-500' },
  { bg: 'bg-green-900/60', text: 'text-green-300', border: 'border-green-500' },
  { bg: 'bg-teal-900/60', text: 'text-teal-300', border: 'border-teal-500' },
  { bg: 'bg-cyan-900/60', text: 'text-cyan-300', border: 'border-cyan-500' },
  { bg: 'bg-indigo-900/60', text: 'text-indigo-300', border: 'border-indigo-500' },
];

const hashTagName = (name) => {
  let hash = 0;
  for (let i = 0; i < name.length; i++) {
    hash = ((hash << 5) - hash) + name.charCodeAt(i);
    hash = hash & hash;
  }
  return Math.abs(hash) % TAG_COLORS.length;
};

export const getTagColors = (tag) => TAG_COLORS[hashTagName(tag)];

const MAX_VISIBLE_TAGS = 30;

export function DashboardTagPane({
  allTags,
  selectedTags,
  tagStates,
  onTagClick,
  onSelectAll,
  onDeselectAll,
  selectedChartName,
}) {
  const [showOverflow, setShowOverflow] = useState(false);

  const visibleTags = allTags.slice(0, MAX_VISIBLE_TAGS);
  const hasOverflow = allTags.length > MAX_VISIBLE_TAGS;

  return (
    <div className="w-52 shrink-0 sticky top-0 self-start max-h-[calc(100vh-8rem)] overflow-hidden flex flex-col bg-charcoal-600 border border-charcoal-200 rounded-lg">
      <div className="p-3 border-b border-charcoal-200/50">
        <h3 className="text-sm font-semibold text-gray-300 mb-1">Tags</h3>
        {selectedChartName && (
          <span className="px-2 py-0.5 text-xs rounded bg-purple-500/30 text-purple-200 border border-purple-500/50">
            {selectedChartName}
          </span>
        )}
      </div>

      <div className="flex-1 p-2 overflow-hidden">
        {allTags.length === 0 ? (
          <p className="text-xs text-gray-500 italic p-2">No tags in data</p>
        ) : (
          <div className="flex flex-wrap gap-1">
            {visibleTags.map(tag => {
              const colors = getTagColors(tag);
              const state = tagStates[tag] || 'none';

              return (
                <button
                  key={tag}
                  onClick={() => onTagClick(tag)}
                  title={
                    state === 'full'
                      ? 'All entities with this tag are in chart. Click to remove.'
                      : state === 'partial'
                      ? 'Some entities with this tag are in chart. Click to remove.'
                      : 'Click to add entities with this tag.'
                  }
                  className={`px-2 py-0.5 text-xs rounded transition-all duration-200 font-medium border cursor-pointer ${
                    state === 'full'
                      ? `${colors.bg} ${colors.text} ${colors.border} shadow-sm hover:opacity-80`
                      : state === 'partial'
                      ? `${colors.text} ${colors.border} border-2 border-dashed shadow-sm hover:opacity-80`
                      : 'bg-charcoal-700/50 text-gray-500 border-charcoal-500/50 hover:bg-charcoal-600/50'
                  }`}
                >
                  {tag}
                </button>
              );
            })}
            {hasOverflow && (
              <button
                onClick={() => setShowOverflow(true)}
                className="px-2 py-0.5 text-xs rounded bg-charcoal-500 text-gray-300 border border-charcoal-300 hover:bg-charcoal-400 transition-colors"
              >
                +{allTags.length - MAX_VISIBLE_TAGS} more
              </button>
            )}
          </div>
        )}
      </div>

      {allTags.length > 0 && (
        <div className="p-2 border-t border-charcoal-200/50 flex gap-1">
          <button
            onClick={onSelectAll}
            className="flex-1 px-2 py-1 text-xs rounded bg-green-500/20 text-green-300 border border-green-500/30 hover:bg-green-500/30 transition-all"
          >
            All
          </button>
          <button
            onClick={onDeselectAll}
            className="flex-1 px-2 py-1 text-xs rounded bg-red-500/20 text-red-300 border border-red-500/30 hover:bg-red-500/30 transition-all"
          >
            None
          </button>
        </div>
      )}

      {showOverflow && (
        <TagOverflowModal
          allTags={allTags}
          tagStates={tagStates}
          onTagClick={onTagClick}
          onClose={() => setShowOverflow(false)}
        />
      )}
    </div>
  );
}
