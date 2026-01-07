import React from 'react';

// 12 distinct colors for tags
const TAG_COLORS = [
  { bg: 'bg-blue-900/40', text: 'text-blue-300', border: 'border-blue-700' },
  { bg: 'bg-purple-900/40', text: 'text-purple-300', border: 'border-purple-700' },
  { bg: 'bg-pink-900/40', text: 'text-pink-300', border: 'border-pink-700' },
  { bg: 'bg-red-900/40', text: 'text-red-300', border: 'border-red-700' },
  { bg: 'bg-orange-900/40', text: 'text-orange-300', border: 'border-orange-700' },
  { bg: 'bg-amber-900/40', text: 'text-amber-300', border: 'border-amber-700' },
  { bg: 'bg-yellow-900/40', text: 'text-yellow-300', border: 'border-yellow-700' },
  { bg: 'bg-lime-900/40', text: 'text-lime-300', border: 'border-lime-700' },
  { bg: 'bg-green-900/40', text: 'text-green-300', border: 'border-green-700' },
  { bg: 'bg-teal-900/40', text: 'text-teal-300', border: 'border-teal-700' },
  { bg: 'bg-cyan-900/40', text: 'text-cyan-300', border: 'border-cyan-700' },
  { bg: 'bg-indigo-900/40', text: 'text-indigo-300', border: 'border-indigo-700' },
];

// Simple hash function to consistently assign colors to tag names
function hashTagName(name) {
  let hash = 0;
  for (let i = 0; i < name.length; i++) {
    hash = ((hash << 5) - hash) + name.charCodeAt(i);
    hash = hash & hash; // Convert to 32-bit integer
  }
  return Math.abs(hash) % TAG_COLORS.length;
}

export function TagBadge({ tag, onRemove, className = '' }) {
  const colorIndex = hashTagName(tag);
  const colors = TAG_COLORS[colorIndex];

  return (
    <span 
      className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs border ${colors.bg} ${colors.text} ${colors.border} ${className}`}
    >
      <span>{tag}</span>
      {onRemove && (
        <button
          onClick={(e) => {
            e.stopPropagation();
            onRemove(tag);
          }}
          className="hover:opacity-70 transition-opacity"
          title="Remove tag"
        >
          ×
        </button>
      )}
    </span>
  );
}

export function TagList({ tags = [], onRemove, maxVisible = 4, className = '' }) {
  if (tags.length === 0) {
    return <span className="text-gray-500 text-xs">-</span>;
  }

  // For 5+ tags, show 3 tags + "+N more"
  const displayCount = tags.length > 4 ? 3 : Math.min(tags.length, maxVisible);
  const visibleTags = tags.slice(0, displayCount);
  const remainingCount = Math.max(0, tags.length - displayCount);

  return (
    <div className={`flex flex-wrap gap-1 items-center ${className}`}>
      {visibleTags.map((tag, idx) => (
        <TagBadge key={`${tag}-${idx}`} tag={tag} onRemove={onRemove} />
      ))}
      {remainingCount > 0 && (
        <span className="inline-flex px-2 py-0.5 rounded-full text-xs bg-gray-900/40 text-gray-400 border border-gray-700">
          +{remainingCount} more
        </span>
      )}
    </div>
  );
}

