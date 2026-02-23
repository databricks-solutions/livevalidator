import React, { useState } from 'react';
import { getTagColors } from '../DashboardTagPane';

export function TagOverflowModal({ allTags, tagStates, onTagClick, onClose }) {
  const [search, setSearch] = useState('');

  const filtered = search
    ? allTags.filter(t => t.toLowerCase().includes(search.toLowerCase()))
    : allTags;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative bg-charcoal-600 border border-charcoal-200 rounded-xl shadow-2xl w-[500px] max-h-[70vh] flex flex-col">
        <div className="p-4 border-b border-charcoal-200 flex items-center justify-between">
          <h3 className="text-lg font-semibold text-gray-100">All Tags ({allTags.length})</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-white text-xl">✕</button>
        </div>

        <div className="px-4 pt-3">
          <input
            type="text"
            placeholder="Search tags..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full px-3 py-2 bg-charcoal-700 border border-charcoal-300 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-purple-500"
            autoFocus
          />
        </div>

        <div className="flex-1 overflow-y-auto p-4">
          <div className="flex flex-wrap gap-1.5">
            {filtered.map(tag => {
              const colors = getTagColors(tag);
              const state = tagStates[tag] || 'none';

              return (
                <button
                  key={tag}
                  onClick={() => onTagClick(tag)}
                  className={`px-2.5 py-1 text-sm rounded transition-all duration-200 font-medium border cursor-pointer ${
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
            {filtered.length === 0 && (
              <p className="text-gray-500 text-sm italic">No tags match "{search}"</p>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
