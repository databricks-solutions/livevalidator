import React, { useState, useRef, useEffect } from 'react';
import { TagBadge } from './TagBadge';

export function TagInput({ 
  tags = [], 
  allTags = [], 
  onChange, 
  placeholder = "Type and press Enter to add tags...",
  className = '' 
}) {
  const [input, setInput] = useState('');
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const inputRef = useRef(null);
  const wrapperRef = useRef(null);

  // Filter suggestions based on input and exclude already selected tags
  const suggestions = input.trim()
    ? allTags.filter(tag => 
        tag.toLowerCase().includes(input.toLowerCase()) && 
        !tags.includes(tag)
      )
    : [];

  // Check if input is a new tag (not in allTags)
  const isNewTag = input.trim() && !allTags.some(tag => tag.toLowerCase() === input.trim().toLowerCase());

  // Combine suggestions with the new tag option if applicable
  const allOptions = isNewTag 
    ? [...suggestions, input.trim()] 
    : suggestions;

  // Close suggestions when clicking outside
  useEffect(() => {
    function handleClickOutside(event) {
      if (wrapperRef.current && !wrapperRef.current.contains(event.target)) {
        setShowSuggestions(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const addTag = (tag) => {
    const trimmedTag = tag.trim();
    if (trimmedTag && !tags.includes(trimmedTag)) {
      onChange([...tags, trimmedTag]);
    }
    setInput('');
    setShowSuggestions(false);
    setSelectedIndex(0);
  };

  const removeTag = (tagToRemove) => {
    onChange(tags.filter(tag => tag !== tagToRemove));
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && input.trim()) {
      e.preventDefault();
      if (allOptions.length > 0) {
        addTag(allOptions[selectedIndex]);
      } else {
        addTag(input);
      }
    } else if (e.key === 'Backspace' && !input && tags.length > 0) {
      removeTag(tags[tags.length - 1]);
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSelectedIndex(prev => Math.min(prev + 1, allOptions.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setSelectedIndex(prev => Math.max(prev - 1, 0));
    } else if (e.key === 'Escape') {
      setShowSuggestions(false);
    }
  };

  const handleInputChange = (e) => {
    setInput(e.target.value);
    setShowSuggestions(true);
    setSelectedIndex(0);
  };

  return (
    <div ref={wrapperRef} className={`relative ${className}`}>
      <div className="flex flex-wrap gap-1.5 p-2 bg-charcoal-600 border border-charcoal-300 rounded-md min-h-[42px] focus-within:border-purple-500">
        {tags.map((tag, idx) => (
          <TagBadge key={`${tag}-${idx}`} tag={tag} onRemove={removeTag} />
        ))}
        <input
          ref={inputRef}
          type="text"
          value={input}
          onChange={handleInputChange}
          onKeyDown={handleKeyDown}
          onFocus={() => setShowSuggestions(true)}
          placeholder={tags.length === 0 ? placeholder : ''}
          className="flex-1 min-w-[120px] bg-transparent border-none outline-none text-gray-200 text-sm placeholder-gray-500"
        />
      </div>

      {/* Suggestions dropdown */}
      {showSuggestions && allOptions.length > 0 && (
        <div className="absolute z-50 w-full mt-1 bg-charcoal-600 border border-charcoal-300 rounded-md shadow-lg max-h-48 overflow-y-auto">
          {allOptions.map((option, idx) => {
            const isNew = option === input.trim() && isNewTag;
            return (
              <div
                key={idx}
                onClick={() => addTag(option)}
                className={`px-3 py-2 cursor-pointer text-sm ${
                  idx === selectedIndex 
                    ? 'bg-purple-600 text-white' 
                    : 'text-gray-200 hover:bg-charcoal-500'
                }`}
              >
                {isNew ? (
                  <span>
                    <span className="text-green-400">+ Create:</span> {option}
                  </span>
                ) : (
                  option
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export function BulkTagModal({ isOpen, onClose, mode, onSubmit, entityCount }) {
  const [tags, setTags] = useState([]);
  const [allTags, setAllTags] = useState([]);

  useEffect(() => {
    if (isOpen) {
      // Fetch all existing tags
      fetch('/api/tags')
        .then(r => r.json())
        .then(data => setAllTags(data.map(t => t.name)))
        .catch(() => setAllTags([]));
      setTags([]);
    }
  }, [isOpen]);

  if (!isOpen) return null;

  const handleSubmit = () => {
    if (tags.length > 0) {
      onSubmit(tags);
      onClose();
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div className="bg-charcoal-500 border border-charcoal-200 rounded-lg p-4 max-w-md w-full" onClick={e => e.stopPropagation()}>
        <h3 className="text-xl font-bold text-purple-400 mb-3">
          {mode === 'add' ? '+ Add Tags' : '− Remove Tags'}
        </h3>
        <p className="text-gray-300 mb-4 text-sm">
          {mode === 'add' 
            ? `Add tags to ${entityCount} selected item${entityCount !== 1 ? 's' : ''}`
            : `Remove tags from ${entityCount} selected item${entityCount !== 1 ? 's' : ''}`
          }
        </p>
        
        <TagInput 
          tags={tags}
          allTags={allTags}
          onChange={setTags}
          placeholder={mode === 'add' ? "Add tags..." : "Remove tags..."}
        />

        <div className="flex gap-2 justify-end mt-4">
          <button 
            onClick={onClose}
            className="px-3 py-1.5 bg-charcoal-600 text-gray-200 rounded hover:bg-charcoal-500"
          >
            Cancel
          </button>
          <button 
            onClick={handleSubmit}
            disabled={tags.length === 0}
            className={`px-3 py-1.5 rounded text-white ${
              tags.length === 0 
                ? 'bg-gray-600 cursor-not-allowed' 
                : mode === 'add'
                  ? 'bg-green-600 hover:bg-green-500'
                  : 'bg-red-600 hover:bg-red-500'
            }`}
          >
            {mode === 'add' ? 'Add' : 'Remove'}
          </button>
        </div>
      </div>
    </div>
  );
}

