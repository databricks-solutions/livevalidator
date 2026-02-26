import { useState, useMemo, useEffect, useRef, useCallback } from 'react';

export function useTagFilter(allTags = [], onTagsChange = null) {
  const [filterTags, setFilterTags] = useState([]);
  const [tagInput, setTagInput] = useState('');
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedSuggestionIndex, setSelectedSuggestionIndex] = useState(0);
  const tagInputRef = useRef(null);
  const inputElementRef = useRef(null);

  const addTagFilter = useCallback((tag) => {
    if (tag && !filterTags.includes(tag)) {
      setFilterTags(prev => [...prev, tag]);
      onTagsChange?.();
    }
    setTagInput('');
    setShowSuggestions(false);
    setSelectedSuggestionIndex(0);
  }, [filterTags, onTagsChange]);

  const removeTagFilter = useCallback((tag) => {
    setFilterTags(prev => prev.filter(t => t !== tag));
    onTagsChange?.();
  }, [onTagsChange]);

  const clearTags = () => {
    setFilterTags([]);
    setTagInput('');
    setShowSuggestions(false);
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

  const filterByTags = (items, getTagsFn) => {
    if (filterTags.length === 0) return items;
    return items.filter(item => {
      const itemTags = getTagsFn(item);
      return filterTags.every(ft => itemTags.includes(ft));
    });
  };

  return {
    filterTags,
    tagInput,
    setTagInput,
    showSuggestions,
    setShowSuggestions,
    selectedSuggestionIndex,
    tagInputRef,
    inputElementRef,
    tagSuggestions,
    addTagFilter,
    removeTagFilter,
    clearTags,
    handleTagKeyDown,
    filterByTags,
  };
}
