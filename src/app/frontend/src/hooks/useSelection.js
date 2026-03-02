import { useState, useCallback } from 'react';

/**
 * Hook for managing row selection state in list views.
 * @returns {Object} Selection state and handlers
 */
export function useSelection() {
  const [selectedIds, setSelectedIds] = useState(new Set());

  const handleSelectAll = useCallback((checked, filteredData) => {
    if (checked) {
      setSelectedIds(new Set(filteredData.map(row => row.id)));
    } else {
      setSelectedIds(new Set());
    }
  }, []);

  const handleSelectRow = useCallback((id, checked) => {
    setSelectedIds(prev => {
      const newSelected = new Set(prev);
      if (checked) {
        newSelected.add(id);
      } else {
        newSelected.delete(id);
      }
      return newSelected;
    });
  }, []);

  const clearSelection = useCallback(() => {
    setSelectedIds(new Set());
  }, []);

  const isAllSelected = useCallback((filteredData) => {
    return filteredData.length > 0 && filteredData.every(row => selectedIds.has(row.id));
  }, [selectedIds]);

  const isSomeSelected = useCallback((filteredData) => {
    return filteredData.some(row => selectedIds.has(row.id)) && !isAllSelected(filteredData);
  }, [selectedIds, isAllSelected]);

  return {
    selectedIds,
    handleSelectAll,
    handleSelectRow,
    clearSelection,
    isAllSelected,
    isSomeSelected,
    selectedCount: selectedIds.size,
    selectedArray: Array.from(selectedIds)
  };
}
