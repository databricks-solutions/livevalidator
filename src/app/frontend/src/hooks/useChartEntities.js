import { useState, useCallback, useMemo, useEffect } from 'react';

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

const getEntityKey = (v) => `${v.entity_type}_${v.entity_id}`;

/**
 * Hook for entity-centric chart management.
 * 
 * Each chart stores a Set of included entity keys (or null for "all entities").
 * Tags are derived from entity membership:
 * - Full: ALL entities with that tag are in the chart
 * - Partial: SOME entities with that tag are in the chart
 * - None: NO entities with that tag are in the chart
 */
export function useChartEntities({
  charts,
  selectedChartId,
  allEntityData,
  allTagsInData,
  loadedCharts, // Charts from server (for initialization)
  onUnsavedChange,
  setDrillDown,
}) {
  const [entityKeys, setEntityKeys] = useState({});

  // Initialize entity keys when loadedCharts changes
  useEffect(() => {
    if (!loadedCharts) return;
    const keys = {};
    loadedCharts.forEach(c => {
      let f = c.filters || {};
      if (typeof f === 'string') {
        try { f = JSON.parse(f); } catch { f = {}; }
      }
      keys[c.id] = f.includedEntityKeys ? new Set(f.includedEntityKeys) : null;
    });
    setEntityKeys(keys);
  }, [loadedCharts]);

  // Get entity keys for a chart (null = all)
  const getChartEntityKeys = useCallback((chartId) => {
    return entityKeys[chartId];
  }, [entityKeys]);

  // Update entity keys for a chart
  const updateChartEntityKeys = useCallback((chartId, newKeys) => {
    setEntityKeys(prev => ({ ...prev, [chartId]: newKeys }));
    onUnsavedChange?.();
  }, [onUnsavedChange]);

  // Add a new chart with empty entity keys
  const addChartEntityKeys = useCallback((chartId) => {
    setEntityKeys(prev => ({ ...prev, [chartId]: new Set() }));
  }, []);

  // Get entity keys in saveable format (arrays instead of Sets)
  const getEntityKeysForSave = useCallback(() => {
    const result = {};
    Object.entries(entityKeys).forEach(([chartId, keys]) => {
      result[chartId] = keys === null ? null : Array.from(keys);
    });
    return result;
  }, [entityKeys]);

  // Filter entities by included keys (null = all)
  const getChartEntities = useCallback((chartId) => {
    const all = Array.from(allEntityData.values());
    const includedKeys = entityKeys[chartId];
    if (includedKeys === null || includedKeys === undefined) return all;
    return all.filter(v => includedKeys.has(getEntityKey(v)));
  }, [allEntityData, entityKeys]);

  // Get selected chart
  const selectedChart = useMemo(() => {
    return charts.find(c => c.id === selectedChartId) || charts[0];
  }, [charts, selectedChartId]);

  // Compute chart entities and tag info for selected chart
  const chartTagInfo = useMemo(() => {
    if (!selectedChart) return { tagsInChart: [], fullTags: [] };
    const entities = getChartEntities(selectedChart.id);
    const chartEntityKeys = new Set(entities.map(getEntityKey));
    const tagsInChart = new Set();
    entities.forEach(v => parseTags(v.tags).forEach(t => tagsInChart.add(t)));

    const fullTags = [];
    tagsInChart.forEach(tag => {
      const allWithTag = Array.from(allEntityData.values()).filter(v => parseTags(v.tags).includes(tag));
      const inChart = allWithTag.filter(v => chartEntityKeys.has(getEntityKey(v)));
      if (inChart.length === allWithTag.length) fullTags.push(tag);
    });

    return { tagsInChart: Array.from(tagsInChart), fullTags };
  }, [selectedChart, getChartEntities, allEntityData]);

  // Compute tag states for the selected chart
  const tagStates = useMemo(() => {
    const states = {};
    const fullSet = new Set(chartTagInfo.fullTags);
    const inChartSet = new Set(chartTagInfo.tagsInChart);
    allTagsInData.forEach(tag => {
      if (fullSet.has(tag)) states[tag] = 'full';
      else if (inChartSet.has(tag)) states[tag] = 'partial';
      else states[tag] = 'none';
    });
    return states;
  }, [allTagsInData, chartTagInfo]);

  // Toggle a tag: add or remove all entities with that tag
  const handleTagClick = useCallback((tag) => {
    if (!selectedChart) return;
    
    const entityKeysWithTag = new Set();
    allEntityData.forEach((v, key) => {
      if (parseTags(v.tags).includes(tag)) {
        entityKeysWithTag.add(key);
      }
    });
    
    const isInChart = chartTagInfo.tagsInChart.includes(tag);
    
    const currentKeys = entityKeys[selectedChart.id];
    const currentSet = currentKeys === null || currentKeys === undefined
      ? new Set(allEntityData.keys()) 
      : new Set(currentKeys);
    
    let newKeys;
    if (isInChart) {
      newKeys = new Set([...currentSet].filter(k => !entityKeysWithTag.has(k)));
    } else {
      newKeys = new Set([...currentSet, ...entityKeysWithTag]);
    }
    
    if (newKeys.size === allEntityData.size) {
      updateChartEntityKeys(selectedChart.id, null);
    } else {
      updateChartEntityKeys(selectedChart.id, newKeys);
    }
    setDrillDown?.(null);
  }, [selectedChart, allEntityData, chartTagInfo, entityKeys, updateChartEntityKeys, setDrillDown]);

  // Add all entities to chart
  const activateAllTags = useCallback(() => {
    if (!selectedChart) return;
    updateChartEntityKeys(selectedChart.id, null);
    setDrillDown?.(null);
  }, [selectedChart, updateChartEntityKeys, setDrillDown]);

  // Remove all entities from chart
  const deactivateAllTags = useCallback(() => {
    if (!selectedChart) return;
    updateChartEntityKeys(selectedChart.id, new Set());
    setDrillDown?.(null);
  }, [selectedChart, updateChartEntityKeys, setDrillDown]);

  return {
    // State management
    getChartEntityKeys,
    updateChartEntityKeys,
    addChartEntityKeys,
    getEntityKeysForSave,
    
    // Filtering
    getChartEntities,
    
    // Derived state
    selectedChart,
    tagStates,
    
    // Click handlers
    handleTagClick,
    activateAllTags,
    deactivateAllTags,
  };
}

export { parseTags, getEntityKey };
