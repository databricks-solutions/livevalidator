import React, { useState, useMemo, useEffect, useCallback } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { SampleDifferencesModal } from '../components/modals/SampleDifferencesModal';
import { ValidationResultsTable } from '../components/ValidationResultsTable';
import { DashboardDirectoryPane } from '../components/DashboardDirectoryPane';
import { DashboardTagPane, getTagColors } from '../components/DashboardTagPane';
import { DashboardPieChart, CATEGORIES, categorizeResult, computePieData } from '../components/DashboardPieChart';
import { dashboardService } from '../services/api';
import { useChartEntities, parseTags, getEntityKey } from '../hooks/useChartEntities';

const TIME_PRESETS = [
  { label: '12h', hours: 12 },
  { label: '1d', hours: 24 },
  { label: '2d', hours: 48 },
  { label: '3d', hours: 72 },
  { label: '5d', hours: 120 },
  { label: '7d', hours: 168 },
];

const formatDate = (date) => {
  return new Date(date).toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
};

export function DashboardView({ dashboardId, onNavigateToEntity, onBack }) {
  const [dashboard, setDashboard] = useState(null);
  const [charts, setCharts] = useState([]);
  const [dashLoading, setDashLoading] = useState(true);
  const [dashError, setDashError] = useState(null);
  const [projects, setProjects] = useState([]);
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);

  // Lazy load validation data
  const [rawData, setRawData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const data = Array.isArray(rawData) ? rawData : (rawData?.data || []);
  
  useEffect(() => {
    let cancelled = false;
    const fetchData = async () => {
      try {
        const res = await fetch('/api/validation-history?days_back=7&limit=10000');
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        const result = await res.json();
        if (!cancelled) {
          setRawData(result.data || result || []);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) setError({ message: err.message });
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => { cancelled = true; clearInterval(interval); };
  }, []);

  const [activePreset, setActivePreset] = useState('7d');
  const [customDateFrom, setCustomDateFrom] = useState('');
  const [customDateTo, setCustomDateTo] = useState('');

  const [selectedChartId, setSelectedChartId] = useState(null);
  const [drillDown, setDrillDown] = useState(null);
  const [selectedSample, setSelectedSample] = useState(null);
  const [loadingSampleId, setLoadingSampleId] = useState(null);
  const [localChartNames, setLocalChartNames] = useState({});
  const [loadedCharts, setLoadedCharts] = useState(null); // For initializing hook

  const loadDashboard = useCallback(async () => {
    setDashLoading(true);
    setDashError(null);
    try {
      const [dash, projs] = await Promise.all([
        dashboardService.get(dashboardId),
        dashboardService.listProjects(),
      ]);
      setDashboard(dash);
      setCharts(dash.charts || []);
      setSelectedChartId(dash.charts?.[0]?.id ?? null);
      setActivePreset(dash.time_range_preset || '7d');
      setCustomDateFrom(dash.time_range_from || '');
      setCustomDateTo(dash.time_range_to || '');
      setProjects(projs);

      const names = {};
      (dash.charts || []).forEach(c => {
        names[c.id] = c.name;
      });
      setLocalChartNames(names);
      setLoadedCharts(dash.charts); // Trigger hook initialization
      setHasUnsavedChanges(false);
    } catch (err) {
      setDashError(err.message);
    } finally {
      setDashLoading(false);
    }
  }, [dashboardId]);

  useEffect(() => {
    loadDashboard();
  }, [loadDashboard]);

  const handleClone = async (name, project) => {
    try {
      await dashboardService.clone(dashboardId, { name, project });
      onBack();
    } catch (err) {
      alert(`Clone failed: ${err.message}`);
    }
  };

  const handleRename = async (newName) => {
    try {
      const updated = await dashboardService.update(dashboardId, {
        name: newName,
        version: dashboard.version,
      });
      setDashboard(prev => ({ ...prev, name: newName, version: updated.version }));
    } catch (err) {
      alert(`Rename failed: ${err.message}`);
    }
  };

  const handleDelete = async () => {
    if (!confirm('Delete this dashboard? This cannot be undone.')) return;
    try {
      await dashboardService.delete(dashboardId);
      onBack();
    } catch (err) {
      alert(`Delete failed: ${err.message}`);
    }
  };

  const handlePublish = async (project) => {
    try {
      await dashboardService.update(dashboardId, {
        project,
        version: dashboard.version,
      });
      await loadDashboard();
    } catch (err) {
      alert(`Publish failed: ${err.message}`);
    }
  };

  const handleAddChart = async () => {
    try {
      const newChart = await dashboardService.addChart(dashboardId, {
        name: `Chart ${charts.length}`,
        filters: { includedEntityKeys: [] },
        sort_order: charts.length,
      });
      setCharts(prev => [...prev, newChart]);
      addChartEntityKeys(newChart.id);
      setLocalChartNames(prev => ({ ...prev, [newChart.id]: newChart.name }));
      setSelectedChartId(newChart.id);
      setDrillDown(null);
    } catch (err) {
      alert(`Add chart failed: ${err.message}`);
    }
  };

  const handleChartRename = (chartId, newName) => {
    setLocalChartNames(prev => ({ ...prev, [chartId]: newName }));
    setHasUnsavedChanges(true);
  };

  const handleRemoveChart = async (chartId) => {
    const chart = charts.find(c => c.id === chartId);
    if (!chart) return;
    try {
      await dashboardService.deleteChart(dashboardId, chartId);
      setCharts(prev => prev.filter(c => c.id !== chartId));
      setLocalChartFilters(prev => {
        const next = { ...prev };
        delete next[chartId];
        return next;
      });
      if (selectedChartId === chartId) {
        setSelectedChartId(charts[0]?.id ?? null);
      }
      setDrillDown(null);
    } catch (err) {
      alert(`Remove chart failed: ${err.message}`);
    }
  };

  const handleViewSample = async (validation) => {
    setLoadingSampleId(validation.id);
    try {
      const res = await fetch(`/api/validation-history/${validation.id}`);
      if (res.ok) {
        const detail = await res.json();
        setSelectedSample(detail);
      }
    } catch (e) {
      console.error('Failed to fetch validation details:', e);
    } finally {
      setLoadingSampleId(null);
    }
  };

  // ========== Data computation ==========

  const timeRange = useMemo(() => {
    if (activePreset === 'custom') {
      return {
        from: customDateFrom ? new Date(customDateFrom).getTime() : null,
        to: customDateTo ? new Date(customDateTo).getTime() : null,
      };
    }
    const preset = TIME_PRESETS.find(p => p.label === activePreset);
    if (!preset) return { from: null, to: null };
    const now = Date.now();
    return { from: now - preset.hours * 60 * 60 * 1000, to: now };
  }, [activePreset, customDateFrom, customDateTo]);

  const timeFilteredData = useMemo(() => {
    if (!data) return [];
    return data.filter(v => {
      const time = new Date(v.requested_at).getTime();
      if (timeRange.from && time < timeRange.from) return false;
      if (timeRange.to && time > timeRange.to) return false;
      return true;
    });
  }, [data, timeRange]);

  const filterOptions = useMemo(() => {
    const systemPairs = new Set();
    const schedules = new Set();
    const tags = new Set();
    timeFilteredData.forEach(v => {
      if (v.source_system_name && v.target_system_name) {
        systemPairs.add(`${v.source_system_name} → ${v.target_system_name}`);
      }
      if (v.schedule_id) schedules.add(v.schedule_id);
      parseTags(v.tags).forEach(t => tags.add(t));
    });
    return {
      systemPairs: Array.from(systemPairs).sort(),
      schedules: Array.from(schedules).sort(),
      tags: Array.from(tags).sort(),
    };
  }, [timeFilteredData]);

  const allTagsInData = useMemo(() => filterOptions.tags, [filterOptions]);

  const allEntityData = useMemo(() => {
    const entityMap = new Map();
    const sorted = [...timeFilteredData].sort((a, b) =>
      new Date(b.requested_at).getTime() - new Date(a.requested_at).getTime()
    );
    sorted.forEach(v => {
      const key = getEntityKey(v);
      if (!entityMap.has(key)) entityMap.set(key, v);
    });
    return entityMap;
  }, [timeFilteredData]);

  // Use the entity management hook
  const {
    getChartEntityKeys,
    updateChartEntityKeys,
    addChartEntityKeys,
    getEntityKeysForSave,
    getChartEntities,
    selectedChart,
    tagStates,
    handleTagClick,
    activateAllTags,
    deactivateAllTags,
  } = useChartEntities({
    charts,
    selectedChartId,
    allEntityData,
    allTagsInData,
    loadedCharts,
    onUnsavedChange: () => setHasUnsavedChanges(true),
    setDrillDown,
  });

  // Compute pie data for each chart
  const chartPieData = useMemo(() => {
    return charts.map(chart => {
      const entities = getChartEntities(chart.id);
      const chartEntityKeys = new Set(entities.map(getEntityKey));
      const tagsInChart = new Set();
      entities.forEach(v => parseTags(v.tags).forEach(t => tagsInChart.add(t)));

      const fullTags = [];
      const partialTagsForChart = [];
      tagsInChart.forEach(tag => {
        const allWithTag = Array.from(allEntityData.values()).filter(v => parseTags(v.tags).includes(tag));
        const inChart = allWithTag.filter(v => chartEntityKeys.has(getEntityKey(v)));
        if (inChart.length === allWithTag.length) fullTags.push(tag);
        else partialTagsForChart.push(tag);
      });

      return {
        chartId: chart.id,
        tagsInChart: Array.from(tagsInChart),
        fullTags,
        partialTags: partialTagsForChart,
        entities,
        pieData: computePieData(entities),
      };
    });
  }, [charts, getChartEntities, allEntityData]);

  // Save handler (needs hook's getEntityKeysForSave)
  const handleSave = async () => {
    try {
      const updatedDash = await dashboardService.update(dashboardId, {
        time_range_preset: activePreset,
        time_range_from: activePreset === 'custom' ? customDateFrom || null : null,
        time_range_to: activePreset === 'custom' ? customDateTo || null : null,
        version: dashboard.version,
      });
      setDashboard(prev => ({ ...prev, version: updatedDash.version }));

      const entityKeysForSave = getEntityKeysForSave();
      for (const chart of charts) {
        const keys = entityKeysForSave[chart.id];
        const filters = { includedEntityKeys: keys };
        const chartName = localChartNames[chart.id];
        const payload = { filters };
        if (chartName !== undefined && chartName !== chart.name) {
          payload.name = chartName;
        }
        await dashboardService.updateChart(dashboardId, chart.id, payload);
      }

      await loadDashboard();
    } catch (err) {
      alert(`Save failed: ${err.message}`);
    }
  };

  const latestPerEntity = useMemo(() => {
    if (!selectedChart) return [];
    return getChartEntities(selectedChart.id).map(v => ({
      ...v,
      _parsedTags: parseTags(v.tags),
    }));
  }, [selectedChart, getChartEntities]);

  const trendData = useMemo(() => {
    const chartEntityKeys = new Set(latestPerEntity.map(getEntityKey));
    const dayMap = new Map();
    timeFilteredData.forEach(v => {
      const date = new Date(v.requested_at);
      const dayKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
      if (!dayMap.has(dayKey)) dayMap.set(dayKey, []);
      dayMap.get(dayKey).push(v);
    });

    const result = [];
    Array.from(dayMap.keys()).sort().forEach(dayKey => {
      const dayRuns = dayMap.get(dayKey);
      const entityMap = new Map();
      dayRuns.sort((a, b) => new Date(b.requested_at).getTime() - new Date(a.requested_at).getTime());
      dayRuns.forEach(v => {
        const key = getEntityKey(v);
        if (!entityMap.has(key) && chartEntityKeys.has(key)) entityMap.set(key, v);
      });
      const counts = { success: 0, rowCountFail: 0, diffFail: 0, error: 0 };
      entityMap.forEach(v => { counts[categorizeResult(v)]++; });
      result.push({ date: dayKey, displayDate: formatDate(dayKey), ...counts });
    });
    return result;
  }, [timeFilteredData, latestPerEntity]);

  const drillDownEntities = useMemo(() => {
    if (!drillDown) return [];
    return latestPerEntity.filter(v => categorizeResult(v) === drillDown.category);
  }, [drillDown, latestPerEntity]);

  const selectChart = (chartId) => {
    if (chartId !== selectedChartId) {
      setSelectedChartId(chartId);
      setDrillDown(null);
    }
  };

  const handlePieClick = (entry, chartId) => {
    if (chartId !== selectedChartId) {
      setSelectedChartId(chartId);
      setDrillDown({ category: entry.category });
    } else {
      setDrillDown(drillDown?.category === entry.category ? null : { category: entry.category });
    }
  };

  const handleTimePresetChange = (preset) => {
    setActivePreset(preset);
    setHasUnsavedChanges(true);
    setDrillDown(null);
  };

  // ========== Rendering ==========

  if (dashLoading || loading) {
    return (
      <div className="flex flex-col items-center justify-center h-96">
        <div className="relative">
          <div className="w-16 h-16 border-4 border-charcoal-300 border-t-rust-light rounded-full animate-spin" />
          <div className="absolute inset-2 w-12 h-12 border-4 border-charcoal-300 border-b-purple-500 rounded-full animate-spin" style={{ animationDirection: 'reverse', animationDuration: '0.8s' }} />
        </div>
        <p className="text-gray-400 mt-6 text-lg">Loading dashboard...</p>
      </div>
    );
  }

  if (dashError) {
    return (
      <div className="text-center py-16">
        <p className="text-red-400 text-lg mb-2">Failed to load dashboard</p>
        <p className="text-gray-500 text-sm">{dashError}</p>
        <button onClick={onBack} className="mt-4 px-4 py-2 bg-charcoal-500 text-gray-200 rounded-lg hover:bg-charcoal-400">
          Back to Directory
        </button>
      </div>
    );
  }

  if (!dashboard) return null;

  return (
    <div>
      <div className="mb-4">
        <h2 className="text-3xl font-bold text-rust-light mb-1">Dashboard</h2>
        <p className="text-gray-400 text-base">Configure charts, apply filters, and analyze validation trends</p>
      </div>

      <DashboardDirectoryPane
        dashboard={dashboard}
        projects={projects}
        onSave={handleSave}
        onClone={handleClone}
        onDelete={handleDelete}
        onPublish={handlePublish}
        onRename={handleRename}
        onBack={onBack}
        hasUnsavedChanges={hasUnsavedChanges}
      />

      <div className="flex gap-4 mt-4">
        {/* Tag Pane (left) */}
        <DashboardTagPane
          allTags={allTagsInData}
          tagStates={tagStates}
          onTagClick={handleTagClick}
          onSelectAll={activateAllTags}
          onDeselectAll={deactivateAllTags}
          selectedChartName={localChartNames[selectedChart?.id] || selectedChart?.name || 'Overall'}
        />

        {/* Main content */}
        <div className="flex-1 min-w-0">
          {/* Time Range */}
          <div className="bg-gradient-to-br from-charcoal-500 to-charcoal-600 border border-charcoal-200 rounded-xl mb-4 shadow-lg overflow-hidden">
            <div className="p-4 bg-charcoal-500/50">
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-gray-300 text-sm font-semibold">Time Range:</span>
                {TIME_PRESETS.map(preset => (
                  <button
                    key={preset.label}
                    onClick={() => handleTimePresetChange(preset.label)}
                    className={`px-3 py-1.5 text-sm rounded-lg transition-all duration-200 font-medium ${
                      activePreset === preset.label
                        ? 'bg-gradient-to-r from-rust-light to-rust text-white shadow-lg shadow-rust/30 scale-105'
                        : 'bg-charcoal-600 text-gray-300 border border-charcoal-300 hover:border-rust-light/50 hover:bg-charcoal-500'
                    }`}
                  >
                    {preset.label}
                  </button>
                ))}
                <button
                  onClick={() => handleTimePresetChange('custom')}
                  className={`px-3 py-1.5 text-sm rounded-lg transition-all duration-200 font-medium ${
                    activePreset === 'custom'
                      ? 'bg-gradient-to-r from-purple-600 to-purple-700 text-white shadow-lg shadow-purple-500/30 scale-105'
                      : 'bg-charcoal-600 text-gray-300 border border-charcoal-300 hover:border-purple-500/50 hover:bg-charcoal-500'
                  }`}
                >
                  Custom
                </button>
                {activePreset === 'custom' && (
                  <div className="flex items-center gap-2 ml-2 bg-charcoal-600/50 px-3 py-1 rounded-lg">
                    <input
                      type="datetime-local"
                      value={customDateFrom}
                      onChange={(e) => { setCustomDateFrom(e.target.value); setHasUnsavedChanges(true); }}
                      className="px-2 py-1 bg-charcoal-700 border border-charcoal-300 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-purple-500"
                    />
                    <span className="text-gray-400 font-medium">→</span>
                    <input
                      type="datetime-local"
                      value={customDateTo}
                      onChange={(e) => { setCustomDateTo(e.target.value); setHasUnsavedChanges(true); }}
                      className="px-2 py-1 bg-charcoal-700 border border-charcoal-300 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-purple-500"
                    />
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Pie Charts */}
          <div className="mb-4">
            <button
              onClick={handleAddChart}
              className="w-full flex items-center justify-center gap-2 py-2 px-4 mb-2 bg-gradient-to-r from-purple-600/30 via-purple-500/40 to-purple-600/30 border border-purple-400/50 hover:border-purple-400 hover:from-purple-600/50 hover:via-purple-500/60 hover:to-purple-600/50 rounded-lg transition-all duration-200 group"
            >
              <span className="text-xl text-purple-300 group-hover:text-purple-200 transition-colors font-light">+</span>
              <span className="text-sm text-purple-300 group-hover:text-purple-200 transition-colors font-medium">Add Chart</span>
            </button>

            <div className="flex items-center justify-center gap-6 py-2 px-4 mb-4 bg-charcoal-500/50 border border-charcoal-300/50 rounded-lg">
              {Object.entries(CATEGORIES).map(([key, cat]) => (
                <div key={key} className="flex items-center gap-2">
                  <span className="w-3 h-3 rounded-full" style={{ backgroundColor: cat.color }} />
                  <span className="text-sm text-gray-300">{cat.label}</span>
                </div>
              ))}
            </div>

            <div className="flex flex-wrap gap-4 justify-center">
              {chartPieData.map(({ chartId, tagsInChart, fullTags, partialTags, entities, pieData }) => {
                const chart = charts.find(c => c.id === chartId);
                const isSelected = chartId === selectedChartId;
                const customName = localChartNames[chartId];
                const isDefaultName = /^Chart \d+$/.test(customName || '');
                const isAllEntities = getChartEntityKeys(chartId) === null;

                // Entity-centric: title based on tags in chart
                let chartTitle;
                if (!isDefaultName && customName) {
                  chartTitle = customName;
                } else if (isAllEntities || tagsInChart.length === 0) {
                  chartTitle = customName || chart?.name || 'All';
                } else if (fullTags.length <= 4) {
                  chartTitle = fullTags.join(', ') || tagsInChart.slice(0, 4).join(', ');
                } else {
                  chartTitle = `${fullTags.slice(0, 4).join(', ')} +${fullTags.length - 4}`;
                }

                return (
                  <DashboardPieChart
                    key={chartId}
                    title={chartTitle}
                    data={pieData}
                    total={entities.length}
                    isSelected={isSelected}
                    chartId={chartId}
                    drillDownCategory={drillDown?.category}
                    onPieClick={handlePieClick}
                    onSelect={selectChart}
                    onRemove={handleRemoveChart}
                    onRename={handleChartRename}
                    chartTags={tagsInChart}
                    chartFullTags={fullTags}
                    chartPartialTags={partialTags}
                    chartName={customName || chart?.name}
                  />
                );
              })}
            </div>
          </div>

          {/* Results Table */}
          <div className="mb-4 bg-charcoal-500 border border-charcoal-200 rounded-lg overflow-visible">
            <div className="p-3 bg-charcoal-400 border-b border-charcoal-200 rounded-t-lg flex items-center justify-between">
              <div>
                <h3 className="text-lg font-semibold text-gray-100 flex items-center gap-2 flex-wrap">
                  {localChartNames[selectedChart?.id] || selectedChart?.name || 'Overall'}
                  {(() => {
                    const cd = chartPieData.find(c => c.chartId === selectedChartId);
                    const tags = cd?.tagsInChart || [];
                    const fullSet = new Set(cd?.fullTags || []);
                    return tags.map(tag => {
                      const colors = getTagColors(tag);
                      const isFull = fullSet.has(tag);
                      return (
                        <span key={tag} className={`px-2 py-0.5 text-sm rounded ${
                          isFull
                            ? `${colors.bg} ${colors.text} border ${colors.border}`
                            : `${colors.text} ${colors.border} border-2 border-dashed`
                        }`}>{tag}</span>
                      );
                    });
                  })()}
                  {drillDown && (
                    <span
                      className="px-2 py-0.5 text-sm rounded flex items-center gap-1"
                      style={{
                        backgroundColor: `${CATEGORIES[drillDown.category].color}20`,
                        color: CATEGORIES[drillDown.category].color,
                        border: `1px solid ${CATEGORIES[drillDown.category].color}`,
                      }}
                    >
                      {CATEGORIES[drillDown.category].icon} {CATEGORIES[drillDown.category].label}
                      <button
                        onClick={() => setDrillDown(null)}
                        className="ml-1 hover:opacity-70 font-bold"
                        title="Clear status filter"
                      >
                        x
                      </button>
                    </span>
                  )}
                </h3>
                <p className="text-sm text-gray-400 mt-1">
                  {(drillDown ? drillDownEntities : latestPerEntity).length} {(drillDown ? drillDownEntities : latestPerEntity).length === 1 ? 'entity' : 'entities'}
                  {drillDown && ` (filtered from ${latestPerEntity.length})`}
                </p>
              </div>
            </div>
            <ValidationResultsTable
              data={drillDown ? drillDownEntities : latestPerEntity}
              onViewSample={handleViewSample}
              loadingSampleId={loadingSampleId}
              onEntityClick={onNavigateToEntity}
              emptyMessage="No entities in this chart"
            />
          </div>

          {selectedSample && (
            <SampleDifferencesModal
              validation={selectedSample}
              onClose={() => setSelectedSample(null)}
            />
          )}

          {/* Trend Chart */}
          <div className="relative overflow-hidden bg-gradient-to-br from-charcoal-500 to-charcoal-600 border border-charcoal-200 rounded-xl p-5 mb-4 shadow-lg">
            <div className="absolute inset-0 bg-gradient-to-r from-purple-500/5 via-transparent to-rust-light/5" />
            <div className="relative">
              <div className="mb-4">
                <h3 className="text-lg font-semibold text-gray-200">Trend Over Time</h3>
                <p className="text-sm text-gray-400">Latest status per entity, by day</p>
              </div>
              {trendData.length === 0 ? (
                <div className="flex flex-col items-center justify-center h-48 text-gray-500">
                  <p className="italic">No trend data available</p>
                </div>
              ) : (
                <ResponsiveContainer width="100%" height={280}>
                  <BarChart data={trendData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                    <defs>
                      {Object.entries(CATEGORIES).map(([key, cat]) => (
                        <linearGradient key={key} id={`barGradient-${key}`} x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%" stopColor={cat.gradient[0]} stopOpacity={1} />
                          <stop offset="100%" stopColor={cat.gradient[1]} stopOpacity={0.8} />
                        </linearGradient>
                      ))}
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.5} />
                    <XAxis dataKey="displayDate" tick={{ fill: '#9ca3af', fontSize: 12 }} axisLine={{ stroke: '#4b5563' }} tickLine={{ stroke: '#4b5563' }} />
                    <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} allowDecimals={false} axisLine={{ stroke: '#4b5563' }} tickLine={{ stroke: '#4b5563' }} />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'rgba(31, 41, 55, 0.95)',
                        border: '1px solid #4b5563',
                        borderRadius: '12px',
                        backdropFilter: 'blur(8px)',
                        boxShadow: '0 10px 40px rgba(0,0,0,0.3)',
                      }}
                      itemStyle={{ color: '#e5e7eb' }}
                      labelStyle={{ color: '#e5e7eb', fontWeight: 'bold', marginBottom: '8px' }}
                      cursor={{ fill: 'rgba(139, 92, 246, 0.1)' }}
                    />
                    <Legend
                      wrapperStyle={{ paddingTop: '15px' }}
                      formatter={(value) => <span className="text-gray-300 text-sm">{value}</span>}
                    />
                    <Bar dataKey="success" name="Success" stackId="a" fill="url(#barGradient-success)" radius={[0, 0, 0, 0]} />
                    <Bar dataKey="rowCountFail" name="Failed - Row Count" stackId="a" fill="url(#barGradient-rowCountFail)" />
                    <Bar dataKey="diffFail" name="Failed - Diff" stackId="a" fill="url(#barGradient-diffFail)" />
                    <Bar dataKey="error" name="Error" stackId="a" fill="url(#barGradient-error)" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
