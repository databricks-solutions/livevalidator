import React, { useState, useMemo, useEffect } from 'react';
import { PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Label } from 'recharts';
import { SampleDifferencesModal } from '../components/modals/SampleDifferencesModal';
import { ValidationResultsTable } from '../components/ValidationResultsTable';

// Status categories with colors and icons
const CATEGORIES = {
  success: { 
    label: 'Success', 
    color: '#22c55e', 
    gradient: ['#22c55e', '#16a34a'],
    icon: '✓',
    bgClass: 'bg-gradient-to-br from-green-900/50 to-green-900/20 border-green-600/50 text-green-300',
    glowClass: 'shadow-green-500/20'
  },
  rowCountFail: { 
    label: 'Failed - Row Count', 
    color: '#ef4444',
    gradient: ['#ef4444', '#dc2626'],
    icon: '≠',
    bgClass: 'bg-gradient-to-br from-red-900/50 to-red-900/20 border-red-600/50 text-red-300',
    glowClass: 'shadow-red-500/20'
  },
  diffFail: { 
    label: 'Failed - Diff', 
    color: '#eab308',
    gradient: ['#eab308', '#ca8a04'],
    icon: '△',
    bgClass: 'bg-gradient-to-br from-yellow-900/50 to-yellow-900/20 border-yellow-600/50 text-yellow-300',
    glowClass: 'shadow-yellow-500/20'
  },
  error: { 
    label: 'Error', 
    color: '#f97316',
    gradient: ['#f97316', '#ea580c'],
    icon: '⚠',
    bgClass: 'bg-gradient-to-br from-orange-900/50 to-orange-900/20 border-orange-600/50 text-orange-300',
    glowClass: 'shadow-orange-500/20'
  },
};

// Time presets in hours
const TIME_PRESETS = [
  { label: '12h', hours: 12 },
  { label: '1d', hours: 24 },
  { label: '2d', hours: 48 },
  { label: '3d', hours: 72 },
  { label: '5d', hours: 120 },
  { label: '7d', hours: 168 },
];

// Tag colors (same as TagBadge)
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

const getTagColors = (tag) => TAG_COLORS[hashTagName(tag)];

// Helper to parse tags from backend (may be JSON string or array)
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

// Categorize a validation result
const categorizeResult = (v) => {
  if (v.status === 'succeeded') return 'success';
  if (v.status === 'error') return 'error';
  if (v.status === 'failed') {
    return v.row_count_match === false ? 'rowCountFail' : 'diffFail';
  }
  return 'error'; // fallback
};

// Format date for display
const formatDate = (date) => {
  return new Date(date).toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
};

const formatDateTime = (date) => {
  return new Date(date).toLocaleString('en-US', { 
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' 
  });
};

export function DashboardView({ onNavigateToEntity }) {
  // Data fetching state (lazy loaded)
  const [rawData, setRawData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  // Handle both array and { data: [...] } response formats
  const data = Array.isArray(rawData) ? rawData : (rawData?.data || []);
  
  // Fetch data on mount and refresh periodically
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
  
  // Filter states
  const [activePreset, setActivePreset] = useState('7d');
  const [customDateFrom, setCustomDateFrom] = useState('');
  const [customDateTo, setCustomDateTo] = useState('');
  const [entityTypeFilter, setEntityTypeFilter] = useState('');
  const [compareModeFilter, setCompareModeFilter] = useState('');
  const [systemPairFilter, setSystemPairFilter] = useState('');
  const [scheduleFilter, setScheduleFilter] = useState('');
  
  // ============================================================================
  // MULTI-CHART STATE (Entity-Centric)
  // 
  // Each chart has { id, includedEntityKeys }
  // - includedEntityKeys: null = ALL entities (for "overall")
  // - includedEntityKeys: Set = specific entities included
  //
  // Tags shown are derived from entities in the chart:
  // - FULL: ALL entities with that tag are in the chart
  // - PARTIAL: SOME entities with that tag are in the chart
  // - NOT IN CHART: no entities with that tag are in the chart (shown grey)
  // ============================================================================
  const [charts, setCharts] = useState([
    { id: 'overall', includedEntityKeys: null } // null = all entities
  ]);
  const [selectedChartId, setSelectedChartId] = useState('overall');
  const [nextChartId, setNextChartId] = useState(1);
  
  // Drill-down state
  const [drillDown, setDrillDown] = useState(null); // { category, tag } or null
  
  // Sample differences modal state
  const [selectedSample, setSelectedSample] = useState(null);
  const [loadingSampleId, setLoadingSampleId] = useState(null);

  // Fetch full validation details on click (sample_differences excluded from list endpoint)
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

  // Compute time range from preset or custom
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

  // Filter data by time range
  const timeFilteredData = useMemo(() => {
    if (!data) return [];
    return data.filter(v => {
      const time = new Date(v.requested_at).getTime();
      if (timeRange.from && time < timeRange.from) return false;
      if (timeRange.to && time > timeRange.to) return false;
      return true;
    });
  }, [data, timeRange]);

  // Extract unique values for filter dropdowns
  const filterOptions = useMemo(() => {
    const systemPairs = new Set();
    const schedules = new Set();
    const tags = new Set();
    
    timeFilteredData.forEach(v => {
      if (v.source_system_name && v.target_system_name) {
        systemPairs.add(`${v.source_system_name} → ${v.target_system_name}`);
      }
      if (v.schedule_id) {
        schedules.add(v.schedule_id);
      }
      parseTags(v.tags).forEach(t => tags.add(t));
    });
    
    return {
      systemPairs: Array.from(systemPairs).sort(),
      schedules: Array.from(schedules).sort(),
      tags: Array.from(tags).sort(),
    };
  }, [timeFilteredData]);

  // Get all tags from the full dataset
  const allTagsInData = useMemo(() => {
    if (!data) return [];
    const tags = new Set();
    data.forEach(v => {
      parseTags(v.tags).forEach(t => tags.add(t));
    });
    return Array.from(tags).sort();
  }, [data]);

  // Apply additional filters
  const filteredData = useMemo(() => {
    return timeFilteredData.filter(v => {
      if (entityTypeFilter && v.entity_type !== entityTypeFilter) return false;
      if (compareModeFilter && v.compare_mode !== compareModeFilter) return false;
      if (systemPairFilter && `${v.source_system_name} → ${v.target_system_name}` !== systemPairFilter) return false;
      if (scheduleFilter && v.schedule_id !== parseInt(scheduleFilter)) return false;
      return true;
    });
  }, [timeFilteredData, entityTypeFilter, compareModeFilter, systemPairFilter, scheduleFilter]);

  // ============================================================================
  // ENTITY-CENTRIC TAG LOGIC
  // 
  // The chart contains ENTITIES, not tags. Tags are derived from entities:
  // - FULL tag: ALL entities with that tag are in the chart
  // - PARTIAL tag: SOME entities with that tag are in the chart  
  // - Grey tag: NO entities with that tag are in the chart
  //
  // Clicking a tag adds/removes ALL entities with that tag.
  // ============================================================================

  // Helper: Create unique key for an entity
  const getEntityKey = (v) => `${v.entity_type}_${v.entity_id}`;

  // All unique entity keys in filtered data (latest run per entity)
  const allEntityData = useMemo(() => {
    const entityMap = new Map();
    const sorted = [...filteredData].sort((a, b) => 
      new Date(b.requested_at).getTime() - new Date(a.requested_at).getTime()
    );
    sorted.forEach(v => {
      const key = getEntityKey(v);
      if (!entityMap.has(key)) {
        entityMap.set(key, v);
      }
    });
    return entityMap; // Map of entityKey -> latest validation result
  }, [filteredData]);

  // Tags present in the filtered data
  const tagsWithData = useMemo(() => {
    const tags = new Set();
    allEntityData.forEach(v => {
      parseTags(v.tags).forEach(t => tags.add(t));
    });
    return tags;
  }, [allEntityData]);

  // Get the currently selected chart
  const selectedChart = useMemo(() => {
    return charts.find(c => c.id === selectedChartId) || charts[0];
  }, [charts, selectedChartId]);

  // Helper: Get entities for a chart
  const getChartEntities = (chart) => {
    if (chart.includedEntityKeys === null) {
      // null = all entities
      return Array.from(allEntityData.values());
    }
    return Array.from(allEntityData.values()).filter(v => 
      chart.includedEntityKeys.has(getEntityKey(v))
    );
  };

  // Helper: Compute pie chart data for a set of entities
  const computePieData = (entities) => {
    const counts = { success: 0, rowCountFail: 0, diffFail: 0, error: 0 };
    entities.forEach(v => {
      const cat = categorizeResult(v);
      counts[cat]++;
    });
    
    return Object.entries(counts)
      .filter(([_, count]) => count > 0)
      .map(([key, count]) => ({
        name: CATEGORIES[key].label,
        value: count,
        color: CATEGORIES[key].color,
        category: key,
      }));
  };

  // Entities for the selected chart (used for Trend and Drill-down)
  const latestPerEntity = useMemo(() => {
    return getChartEntities(selectedChart);
  }, [selectedChart, allEntityData]);

  // Compute pie data for each chart, including full vs partial tag status
  const chartPieData = useMemo(() => {
    return charts.map(chart => {
      const entities = getChartEntities(chart);
      const chartEntityKeys = new Set(entities.map(getEntityKey));
      
      // Get all tags from entities in this chart
      const tagsInChart = new Set();
      entities.forEach(v => {
        parseTags(v.tags).forEach(t => tagsInChart.add(t));
      });
      
      // Determine which tags are "full" vs "partial"
      const fullTags = [];
      const partialTagsForChart = [];
      
      tagsInChart.forEach(tag => {
        // Get all entities (from full dataset) that have this tag
        const allEntitiesWithTag = Array.from(allEntityData.values()).filter(v => 
          parseTags(v.tags).includes(tag)
        );
        // How many of those are in the chart?
        const entitiesInChart = allEntitiesWithTag.filter(v => 
          chartEntityKeys.has(getEntityKey(v))
        );
        
        if (entitiesInChart.length === allEntitiesWithTag.length) {
          fullTags.push(tag);
        } else {
          partialTagsForChart.push(tag);
        }
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
  }, [charts, allEntityData]);

  // Tags and their states for the selected chart (for the banner)
  const { tagsInChart, fullTags, partialTags } = useMemo(() => {
    const chartData = chartPieData.find(c => c.chartId === selectedChartId);
    if (!chartData) return { tagsInChart: new Set(), fullTags: new Set(), partialTags: new Set() };
    return {
      tagsInChart: new Set(chartData.tagsInChart),
      fullTags: new Set(chartData.fullTags),
      partialTags: new Set(chartData.partialTags),
    };
  }, [chartPieData, selectedChartId]);

  // Trend chart data - latest per entity per day (uses selected chart's entities)
  const trendData = useMemo(() => {
    // Get the set of entity keys in the selected chart
    const chartEntityKeys = new Set(latestPerEntity.map(getEntityKey));
    
    // Group all runs by day
    const dayMap = new Map();
    
    filteredData.forEach(v => {
      const date = new Date(v.requested_at);
      const dayKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
      
      if (!dayMap.has(dayKey)) {
        dayMap.set(dayKey, []);
      }
      dayMap.get(dayKey).push(v);
    });
    
    // For each day, get latest per entity and count categories
    const result = [];
    const sortedDays = Array.from(dayMap.keys()).sort();
    
    sortedDays.forEach(dayKey => {
      const dayRuns = dayMap.get(dayKey);
      
      // Get latest per entity for this day, only for entities in the chart
      const entityMap = new Map();
      dayRuns.sort((a, b) => new Date(b.requested_at).getTime() - new Date(a.requested_at).getTime());
      dayRuns.forEach(v => {
        const key = getEntityKey(v);
        if (!entityMap.has(key) && chartEntityKeys.has(key)) {
          entityMap.set(key, v);
        }
      });
      
      const dayEntities = Array.from(entityMap.values());
      const counts = { success: 0, rowCountFail: 0, diffFail: 0, error: 0 };
      dayEntities.forEach(v => {
        counts[categorizeResult(v)]++;
      });
      
      result.push({
        date: dayKey,
        displayDate: formatDate(dayKey),
        ...counts,
      });
    });
    
    return result;
  }, [filteredData, latestPerEntity]);

  // Drill-down entities
  const drillDownEntities = useMemo(() => {
    if (!drillDown) return [];
    
    // Filter by category
    return latestPerEntity.filter(v => categorizeResult(v) === drillDown.category);
  }, [drillDown, latestPerEntity]);

  // ============================================================================
  // TAG CLICK HANDLERS (Entity-Centric)
  // 
  // Clicking a tag adds or removes ALL entities with that tag from the chart.
  // ============================================================================

  /** Update includedEntityKeys for a specific chart */
  const updateChartEntities = (chartId, updater) => {
    setCharts(prev => prev.map(chart => {
      if (chart.id !== chartId) return chart;
      
      // Get current included keys (null means all)
      const currentKeys = chart.includedEntityKeys === null 
        ? new Set(allEntityData.keys()) 
        : new Set(chart.includedEntityKeys);
      
      const newKeys = updater(currentKeys);
      return { ...chart, includedEntityKeys: newKeys };
    }));
  };

  /**
   * Toggle a tag: add or remove all entities with that tag from the chart.
   * - If ANY entity with this tag is in the chart → remove ALL entities with this tag
   * - If NO entities with this tag are in the chart → add ALL entities with this tag
   */
  const handleTagClick = (tag) => {
    if (!tagsWithData.has(tag)) return;
    
    // Get all entity keys that have this tag
    const entityKeysWithTag = new Set();
    allEntityData.forEach((v, key) => {
      if (parseTags(v.tags).includes(tag)) {
        entityKeysWithTag.add(key);
      }
    });
    
    // Check if any of these entities are currently in the chart
    const isInChart = tagsInChart.has(tag);
    
    updateChartEntities(selectedChartId, (currentKeys) => {
      if (isInChart) {
        // Remove all entities with this tag
        return new Set([...currentKeys].filter(k => !entityKeysWithTag.has(k)));
      } else {
        // Add all entities with this tag
        return new Set([...currentKeys, ...entityKeysWithTag]);
      }
    });
  };

  /** Add all entities to the selected chart */
  const activateAllTags = () => {
    setCharts(prev => prev.map(chart => 
      chart.id === selectedChartId 
        ? { ...chart, includedEntityKeys: null } // null = all entities
        : chart
    ));
  };

  /** Remove all entities from selected chart (empty chart) */
  const deactivateAllTags = () => {
    setCharts(prev => prev.map(chart => 
      chart.id === selectedChartId 
        ? { ...chart, includedEntityKeys: new Set() } // empty set
        : chart
    ));
  };

  // ============================================================================
  // CHART MANAGEMENT
  // ============================================================================

  /** Add a new chart (empty by default) */
  const addChart = () => {
    const newChart = {
      id: `chart-${nextChartId}`,
      includedEntityKeys: new Set(), // Empty chart
    };
    setCharts(prev => [...prev, newChart]);
    setSelectedChartId(newChart.id);
    setNextChartId(prev => prev + 1);
    setDrillDown(null);
  };

  /** Remove a chart (can't remove "overall") */
  const removeChart = (chartId) => {
    if (chartId === 'overall') return;
    setCharts(prev => prev.filter(c => c.id !== chartId));
    if (selectedChartId === chartId) {
      setSelectedChartId('overall');
    }
    setDrillDown(null);
  };

  /** Select a chart */
  const selectChart = (chartId) => {
    if (chartId !== selectedChartId) {
      setSelectedChartId(chartId);
      setDrillDown(null);
    }
  };

  const handlePieClick = (entry, chartId) => {
    // First, select this chart if not already selected
    if (chartId !== selectedChartId) {
      setSelectedChartId(chartId);
      setDrillDown({ category: entry.category });
    } else {
      // Toggle drilldown if same chart
      if (drillDown?.category === entry.category) {
        setDrillDown(null);
      } else {
        setDrillDown({ category: entry.category });
      }
    }
  };

  const clearAllFilters = () => {
    setActivePreset('7d');
    setCustomDateFrom('');
    setCustomDateTo('');
    // Reset all charts to default state
    setCharts([{ id: 'overall', includedEntityKeys: null }]);
    setSelectedChartId('overall');
    setEntityTypeFilter('');
    setCompareModeFilter('');
    setSystemPairFilter('');
    setScheduleFilter('');
    setDrillDown(null);
  };

  const hasActiveFilters = entityTypeFilter || compareModeFilter || systemPairFilter || scheduleFilter || (selectedChart.includedEntityKeys !== null && selectedChart.includedEntityKeys.size < allEntityData.size) || activePreset !== '7d' || charts.length > 1;

  // Custom pie label
  const renderPieLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent, name, value }) => {
    if (percent < 0.05) return null; // Don't show label for tiny slices
    const RADIAN = Math.PI / 180;
    const radius = innerRadius + (outerRadius - innerRadius) * 0.5;
    const x = cx + radius * Math.cos(-midAngle * RADIAN);
    const y = cy + radius * Math.sin(-midAngle * RADIAN);
    const pct = Math.round(percent * 100);
    
    return (
      <text x={x} y={y} fill="white" textAnchor="middle" dominantBaseline="central" fontWeight="bold" style={{ pointerEvents: 'none' }}>
        <tspan x={x} dy="-0.4em" fontSize={13}>{value}</tspan>
        <tspan x={x} dy="1.2em" fontSize={10} opacity={0.8}>{pct}%</tspan>
      </text>
    );
  };

  // Custom tooltip for pie chart
  const CustomPieTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-charcoal-600/95 backdrop-blur-sm border border-charcoal-300 rounded-lg px-3 py-2 shadow-xl">
          <div className="flex items-center gap-2">
            <span className="w-3 h-3 rounded-full" style={{ backgroundColor: data.color }} />
            <span className="font-medium text-gray-200">{data.name}</span>
          </div>
          <div className="text-2xl font-bold mt-1" style={{ color: data.color }}>
            {data.value} <span className="text-sm text-gray-400">entities</span>
          </div>
        </div>
      );
    }
    return null;
  };

  // Pie chart component with center label
  const PieChartCard = ({ title, data, tag = null, total, isChartSelected = false, chartId }) => {
    const isEmpty = data.length === 0;
    const entityCount = total ?? latestPerEntity.length;
    const isTagPie = tag !== null;
    
    // Custom center label component with text wrapping
    const CenterLabel = ({ viewBox }) => {
      const { cx, cy } = viewBox;
      // Split title by comma for wrapping
      const titleLines = title.includes(',') 
        ? title.split(',').map(s => s.trim())
        : [title];
      const lineHeight = 18;
      const totalTextHeight = titleLines.length * lineHeight + 20; // +20 for entity count
      const startY = cy - totalTextHeight / 2 + lineHeight / 2;
      
      return (
        <g style={{ pointerEvents: 'none' }}>
          {titleLines.map((line, i) => (
            <text 
              key={i}
              x={cx} 
              y={startY + i * lineHeight} 
              textAnchor="middle" 
              dominantBaseline="central" 
              className="fill-gray-200 font-semibold"
              style={{ fontSize: titleLines.length > 2 ? '14px' : '16px', pointerEvents: 'none' }}
            >
              {line}
            </text>
          ))}
          <text 
            x={cx} 
            y={startY + titleLines.length * lineHeight + 6} 
            textAnchor="middle" 
            dominantBaseline="central" 
            className="fill-gray-400 text-sm"
            style={{ pointerEvents: 'none' }}
          >
            {entityCount} {entityCount === 1 ? 'entity' : 'entities'}
          </text>
        </g>
      );
    };
    
    return (
      <div className={`relative overflow-hidden bg-gradient-to-br from-charcoal-500 to-charcoal-600 border border-charcoal-200 rounded-xl p-5 flex flex-col shadow-lg transition-all duration-300 hover:shadow-xl hover:border-charcoal-100 ${
        isTagPie ? 'ring-1 ring-purple-500/30' : ''
      }`}>
        {/* Decorative corner accent */}
        <div className="absolute top-0 right-0 w-20 h-20 bg-gradient-to-br from-rust-light/10 to-transparent rounded-bl-full" />
        
        {isEmpty ? (
          <div className="flex-1 flex flex-col items-center justify-center min-h-[400px] text-gray-500">
            <h3 className="text-lg font-semibold text-gray-200 mb-2">{title}</h3>
            <p className="italic">No data</p>
          </div>
        ) : (
          <div>
          <ResponsiveContainer width="100%" height={420}>
            <PieChart>
              <defs>
                {data.map((entry, index) => (
                  <linearGradient key={`gradient-${index}`} id={`pieGradient-${entry.category}`} x1="0" y1="0" x2="1" y2="1">
                    <stop offset="0%" stopColor={CATEGORIES[entry.category].gradient[0]} />
                    <stop offset="100%" stopColor={CATEGORIES[entry.category].gradient[1]} />
                  </linearGradient>
                ))}
                <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
                  <feDropShadow dx="0" dy="2" stdDeviation="3" floodOpacity="0.3"/>
                </filter>
              </defs>
              <Pie
                data={data}
                cx="50%"
                cy="50%"
                innerRadius={100}
                outerRadius={170}
                paddingAngle={3}
                dataKey="value"
                labelLine={false}
                label={renderPieLabel}
                onClick={(_, index) => handlePieClick(data[index], chartId)}
                cursor="pointer"
                filter="url(#shadow)"
                isAnimationActive={false}
              >
                {data.map((entry, index) => {
                  const isHighlighted = isChartSelected && drillDown?.category === entry.category;
                  return (
                  <Cell 
                    key={`cell-${index}`} 
                    fill={`url(#pieGradient-${entry.category})`}
                    stroke={isHighlighted ? '#fff' : 'rgba(255,255,255,0.1)'}
                    strokeWidth={isHighlighted ? 3 : 1}
                    className="transition-all duration-200 hover:opacity-80"
                    style={{ cursor: 'pointer' }}
                  />
                  );
                })}
                <Label content={<CenterLabel />} position="center" />
              </Pie>
              <Tooltip content={<CustomPieTooltip />} isAnimationActive={false} />
            </PieChart>
          </ResponsiveContainer>
          </div>
        )}
      </div>
    );
  };

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-96">
        <div className="relative">
          {/* Spinning rings */}
          <div className="w-16 h-16 border-4 border-charcoal-300 border-t-rust-light rounded-full animate-spin" />
          <div className="absolute inset-2 w-12 h-12 border-4 border-charcoal-300 border-b-purple-500 rounded-full animate-spin" style={{ animationDirection: 'reverse', animationDuration: '0.8s' }} />
        </div>
        <p className="text-gray-400 mt-6 text-lg">Loading dashboard data...</p>
        <p className="text-gray-500 text-sm mt-1">Crunching the numbers</p>
      </div>
    );
  }

  return (
    <div>
      {/* Header */}
      <div className="mb-6">
        <h2 className="text-3xl font-bold bg-gradient-to-r from-rust-light to-orange-400 bg-clip-text text-transparent mb-1">
          Reporting Dashboard
        </h2>
        <p className="text-gray-400 text-base">Validation health overview • Latest run per entity</p>
      </div>

      {/* Filters */}
      <div className="bg-gradient-to-br from-charcoal-500 to-charcoal-600 border border-charcoal-200 rounded-xl mb-6 shadow-lg overflow-hidden">
        {/* Time Range */}
        <div className="p-4 border-b border-charcoal-200/50 bg-charcoal-500/50">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-gray-300 text-sm font-semibold">
              Time Range:
            </span>
            {TIME_PRESETS.map(preset => (
              <button
                key={preset.label}
                onClick={() => { setActivePreset(preset.label); setDrillDown(null); }}
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
              onClick={() => setActivePreset('custom')}
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
                  onChange={(e) => setCustomDateFrom(e.target.value)}
                  className="px-2 py-1 bg-charcoal-700 border border-charcoal-300 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-purple-500 focus:ring-1 focus:ring-purple-500/50"
                />
                <span className="text-gray-400 font-medium">→</span>
                <input
                  type="datetime-local"
                  value={customDateTo}
                  onChange={(e) => setCustomDateTo(e.target.value)}
                  className="px-2 py-1 bg-charcoal-700 border border-charcoal-300 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-purple-500 focus:ring-1 focus:ring-purple-500/50"
                />
              </div>
            )}
          </div>
        </div>

        {/* Additional Filters */}
        <div className="p-4">
          <div className="flex justify-between items-center mb-3">
            <span className="text-gray-300 text-sm font-semibold">
              Filters:
            </span>
            {hasActiveFilters && (
              <button
                onClick={clearAllFilters}
                className="px-3 py-1 text-xs rounded-lg bg-red-500/20 text-red-300 border border-red-500/30 hover:bg-red-500/30 transition-all duration-200 font-medium flex items-center gap-1"
              >
                ✕ Clear All
              </button>
            )}
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
            {/* Entity Type */}
            <div className="relative">
              <select
                value={entityTypeFilter}
                onChange={(e) => { setEntityTypeFilter(e.target.value); setDrillDown(null); }}
                className="w-full px-3 py-2 bg-charcoal-700 border border-charcoal-300 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-purple-500 focus:ring-1 focus:ring-purple-500/50 appearance-none cursor-pointer transition-all hover:border-charcoal-100"
              >
                <option value="">All Entity Types</option>
                <option value="table">Tables</option>
                <option value="compare_query">Queries</option>
              </select>
              <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none text-gray-400">▾</div>
            </div>

            {/* Compare Mode */}
            <div className="relative">
              <select
                value={compareModeFilter}
                onChange={(e) => { setCompareModeFilter(e.target.value); setDrillDown(null); }}
                className="w-full px-3 py-2 bg-charcoal-700 border border-charcoal-300 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-purple-500 focus:ring-1 focus:ring-purple-500/50 appearance-none cursor-pointer transition-all hover:border-charcoal-100"
              >
                <option value="">All Compare Modes</option>
                <option value="primary_key">Primary Key</option>
                <option value="except_all">Except All</option>
              </select>
              <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none text-gray-400">▾</div>
            </div>

            {/* System Pair */}
            <div className="relative">
              <select
                value={systemPairFilter}
                onChange={(e) => { setSystemPairFilter(e.target.value); setDrillDown(null); }}
                className="w-full px-3 py-2 bg-charcoal-700 border border-charcoal-300 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-purple-500 focus:ring-1 focus:ring-purple-500/50 appearance-none cursor-pointer transition-all hover:border-charcoal-100"
              >
                <option value="">All System Pairs</option>
                {filterOptions.systemPairs.map(pair => (
                  <option key={pair} value={pair}>{pair}</option>
                ))}
              </select>
              <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none text-gray-400">▾</div>
            </div>

            {/* Schedule */}
            <div className="relative">
              <select
                value={scheduleFilter}
                onChange={(e) => { setScheduleFilter(e.target.value); setDrillDown(null); }}
                className="w-full px-3 py-2 bg-charcoal-700 border border-charcoal-300 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-purple-500 focus:ring-1 focus:ring-purple-500/50 appearance-none cursor-pointer transition-all hover:border-charcoal-100"
              >
                <option value="">All Schedules</option>
                {filterOptions.schedules.map(id => (
                  <option key={id} value={id}>Schedule #{id}</option>
                ))}
              </select>
              <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none text-gray-400">▾</div>
            </div>
          </div>
        </div>
      </div>

      {/* Activated Tags Banner */}
      {allTagsInData.length > 0 && (
        <div className="mb-6 p-4 bg-gradient-to-br from-charcoal-500 to-charcoal-600 border border-charcoal-200 rounded-xl shadow-lg">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-3">
              <h3 className="text-sm font-semibold text-gray-300">Tags</h3>
              <span className="px-2 py-0.5 text-xs rounded bg-purple-500/30 text-purple-200 border border-purple-500/50">
                {selectedChartId === 'overall' ? 'Overall' : `Chart ${selectedChartId.replace('chart-', '#')}`}
              </span>
              <span className="text-xs text-gray-500">Click to add/remove entities with that tag</span>
            </div>
            <div className="flex items-center gap-2">
              {latestPerEntity.length < allEntityData.size && (
                <button
                  onClick={activateAllTags}
                  className="px-2 py-1 text-xs rounded-lg bg-green-500/20 text-green-300 border border-green-500/30 hover:bg-green-500/30 transition-all"
                >
                  Add All
                </button>
              )}
              {latestPerEntity.length > 0 && (
                <button
                  onClick={deactivateAllTags}
                  className="px-2 py-1 text-xs rounded-lg bg-red-500/20 text-red-300 border border-red-500/30 hover:bg-red-500/30 transition-all"
                >
                  Remove All
                </button>
              )}
            </div>
          </div>
          <div className="flex flex-wrap gap-1.5">
            {allTagsInData.map(tag => {
              const hasData = tagsWithData.has(tag);
              const isInChart = tagsInChart.has(tag);
              const isFull = fullTags.has(tag);
              const isPartial = partialTags.has(tag);
              const colors = getTagColors(tag);
              
              return (
                <button
                  key={tag}
                  onClick={() => handleTagClick(tag)}
                  disabled={!hasData}
                  title={
                    !hasData 
                      ? 'No data in current filters' 
                      : isInChart 
                        ? isPartial 
                          ? 'Partial: some entities with this tag not in chart. Click to remove all.'
                          : 'Full: all entities with this tag are in chart. Click to remove all.'
                        : 'Click to add all entities with this tag'
                  }
                  style={{ cursor: hasData ? 'pointer' : 'not-allowed' }}
                  className={`px-2.5 py-1 text-sm rounded-md transition-all duration-200 font-medium border ${
                    !hasData
                      ? 'bg-charcoal-700/30 text-gray-600 border-charcoal-600/30 opacity-50'
                      : isFull
                        ? `${colors.bg} ${colors.text} ${colors.border} shadow-sm hover:opacity-80`
                        : isPartial
                          ? `${colors.text} ${colors.border} border-2 border-dashed shadow-sm hover:opacity-80`
                          : 'bg-charcoal-700/50 text-gray-500 border-charcoal-500/50 hover:bg-charcoal-600/50'
                  }`}
                >
                  {tag}
                </button>
              );
            })}
          </div>
          {latestPerEntity.length === 0 && (
            <p className="mt-3 text-sm text-gray-500 italic">
              No entities in chart - click tags to add entities
            </p>
          )}
        </div>
      )}

      {/* Pie Charts Section */}
      <div className="mb-6">
        {/* Add Chart Button - banner style */}
        <button
          onClick={addChart}
          className="w-full flex items-center justify-center gap-2 py-2 px-4 mb-2 bg-gradient-to-r from-purple-600/30 via-purple-500/40 to-purple-600/30 border border-purple-400/50 hover:border-purple-400 hover:from-purple-600/50 hover:via-purple-500/60 hover:to-purple-600/50 rounded-lg transition-all duration-200 group"
        >
          <span className="text-xl text-purple-300 group-hover:text-purple-200 transition-colors font-light">+</span>
          <span className="text-sm text-purple-300 group-hover:text-purple-200 transition-colors font-medium">Add Chart</span>
        </button>
        
        {/* Shared Legend Banner */}
        <div className="flex items-center justify-center gap-6 py-2 px-4 mb-4 bg-charcoal-500/50 border border-charcoal-300/50 rounded-lg">
          {Object.entries(CATEGORIES).map(([key, cat]) => (
            <div key={key} className="flex items-center gap-2">
              <span 
                className="w-3 h-3 rounded-full" 
                style={{ backgroundColor: cat.color }}
              />
              <span className="text-sm text-gray-300">{cat.label}</span>
            </div>
          ))}
        </div>
        
        {/* Charts Grid - max 3 per row, centered */}
        <div className="flex flex-wrap gap-4 justify-center">
          {chartPieData.map(({ chartId, tagsInChart: chartTags, fullTags: chartFullTags, partialTags: chartPartialTags, entities, pieData }) => {
            const isSelected = chartId === selectedChartId;
            const isOverall = chartId === 'overall';
            
            // Build chart title from FULLY activated tags only (max 3, then "+N")
            let chartTitle;
            if (isOverall) {
              chartTitle = 'Overall';
            } else if (chartFullTags.length === 0 && chartPartialTags.length === 0) {
              chartTitle = 'Empty';
            } else if (chartFullTags.length === 0) {
              chartTitle = '(partial only)';
            } else if (chartFullTags.length <= 4) {
              chartTitle = chartFullTags.join(', ');
            } else {
              chartTitle = `${chartFullTags.slice(0, 4).join(', ')} +${chartFullTags.length - 4}`;
            }
            
            return (
              <div
                key={chartId}
                onClick={() => selectChart(chartId)}
                className={`relative cursor-pointer transition-all duration-200 flex flex-col w-full md:w-[calc(50%-0.5rem)] lg:w-[calc(33.333%-0.75rem)] max-w-[400px] ${
                  isSelected 
                    ? 'ring-2 ring-purple-500 ring-offset-2 ring-offset-charcoal-600 rounded-xl' 
                    : 'hover:ring-1 hover:ring-charcoal-300 rounded-xl'
                }`}
              >
                {/* Delete button for non-overall charts */}
                {!isOverall && (
                  <button
                    onClick={(e) => { e.stopPropagation(); removeChart(chartId); }}
                    className="absolute top-2 right-2 z-10 w-6 h-6 flex items-center justify-center rounded-full bg-charcoal-700/80 text-gray-400 hover:bg-red-500/80 hover:text-white transition-all text-sm font-bold"
                    title="Remove chart"
                  >
                    x
                  </button>
                )}
                
                {/* Selected indicator */}
                {isSelected && (
                  <div className="absolute top-2 left-2 z-10 px-2 py-0.5 text-xs rounded bg-purple-500 text-white font-medium">
                    Selected
                  </div>
                )}
                
                <PieChartCard 
                  title={chartTitle} 
                  data={pieData}
                  total={entities.length}
                  isChartSelected={isSelected}
                  chartId={chartId}
                />
                
                {/* All tags at bottom (full + partial) */}
                {!isOverall && chartTags.length > 0 && (
                  <div className="px-3 pb-3 pt-1 bg-charcoal-600/50 rounded-b-xl border-t border-charcoal-400/30">
                    <div className="flex flex-wrap gap-1 justify-center">
                      {chartTags.map(tag => {
                        const colors = getTagColors(tag);
                        const isPartial = chartPartialTags.includes(tag);
                        return (
                          <span 
                            key={tag}
                            title={isPartial ? 'Partial: not all entities with this tag' : 'Full: all entities with this tag'}
                            className={`px-1.5 py-0.5 text-xs rounded ${
                              isPartial 
                                ? `${colors.text} ${colors.border} border-2 border-dashed`
                                : `${colors.bg} ${colors.text} border ${colors.border}`
                            }`}
                          >
                            {tag}
                          </span>
                        );
                      })}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
          
        </div>
      </div>

      {/* Results Table for Selected Chart */}
      <div className="mb-6 bg-charcoal-500 border border-charcoal-200 rounded-lg overflow-visible">
        <div className="p-3 bg-charcoal-400 border-b border-charcoal-200 rounded-t-lg flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-100 flex items-center gap-2 flex-wrap">
              {selectedChartId === 'overall' ? (
                'Overall'
              ) : tagsInChart.size === 0 ? (
                'Empty Chart'
              ) : (
                Array.from(tagsInChart).map(tag => {
                  const colors = getTagColors(tag);
                  const isFull = fullTags.has(tag);
                  return (
                    <span 
                      key={tag}
                      className={`px-2 py-0.5 text-sm rounded ${
                        isFull 
                          ? `${colors.bg} ${colors.text} border ${colors.border}`
                          : `${colors.text} ${colors.border} border-2 border-dashed`
                      }`}
                    >
                      {tag}
                    </span>
                  );
                })
              )}
              {drillDown && (
                <span 
                  className="px-2 py-0.5 text-sm rounded flex items-center gap-1"
                  style={{ 
                    backgroundColor: `${CATEGORIES[drillDown.category].color}20`,
                    color: CATEGORIES[drillDown.category].color,
                    border: `1px solid ${CATEGORIES[drillDown.category].color}`
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

      {/* Sample Differences Modal */}
      {selectedSample && (
        <SampleDifferencesModal 
          validation={selectedSample} 
          onClose={() => setSelectedSample(null)} 
        />
      )}

      {/* Trend Chart */}
      <div className="relative overflow-hidden bg-gradient-to-br from-charcoal-500 to-charcoal-600 border border-charcoal-200 rounded-xl p-5 mb-6 shadow-lg">
        {/* Decorative background */}
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
                <XAxis 
                  dataKey="displayDate" 
                  tick={{ fill: '#9ca3af', fontSize: 12 }} 
                  axisLine={{ stroke: '#4b5563' }}
                  tickLine={{ stroke: '#4b5563' }}
                />
                <YAxis 
                  tick={{ fill: '#9ca3af', fontSize: 12 }} 
                  allowDecimals={false}
                  axisLine={{ stroke: '#4b5563' }}
                  tickLine={{ stroke: '#4b5563' }}
                />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: 'rgba(31, 41, 55, 0.95)', 
                    border: '1px solid #4b5563', 
                    borderRadius: '12px',
                    backdropFilter: 'blur(8px)',
                    boxShadow: '0 10px 40px rgba(0,0,0,0.3)'
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
  );
}
