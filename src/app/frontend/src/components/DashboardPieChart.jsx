import React from 'react';
import { PieChart, Pie, Cell, Tooltip, Label, ResponsiveContainer } from 'recharts';
import { getTagColors } from './DashboardTagPane';

const CATEGORIES = {
  success: {
    label: 'Success',
    color: '#22c55e',
    gradient: ['#22c55e', '#16a34a'],
    icon: '✓',
  },
  rowCountFail: {
    label: 'Failed - Row Count',
    color: '#ef4444',
    gradient: ['#ef4444', '#dc2626'],
    icon: '≠',
  },
  diffFail: {
    label: 'Failed - Diff',
    color: '#eab308',
    gradient: ['#eab308', '#ca8a04'],
    icon: '△',
  },
  error: {
    label: 'Error',
    color: '#f97316',
    gradient: ['#f97316', '#ea580c'],
    icon: '⚠',
  },
};

export { CATEGORIES };

export const categorizeResult = (v) => {
  if (v.status === 'succeeded') return 'success';
  if (v.status === 'error') return 'error';
  if (v.status === 'failed') {
    return v.row_count_match === false ? 'rowCountFail' : 'diffFail';
  }
  return 'error';
};

export const computePieData = (entities) => {
  const counts = { success: 0, rowCountFail: 0, diffFail: 0, error: 0 };
  entities.forEach(v => {
    counts[categorizeResult(v)]++;
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

const renderPieLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent, value }) => {
  if (percent < 0.05) return null;
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

export function DashboardPieChart({
  title,
  data,
  total,
  isSelected,
  chartId,
  drillDownCategory,
  onPieClick,
  onSelect,
  onRemove,
  isOverall,
  chartTags,
  chartFullTags,
  chartPartialTags,
}) {
  const isEmpty = data.length === 0;
  const entityCount = total ?? 0;

  const CenterLabel = ({ viewBox }) => {
    const { cx, cy } = viewBox;
    const titleLines = title.includes(',')
      ? title.split(',').map(s => s.trim())
      : [title];
    const lineHeight = 18;
    const totalTextHeight = titleLines.length * lineHeight + 20;
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
    <div
      onClick={() => onSelect(chartId)}
      className={`relative cursor-pointer transition-all duration-200 flex flex-col w-full md:w-[calc(50%-0.5rem)] lg:w-[calc(33.333%-0.75rem)] max-w-[400px] ${
        isSelected
          ? 'ring-2 ring-purple-500 ring-offset-2 ring-offset-charcoal-600 rounded-xl'
          : 'hover:ring-1 hover:ring-charcoal-300 rounded-xl'
      }`}
    >
      {!isOverall && (
        <button
          onClick={(e) => { e.stopPropagation(); onRemove(chartId); }}
          className="absolute top-2 right-2 z-10 w-6 h-6 flex items-center justify-center rounded-full bg-charcoal-700/80 text-gray-400 hover:bg-red-500/80 hover:text-white transition-all text-sm font-bold"
          title="Remove chart"
        >
          x
        </button>
      )}

      {isSelected && (
        <div className="absolute top-2 left-2 z-10 px-2 py-0.5 text-xs rounded bg-purple-500 text-white font-medium">
          Selected
        </div>
      )}

      <div className={`relative overflow-hidden bg-gradient-to-br from-charcoal-500 to-charcoal-600 border border-charcoal-200 rounded-xl p-5 flex flex-col shadow-lg transition-all duration-300 hover:shadow-xl hover:border-charcoal-100`}>
        <div className="absolute top-0 right-0 w-20 h-20 bg-gradient-to-br from-rust-light/10 to-transparent rounded-bl-full" />

        {isEmpty ? (
          <div className="flex-1 flex flex-col items-center justify-center min-h-[400px] text-gray-500">
            <h3 className="text-lg font-semibold text-gray-200 mb-2">{title}</h3>
            <p className="italic">No data</p>
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={420}>
            <PieChart>
              <defs>
                {data.map((entry, index) => (
                  <linearGradient key={`gradient-${index}`} id={`pieGradient-${chartId}-${entry.category}`} x1="0" y1="0" x2="1" y2="1">
                    <stop offset="0%" stopColor={CATEGORIES[entry.category].gradient[0]} />
                    <stop offset="100%" stopColor={CATEGORIES[entry.category].gradient[1]} />
                  </linearGradient>
                ))}
                <filter id={`shadow-${chartId}`} x="-20%" y="-20%" width="140%" height="140%">
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
                onClick={(_, index) => onPieClick(data[index], chartId)}
                cursor="pointer"
                filter={`url(#shadow-${chartId})`}
                isAnimationActive={false}
              >
                {data.map((entry, index) => {
                  const isHighlighted = isSelected && drillDownCategory === entry.category;
                  return (
                    <Cell
                      key={`cell-${index}`}
                      fill={`url(#pieGradient-${chartId}-${entry.category})`}
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
        )}
      </div>

      {!isOverall && chartTags && chartTags.length > 0 && (
        <div className="px-3 pb-3 pt-1 bg-charcoal-600/50 rounded-b-xl border-t border-charcoal-400/30">
          <div className="flex flex-wrap gap-1 justify-center">
            {chartTags.map(tag => {
              const colors = getTagColors(tag);
              const isPartial = chartPartialTags?.includes(tag);
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
}
