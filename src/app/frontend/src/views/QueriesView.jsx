import React, { useState } from 'react';
import { EntityListView } from '../components/EntityListView';
import { Checkbox } from '../components/Checkbox';
import { TagList } from '../components/TagBadge';
import { parseArray } from '../utils/arrays';

const queryConfig = {
  entityType: 'query',
  entityTypePlural: 'queries',
  title: 'Compare Queries',
  subtitle: 'Manage SQL query-to-query validation configurations',
  addButtonLabel: '+ Add Query',
  triggerAction: 'compare_query',
  apiEndpoint: '/api/queries',
  filterFn: (row, search) => {
    const name = row.name?.toLowerCase() || '';
    const sql = (row.src_sql || row.sql || '').toLowerCase();
    const tgtSql = (row.tgt_sql || '').toLowerCase();
    return name.includes(search) || sql.includes(search) || tgtSql.includes(search);
  },
  exportHeaders: ['name', 'sql', 'source', 'target', 'schedule_name', 'is_active', 'compare_mode', 'pk_columns', 'tags'],
  exportRowFn: (row, systems) => {
    const srcSystem = systems.find(s => s.id === row.src_system_id)?.name || '';
    const tgtSystem = systems.find(s => s.id === row.tgt_system_id)?.name || '';
    return [
      row.name, row.sql, srcSystem, tgtSystem,
      parseArray(row.schedules).join(','),
      row.is_active ? 'true' : 'false', row.compare_mode || 'except_all',
      Array.isArray(row.pk_columns) ? row.pk_columns.join(',') : (row.pk_columns || ''),
      parseArray(row.tags).join(',')
    ];
  },
  exportFilename: 'queries',
  deleteConfirmText: (count) => `Are you sure you want to delete ${count} quer${count !== 1 ? 'ies' : 'y'}? This action cannot be undone.`,
  columns: [
    { key: 'active', label: 'Active', align: 'center', className: 'w-12' },
    { key: 'name', label: 'Name' },
    { key: 'last_run', label: 'Last Run' },
    { key: 'source', label: 'Source' },
    { key: 'target', label: 'Target' },
    { key: 'sql', label: 'SQL' },
    { key: 'compare_mode', label: 'Compare Mode' },
    { key: 'pk_columns', label: 'PK Columns' },
    { key: 'schedules', label: 'Schedules' },
    { key: 'tags', label: 'Tags' },
    { key: 'actions', label: 'Actions' },
  ],
  renderRow: (props) => <QueryRow {...props} />,
};

function QueryRow({ row, isSelected, isHighlighted, highlightedRowRef, handleSelectRow, onEdit, onDelete, onTrigger, onNavigateToResult, renderCell, systems, entityTypePlural, triggerAction }) {
  const [expanded, setExpanded] = useState(false);
  const scheduleNames = parseArray(row.schedules).join(', ');
  
  return (
    <React.Fragment key={row.id}>
      <tr 
        ref={isHighlighted ? highlightedRowRef : null}
        className={`border-b border-charcoal-300/30 hover:bg-charcoal-400/50 transition-colors ${isSelected ? 'bg-purple-900/20' : ''} ${!row.is_active ? 'opacity-50' : ''} ${isHighlighted ? 'bg-rust-light/20 ring-2 ring-rust-light' : ''}`}
      >
        <td className="px-2 py-1 text-sm">
          <Checkbox checked={isSelected} onChange={(e) => handleSelectRow(row.id, e.target.checked)} />
        </td>
        <td className="px-2 py-1 text-center">
          {row.is_active ? <span className="text-green-500 text-lg" title="Active">●</span> : <span className="text-gray-600 text-lg" title="Disabled">○</span>}
        </td>
        <td className="px-2 py-1 text-gray-100 text-sm whitespace-nowrap">{row.name}</td>
        <td className="px-2 py-1">
          <StatusBadge row={row} onNavigateToResult={onNavigateToResult} />
        </td>
        <td className="px-2 py-1 text-gray-200 text-sm">{renderCell('queries', row, 'src_system_id', systems)}</td>
        <td className="px-2 py-1 text-gray-200 text-sm">{renderCell('queries', row, 'tgt_system_id', systems)}</td>
        <td 
          className="px-2 py-1 text-gray-300 text-sm max-w-xs truncate font-mono cursor-pointer hover:text-gray-100" 
          onClick={() => setExpanded(!expanded)}
          title="Click to view full query"
        >
          <span className="text-gray-500 mr-1">{expanded ? '▼' : '▶'}</span>{row.src_sql || row.sql}
        </td>
        <td className="px-2 py-1 text-gray-300 text-sm whitespace-nowrap">{row.compare_mode}</td>
        <td className="px-2 py-1 text-gray-300 text-sm">{row.pk_columns?.join(', ') || '-'}</td>
        <td className="px-2 py-1 text-purple-400 text-sm">{scheduleNames || '-'}</td>
        <td className="px-2 py-1"><TagList tags={parseArray(row.tags)} maxVisible={3} /></td>
        <td className="px-2 py-1 whitespace-nowrap">
          <button onClick={() => onEdit(row)} className="px-1.5 py-0.5 text-sm bg-purple-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-purple-500 mr-1">Edit</button>
          <button onClick={() => onDelete(entityTypePlural, row.id)} className="px-1.5 py-0.5 text-sm bg-red-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-red-500 mr-1">Del</button>
          <button onClick={() => onTrigger(triggerAction, row.id)} className="px-1.5 py-0.5 text-sm bg-green-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-green-500">▶️</button>
        </td>
      </tr>
      {expanded && (
        <tr className="border-b border-charcoal-300/30 bg-charcoal-600/30">
          <td colSpan="12" className="p-3">
            <pre className="bg-charcoal-700/50 rounded p-2 text-sm text-gray-200 font-mono overflow-x-auto whitespace-pre-wrap break-words">
{row.src_sql || row.sql || 'No SQL'}
            </pre>
            {row.tgt_sql && (
              <pre className="bg-charcoal-700/50 rounded p-2 text-sm text-gray-200 font-mono overflow-x-auto whitespace-pre-wrap break-words mt-2">
{row.tgt_sql}
              </pre>
            )}
          </td>
        </tr>
      )}
    </React.Fragment>
  );
}

function StatusBadge({ row, onNavigateToResult }) {
  if (row.last_run_status === 'succeeded') {
    return (
      <button onClick={() => onNavigateToResult(row.last_run_id)} className="px-1.5 py-0.5 text-sm rounded-full bg-green-900/40 text-green-300 border border-green-700 whitespace-nowrap hover:bg-green-900/60 transition-colors" title={`Last run: ${new Date(row.last_run_timestamp).toLocaleString()}`}>
        ✓ Success
      </button>
    );
  }
  if (row.last_run_status === 'failed') {
    return (
      <button onClick={() => onNavigateToResult(row.last_run_id)} className="px-1.5 py-0.5 text-sm rounded-full bg-red-900/40 text-red-300 border border-red-700 whitespace-nowrap hover:bg-red-900/60 transition-colors" title={`Last run: ${new Date(row.last_run_timestamp).toLocaleString()}`}>
        ✗ Failed
      </button>
    );
  }
  if (row.last_run_status === 'error') {
    return (
      <button onClick={() => onNavigateToResult(row.last_run_id)} className="px-1.5 py-0.5 text-sm rounded-full bg-orange-900/40 text-orange-300 border border-orange-700 hover:bg-orange-900/60 transition-colors animate-pulse max-w-[200px] truncate" title={row.last_run_error || 'Unknown error'}>
        ⚠ {row.last_run_error ? row.last_run_error.substring(0, 30) + (row.last_run_error.length > 30 ? '...' : '') : 'Error'}
      </button>
    );
  }
  return <span className="px-1.5 py-0.5 text-sm rounded-full bg-gray-900/40 text-gray-500 border border-gray-700 whitespace-nowrap">No recent run</span>;
}

export function QueriesView(props) {
  return <EntityListView {...props} config={queryConfig} />;
}
