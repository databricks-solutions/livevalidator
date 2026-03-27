import React from 'react';
import { EntityListView } from '../components/EntityListView';
import { Checkbox } from '../components/Checkbox';
import { TagList } from '../components/TagBadge';
import { parseArray } from '../utils/arrays';

const tableConfig = {
  entityType: 'table',
  entityTypePlural: 'tables',
  title: 'Tables',
  subtitle: 'Manage table-to-table validation configurations',
  addButtonLabel: '+ Add Table',
  triggerAction: 'table',
  apiEndpoint: '/api/tables',
  filterFn: (row, search) => {
    const srcTable = `${row.src_schema}.${row.src_table}`.toLowerCase();
    const tgtTable = `${row.tgt_schema}.${row.tgt_table}`.toLowerCase();
    const name = row.name?.toLowerCase() || '';
    return srcTable.includes(search) || tgtTable.includes(search) || name.includes(search);
  },
  exportHeaders: ['name', 'src_schema', 'src_table', 'tgt_schema', 'tgt_table', 'source', 'target', 'schedule_name', 'is_active', 'compare_mode', 'pk_columns', 'watermark_filter', 'exclude_columns', 'config_overrides', 'tags'],
  exportRowFn: (row, systems) => {
    const srcSystem = systems.find(s => s.id === row.src_system_id)?.name || '';
    const tgtSystem = systems.find(s => s.id === row.tgt_system_id)?.name || '';
    const configOverrides = row.config_overrides ? JSON.stringify(typeof row.config_overrides === 'string' ? JSON.parse(row.config_overrides) : row.config_overrides) : '';
    return [
      row.name, row.src_schema, row.src_table, row.tgt_schema, row.tgt_table,
      srcSystem, tgtSystem, parseArray(row.schedules).join(','),
      row.is_active ? 'true' : 'false', row.compare_mode || 'except_all',
      Array.isArray(row.pk_columns) ? row.pk_columns.join(',') : (row.pk_columns || ''),
      row.watermark_filter || '',
      Array.isArray(row.exclude_columns) ? row.exclude_columns.join(',') : (row.exclude_columns || ''),
      configOverrides,
      parseArray(row.tags).join(',')
    ];
  },
  exportFilename: 'tables',
  deleteConfirmText: (count) => `Are you sure you want to delete ${count} table${count !== 1 ? 's' : ''}? This action cannot be undone.`,
  columns: [
    { key: 'status', label: 'Status', align: 'center', className: 'w-16' },
    { key: 'table', label: 'Table' },
    { key: 'last_run', label: 'Last Run', className: 'w-24' },
    { key: 'source', label: 'Source' },
    { key: 'target', label: 'Target' },
    { key: 'compare_mode', label: 'Compare Mode', className: 'w-24' },
    { key: 'pk_columns', label: 'PK Columns', style: { maxWidth: '200px' } },
    { key: 'exclude_columns', label: 'Exclude Columns', style: { maxWidth: '250px' } },
    { key: 'schedules', label: 'Schedules', className: 'w-20' },
    { key: 'tags', label: 'Tags', className: 'w-24' },
    { key: 'actions', label: 'Actions', className: 'w-20' },
  ],
  renderRow: ({ row, isSelected, isHighlighted, highlightedRowRef, handleSelectRow, onEdit, onDelete, onTrigger, onNavigateToResult, renderCell, systems, entityTypePlural, triggerAction }) => {
    const scheduleNames = parseArray(row.schedules).join(', ');
    const srcTable = `${row.src_schema}.${row.src_table}`;
    const tgtTable = `${row.tgt_schema}.${row.tgt_table}`;
    const tablesMatch = srcTable === tgtTable;
    
    return (
      <tr 
        key={row.id}
        ref={isHighlighted ? highlightedRowRef : null}
        className={`border-b border-charcoal-300/30 hover:bg-charcoal-400/50 transition-colors ${isSelected ? 'bg-purple-900/20' : ''} ${!row.is_active ? 'opacity-50' : ''} ${isHighlighted ? 'bg-rust-light/20 ring-2 ring-rust-light' : ''}`}
      >
        <td className="px-2 py-1 text-sm">
          <Checkbox checked={isSelected} onChange={(e) => handleSelectRow(row.id, e.target.checked)} />
        </td>
        <td className="px-2 py-1 text-center">
          {row.is_active ? <span className="text-green-500 text-lg" title="Active">●</span> : <span className="text-gray-600 text-lg" title="Disabled">○</span>}
        </td>
        <td className="px-2 py-1 text-sm">
          <div className="flex flex-col gap-0.5">
            {tablesMatch ? (
              <span className="text-gray-100 whitespace-nowrap">{srcTable}</span>
            ) : (
              <div className="flex flex-col text-gray-100">
                <span className="whitespace-nowrap">src: {srcTable}</span>
                <span className="whitespace-nowrap">tgt: {tgtTable}</span>
              </div>
            )}
            <span className="text-gray-500 text-xs whitespace-nowrap">{row.name}</span>
          </div>
        </td>
        <td className="px-2 py-1">
          <StatusBadge row={row} onNavigateToResult={onNavigateToResult} />
        </td>
        <td className="px-2 py-1 text-gray-100 text-sm w-40">{renderCell('tables', row, 'src_system_id', systems)}</td>
        <td className="px-2 py-1 text-gray-100 text-sm w-40">{renderCell('tables', row, 'tgt_system_id', systems)}</td>
        <td className="px-2 py-1 text-gray-300 text-sm whitespace-nowrap">{row.compare_mode}</td>
        <td className="px-2 py-1 text-gray-300 text-sm" style={{ maxWidth: '200px' }}>{row.pk_columns?.join(', ') || '-'}</td>
        <td className="px-2 py-1 text-gray-300 text-sm" style={{ maxWidth: '250px' }}>{row.exclude_columns?.join(', ') || '-'}</td>
        <td className="px-2 py-1 text-purple-400 text-sm">{scheduleNames || '-'}</td>
        <td className="px-2 py-1"><TagList tags={parseArray(row.tags)} maxVisible={3} /></td>
        <td className="px-2 py-1 whitespace-nowrap">
          <button onClick={() => onEdit(row)} className="px-1.5 py-0.5 text-sm bg-purple-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-purple-500 mr-1">Edit</button>
          <button onClick={() => onDelete(entityTypePlural, row.id)} className="px-1.5 py-0.5 text-sm bg-red-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-red-500 mr-1">Del</button>
          <button onClick={() => onTrigger(triggerAction, row.id)} className="px-1.5 py-0.5 text-sm bg-green-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-green-500">▶️</button>
        </td>
      </tr>
    );
  },
};

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
  return <span className="px-1.5 py-0.5 text-sm rounded-full bg-gray-900/40 text-gray-500 border border-gray-700 whitespace-nowrap">No recent</span>;
}

export function TablesView(props) {
  return <EntityListView {...props} config={tableConfig} />;
}
