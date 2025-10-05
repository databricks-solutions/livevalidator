import React, { useState, useEffect, useRef } from 'react';

// Hooks
import { useFetch } from './hooks/useFetch';

// Components
import { Sidebar } from './components/Sidebar';
import { ErrorBox } from './components/ErrorBox';
import {
  VersionConflictDialog,
  UploadCSVModal,
  TableModal,
  QueryModal,
  ScheduleModal,
  SystemModal
} from './components/modals';

// Services
import { apiCall } from './services/api';

const DEFAULT_USER = "user@company.com";

// InlineEditCell Component (kept in App.jsx as it's tightly coupled to handleCellEdit)
const InlineEditCell = ({ value, onSave, onCancel, type = "text", options = [] }) => {
  const [val, setVal] = useState(value);
  const inputRef = useRef(null);
  
  useEffect(() => { inputRef.current?.focus(); }, []);
  
  const handleBlur = () => { 
    if (val !== value) onSave(val); 
    else onCancel(); 
  };
  
  const handleKey = (e) => {
    if (e.key === "Enter") { e.preventDefault(); onSave(val); }
    if (e.key === "Escape") { e.preventDefault(); onCancel(); }
  };
  
  if (type === "select") {
    return (
      <select 
        ref={inputRef} 
        value={val} 
        onChange={e => { setVal(+e.target.value); onSave(+e.target.value); }} 
        onBlur={handleBlur} 
        className="p-1 w-full bg-charcoal-400 text-gray-100 border border-purple-500 rounded focus:outline-none focus:ring-2 focus:ring-purple-500"
      >
        {options.map(o => <option key={o.id} value={o.id}>{o.name}</option>)}
      </select>
    );
  }
  
  return (
    <input 
      ref={inputRef} 
      type={type} 
      value={val} 
      onChange={e => setVal(type === "number" ? +e.target.value : e.target.value)} 
      onBlur={handleBlur} 
      onKeyDown={handleKey} 
      className="p-1 w-full bg-charcoal-400 text-gray-100 border border-purple-500 rounded focus:outline-none focus:ring-2 focus:ring-purple-500" 
    />
  );
};

export default function App() {
  const [view, setView] = useState('tables');
  const [conflict, setConflict] = useState(null);
  
  // Data fetching
  const tbl = useFetch(`/api/tables`, []);
  const qs = useFetch(`/api/queries`, []);
  const sc = useFetch(`/api/schedules`, []);
  const sys = useFetch(`/api/systems`, []);
  
  // Check if database needs initialization
  const setupRequired = [tbl.error, qs.error, sc.error, sys.error].some(
    err => err?.action === "setup_required"
  );
  
  // Fetch bindings for all entities
  const [bindings, setBindings] = useState({});

  // Modal/Edit states
  const [editingCell, setEditingCell] = useState(null);
  const [editingTable, setEditingTable] = useState(null);
  const [editingQuery, setEditingQuery] = useState(null);
  const [editingSchedule, setEditingSchedule] = useState(null);
  const [editingSystem, setEditingSystem] = useState(null);
  const [uploadCSVType, setUploadCSVType] = useState(null);

  const refreshAll = () => { 
    tbl.refresh(); 
    qs.refresh(); 
    sc.refresh(); 
    sys.refresh();
    fetchBindings();
  };
  
  // Fetch all bindings
  const fetchBindings = async () => {
    try {
      // Fetch bindings for datasets
      const datasetBindings = await Promise.all(
        tbl.data.map(async (row) => {
          try {
            const binds = await fetch(`/api/bindings/dataset/${row.id}`).then(r => r.json());
            return { entityType: 'dataset', entityId: row.id, bindings: binds };
          } catch {
            return { entityType: 'dataset', entityId: row.id, bindings: [] };
          }
        })
      );
      
      // Fetch bindings for queries
      const queryBindings = await Promise.all(
        qs.data.map(async (row) => {
          try {
            const binds = await fetch(`/api/bindings/compare_query/${row.id}`).then(r => r.json());
            return { entityType: 'compare_query', entityId: row.id, bindings: binds };
          } catch {
            return { entityType: 'compare_query', entityId: row.id, bindings: [] };
          }
        })
      );
      
      // Organize bindings by entity
      const bindingsMap = {};
      [...datasetBindings, ...queryBindings].forEach(({ entityType, entityId, bindings }) => {
        const key = `${entityType}_${entityId}`;
        bindingsMap[key] = bindings;
      });
      
      setBindings(bindingsMap);
    } catch (err) {
      console.error('Error fetching bindings:', err);
    }
  };
  
  // Fetch bindings when data loads
  useEffect(() => {
    if (tbl.data.length > 0 || qs.data.length > 0) {
      fetchBindings();
    }
  }, [tbl.data.length, qs.data.length]);

  // Inline cell editing handler
  const handleCellEdit = async (type, row, field, newValue) => {
    if (newValue === row[field]) { setEditingCell(null); return; }
    try {
      const endpoint = `/api/${type}/${row.id}`;
      const body = { [field]: newValue, version: row.version, updated_by: DEFAULT_USER };
      await apiCall("PUT", endpoint, body);
      refreshAll();
      setEditingCell(null);
    } catch (err) {
      if (err.message.includes("409") || err.message.includes("version_conflict")) {
        setConflict({
          row, 
          onRefresh: () => { refreshAll(); setConflict(null); setEditingCell(null); }, 
          onCancel: () => setConflict(null)
        });
      } else {
        alert(`Error: ${err.message}`);
      }
    }
  };

  // Schedule save handler
  const handleScheduleSave = async (form) => {
    try {
      if (editingSchedule?.id) {
        await apiCall("PUT", `/api/schedules/${editingSchedule.id}`, form);
      } else {
        await apiCall("POST", `/api/schedules`, form);
      }
      refreshAll();
      setEditingSchedule(null);
    } catch (err) {
      if (err.message.includes("409") || err.message.includes("version_conflict")) {
        setConflict({
          row: editingSchedule, 
          onRefresh: () => { refreshAll(); setConflict(null); setEditingSchedule(null); }, 
          onCancel: () => setConflict(null)
        });
      } else {
        alert(`Error: ${err.message}`);
      }
    }
  };

  // System save handler
  const handleSystemSave = async (form) => {
    try {
      if (editingSystem?.id) {
        await apiCall("PUT", `/api/systems/${editingSystem.id}`, form);
      } else {
        await apiCall("POST", `/api/systems`, form);
      }
      refreshAll();
      setEditingSystem(null);
    } catch (err) {
      if (err.message.includes("409") || err.message.includes("version_conflict")) {
        setConflict({
          row: editingSystem, 
          onRefresh: () => { refreshAll(); setConflict(null); setEditingSystem(null); }, 
          onCancel: () => setConflict(null)
        });
      } else {
        alert(`Error: ${err.message}`);
      }
    }
  };

  // Table save handler
  const handleTableSave = async (form, selectedSchedules) => {
    try {
      let tableId;
      if (editingTable?.id) {
        await apiCall("PUT", `/api/tables/${editingTable.id}`, form);
        tableId = editingTable.id;
      } else {
        const result = await apiCall("POST", `/api/tables`, form);
        tableId = result.id;
      }
      
      // Sync schedule bindings
      if (tableId && selectedSchedules) {
        // Get current bindings
        const currentBindings = await fetch(`/api/bindings/dataset/${tableId}`).then(r => r.json()).catch(() => []);
        const currentScheduleIds = currentBindings.map(b => b.schedule_id);
        
        // Remove bindings that are no longer selected
        for (const binding of currentBindings) {
          if (!selectedSchedules.includes(binding.schedule_id)) {
            await apiCall("DELETE", `/api/bindings/${binding.id}`);
          }
        }
        
        // Add new bindings
        for (const scheduleId of selectedSchedules) {
          if (!currentScheduleIds.includes(scheduleId)) {
            await apiCall("POST", `/api/bindings`, {
              schedule_id: scheduleId,
              entity_type: 'dataset',
              entity_id: tableId
            });
          }
        }
      }
      
      refreshAll();
      setEditingTable(null);
    } catch (err) {
      if (err.message.includes("409") || err.message.includes("version_conflict")) {
        setConflict({
          row: editingTable, 
          onRefresh: () => { refreshAll(); setConflict(null); setEditingTable(null); }, 
          onCancel: () => setConflict(null)
        });
      } else {
        alert(`Error: ${err.message}`);
      }
    }
  };

  // Query save handler
  const handleQuerySave = async (form, selectedSchedules) => {
    try {
      let queryId;
      if (editingQuery.id) {
        await apiCall("PUT", `/api/queries/${editingQuery.id}`, form);
        queryId = editingQuery.id;
      } else {
        const result = await apiCall("POST", `/api/queries`, form);
        queryId = result.id;
      }
      
      // Sync schedule bindings
      if (queryId && selectedSchedules) {
        // Get current bindings
        const currentBindings = await fetch(`/api/bindings/compare_query/${queryId}`).then(r => r.json()).catch(() => []);
        const currentScheduleIds = currentBindings.map(b => b.schedule_id);
        
        // Remove bindings that are no longer selected
        for (const binding of currentBindings) {
          if (!selectedSchedules.includes(binding.schedule_id)) {
            await apiCall("DELETE", `/api/bindings/${binding.id}`);
          }
        }
        
        // Add new bindings
        for (const scheduleId of selectedSchedules) {
          if (!currentScheduleIds.includes(scheduleId)) {
            await apiCall("POST", `/api/bindings`, {
              schedule_id: scheduleId,
              entity_type: 'compare_query',
              entity_id: queryId
            });
          }
        }
      }
      
      refreshAll();
      setEditingQuery(null);
    } catch (err) {
      if (err.message.includes("409") || err.message.includes("version_conflict")) {
        setConflict({
          row: editingQuery, 
          onRefresh: () => { refreshAll(); setConflict(null); setEditingQuery(null); }, 
          onCancel: () => setConflict(null)
        });
      } else {
        alert(`Error: ${err.message}`);
      }
    }
  };

  // Delete handler
  const handleDelete = async (type, id) => {
    if (!confirm("Delete this record?")) return;
    try {
      await apiCall("DELETE", `/api/${type}/${id}`);
      refreshAll();
    } catch (err) {
      alert(`Error: ${err.message}`);
    }
  };


  // Trigger now
  const triggerNow = async (entity_type, entity_id) => {
    try {
      await apiCall("POST", `/api/triggers`, { entity_type, entity_id, requested_by: DEFAULT_USER });
      alert("Triggered successfully!");
    } catch (err) {
      alert(`Error: ${err.message}`);
    }
  };

  // Render cell with inline editing
  const renderCell = (type, row, field, options = null) => {
    const isEditing = editingCell?.type === type && editingCell?.rowId === row.id && editingCell?.field === field;
    const value = row[field];
    
    if (isEditing) {
      return (
        <InlineEditCell
          value={value}
          type={options ? "select" : (typeof value === "number" ? "number" : "text")}
          options={options || []}
          onSave={(newVal) => handleCellEdit(type, row, field, newVal)}
          onCancel={() => setEditingCell(null)}
        />
      );
    }
    
    const displayValue = options ? options.find(o => o.id === value)?.name || value : value;
    return (
      <span 
        onClick={() => setEditingCell({ type, rowId: row.id, field })} 
        className="cursor-pointer block p-1 rounded hover:bg-charcoal-300 transition-colors"
      >
        {displayValue}
      </span>
    );
  };

  return (
    <div className="flex h-screen font-sans">
      <Sidebar view={view} setView={setView} />
      <div className="ml-48 flex-1 p-10 overflow-y-auto">
        <h1 className="mt-0 text-3xl font-bold text-gray-100">LiveValidator Control Panel</h1>

        {/* Database Setup Required Banner */}
        {setupRequired && (
          <div className="my-4 p-4 bg-rust border-l-4 border-rust-light rounded-md shadow-lg">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-lg font-semibold text-gray-100 mb-1">⚠️ Database Not Initialized</h3>
                <p className="text-gray-200">
                  The database tables have not been created yet. Please go to the Setup tab and click "Initialize Database".
                </p>
              </div>
              <button 
                onClick={() => setView('setup')} 
                className="ml-4 px-4 py-2 bg-purple-600 text-gray-100 font-semibold border-0 rounded-md cursor-pointer hover:bg-purple-500 whitespace-nowrap"
              >
                Go to Setup →
              </button>
            </div>
          </div>
        )}

        {/* Modals */}
        {conflict && <VersionConflictDialog current={conflict.row} onRefresh={conflict.onRefresh} onCancel={conflict.onCancel} />}
        {editingTable && <TableModal table={editingTable} systems={sys.data} schedules={sc.data} onSave={handleTableSave} onClose={() => setEditingTable(null)} />}
        {editingQuery && <QueryModal query={editingQuery} systems={sys.data} schedules={sc.data} onSave={handleQuerySave} onClose={() => setEditingQuery(null)} />}
        {editingSchedule && <ScheduleModal schedule={editingSchedule} onSave={handleScheduleSave} onClose={() => setEditingSchedule(null)} />}
        {editingSystem && <SystemModal system={editingSystem} onSave={handleSystemSave} onClose={() => setEditingSystem(null)} />}
        {uploadCSVType && <UploadCSVModal type={uploadCSVType} systems={sys.data} schedules={sc.data} onClose={() => setUploadCSVType(null)} onUpload={refreshAll} />}

        {/* Tables View */}
        {view === 'tables' && (
          <>
            {tbl.error && tbl.error.action !== "setup_required" && <ErrorBox message={tbl.error.message} onClose={tbl.clearError} />}
            <h2 className="text-2xl font-semibold text-rust-light mb-4">Tables</h2>
            <div className="mb-3 flex gap-2">
              <button onClick={() => setEditingTable({})} className="px-3 py-2 bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500">Add Table</button>
              <button onClick={() => setUploadCSVType('tables')} className="px-3 py-2 bg-rust text-gray-100 border-0 rounded-md cursor-pointer hover:bg-rust-light">📂 Upload CSV</button>
        </div>
            {tbl.loading ? <p className="text-gray-400">Loading…</p> : (
              <div className="overflow-x-auto">
                <table className="w-full border-collapse">
                  <thead>
                    <tr className="border-b-2 border-charcoal-200">
                      <th className="text-left p-2 text-gray-200 font-medium">Name</th>
                      <th className="text-left p-2 text-gray-200 font-medium">Source Table</th>
                      <th className="text-left p-2 text-gray-200 font-medium">Target Table</th>
                      <th className="text-left p-2 text-gray-200 font-medium">Source</th>
                      <th className="text-left p-2 text-gray-200 font-medium">Target</th>
                      <th className="text-left p-2 text-gray-400 font-medium">Compare Mode</th>
                      <th className="text-left p-2 text-gray-400 font-medium">Watermark</th>
                      <th className="text-left p-2 text-gray-400 font-medium">PK Columns</th>
                      <th className="text-left p-2 text-gray-400 font-medium">Include</th>
                      <th className="text-left p-2 text-gray-400 font-medium">Exclude</th>
                    <th className="text-left p-2 text-gray-400 font-medium">Schedules</th>
                    <th className="text-left p-2 text-gray-200 font-medium">Actions</th>
                  </tr>
                </thead>
            <tbody>
                  {tbl.data.map(row => {
                    const entityBindings = bindings[`dataset_${row.id}`] || [];
                    const scheduleNames = entityBindings.map(b => sc.data.find(s => s.id === b.schedule_id)?.name).filter(Boolean).join(', ');
                    return (
                    <tr key={row.id} className="border-b border-charcoal-200">
                      <td className="p-2 text-gray-100">{row.name}</td>
                      <td className="p-2 text-gray-500 text-sm">{row.src_schema}.{row.src_table}</td>
                      <td className="p-2 text-gray-500 text-sm">{row.tgt_schema}.{row.tgt_table}</td>
                      <td className="p-2 text-gray-100">{renderCell('tables', row, 'src_system_id', sys.data)}</td>
                      <td className="p-2 text-gray-100">{renderCell('tables', row, 'tgt_system_id', sys.data)}</td>
                      <td className="p-2 text-gray-300 text-sm">{row.compare_mode}</td>
                      <td className="p-2 text-gray-300 text-sm">{row.watermark_column || '-'}</td>
                      <td className="p-2 text-gray-300 text-xs">{row.pk_columns?.join(', ') || '-'}</td>
                      <td className="p-2 text-gray-300 text-xs">{row.include_columns?.join(', ') || '-'}</td>
                      <td className="p-2 text-gray-300 text-xs">{row.exclude_columns?.join(', ') || '-'}</td>
                      <td className="p-2 text-purple-400 text-xs">{scheduleNames || '-'}</td>
                        <td className="p-2">
                          <button onClick={() => setEditingTable(row)} className="px-2 py-1 text-xs bg-purple-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-purple-500 mr-1">Edit</button>
                          <button onClick={() => handleDelete('tables', row.id)} className="px-2 py-1 text-xs bg-red-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-red-500 mr-1">Del</button>
                          <button onClick={() => triggerNow('dataset', row.id)} className="px-2 py-1 text-xs bg-green-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-green-500">▶️</button>
                  </td>
                </tr>
                    );
                  })}
            </tbody>
          </table>
              </div>
            )}
          </>
        )}

        {/* Queries View */}
        {view === 'queries' && (
          <>
            {qs.error && qs.error.action !== "setup_required" && <ErrorBox message={qs.error.message} onClose={qs.clearError} />}
            <h2 className="text-2xl font-semibold text-rust-light mb-4">Compare Queries</h2>
            <div className="mb-3 flex gap-2">
              <button onClick={() => setEditingQuery({})} className="px-3 py-2 bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500">Add Query</button>
              <button onClick={() => setUploadCSVType('queries')} className="px-3 py-2 bg-rust text-gray-100 border-0 rounded-md cursor-pointer hover:bg-rust-light">📂 Upload CSV</button>
        </div>
            {qs.loading ? <p className="text-gray-400">Loading…</p> : (
              <div className="overflow-x-auto">
                <table className="w-full border-collapse">
                  <thead>
                    <tr className="border-b-2 border-charcoal-200">
                      <th className="text-left p-2 text-gray-400 font-medium">Name</th>
                      <th className="text-left p-2 text-gray-400 font-medium">Source</th>
                      <th className="text-left p-2 text-gray-400 font-medium">Target</th>
                      <th className="text-left p-2 text-gray-400 font-medium">SQL</th>
                      <th className="text-left p-2 text-gray-400 font-medium">Compare Mode</th>
                      <th className="text-left p-2 text-gray-400 font-medium">PK Columns</th>
                      <th className="text-left p-2 text-gray-400 font-medium">Include</th>
                      <th className="text-left p-2 text-gray-400 font-medium">Exclude</th>
                    <th className="text-left p-2 text-gray-400 font-medium">Schedules</th>
                    <th className="text-left p-2 text-gray-400 font-medium">Actions</th>
                  </tr>
                </thead>
            <tbody>
                  {qs.data.map(row => {
                    const entityBindings = bindings[`compare_query_${row.id}`] || [];
                    const scheduleNames = entityBindings.map(b => sc.data.find(s => s.id === b.schedule_id)?.name).filter(Boolean).join(', ');
                    return (
                    <tr key={row.id} className="border-b border-charcoal-200">
                      <td className="p-2 text-gray-100">{row.name}</td>
                      <td className="p-2 text-gray-200">{renderCell('queries', row, 'src_system_id', sys.data)}</td>
                      <td className="p-2 text-gray-200">{renderCell('queries', row, 'tgt_system_id', sys.data)}</td>
                      <td className="p-2 text-gray-400 text-xs max-w-xs truncate font-mono">{row.src_sql || row.sql}</td>
                      <td className="p-2 text-gray-300 text-sm">{row.compare_mode}</td>
                      <td className="p-2 text-gray-300 text-xs">{row.pk_columns?.join(', ') || '-'}</td>
                      <td className="p-2 text-purple-400 text-xs">{scheduleNames || '-'}</td>
                        <td className="p-2">
                          <button onClick={() => setEditingQuery(row)} className="px-2 py-1 text-xs bg-purple-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-purple-500 mr-1">Edit</button>
                          <button onClick={() => handleDelete('queries', row.id)} className="px-2 py-1 text-xs bg-red-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-red-500 mr-1">Del</button>
                          <button onClick={() => triggerNow('compare_query', row.id)} className="px-2 py-1 text-xs bg-green-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-green-500">▶️</button>
                        </td>
                      </tr>
                    );
                  })}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}

        {/* Schedules View */}
        {view === 'schedules' && (
          <>
            {sc.error && sc.error.action !== "setup_required" && <ErrorBox message={sc.error.message} onClose={sc.clearError} />}
            <h2 className="text-2xl font-semibold text-rust-light mb-4">Schedules</h2>
            <button onClick={() => setEditingSchedule({})} className="mb-3 px-3 py-2 bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500">Add Schedule</button>
            {sc.loading ? <p className="text-gray-400">Loading…</p> : (
              <table className="w-full border-collapse">
                <thead>
                  <tr className="border-b-2 border-charcoal-200">
                    <th className="text-left p-2 text-gray-400 font-medium">Name</th>
                    <th className="text-left p-2 text-gray-400 font-medium">
                      <a href="https://crontab.cronhub.io/" target="_blank" rel="noopener noreferrer" className="text-rust-light hover:text-rust underline">Cron</a>
                    </th>
                    <th className="text-left p-2 text-gray-400 font-medium">Timezone</th>
                    <th className="text-left p-2 text-gray-400 font-medium">Enabled</th>
                    <th className="text-left p-2 text-gray-400 font-medium">Max Concurrency</th>
                    <th className="text-left p-2 text-gray-400 font-medium">Backfill</th>
                    <th className="text-left p-2 text-gray-400 font-medium">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {sc.data.map(row => (
                    <tr key={row.id} className="border-b border-charcoal-200">
                      <td className="p-2 text-gray-100">{row.name}</td>
                      <td className="p-2 text-gray-200 font-mono text-sm">{renderCell('schedules', row, 'cron_expr')}</td>
                      <td className="p-2 text-gray-200">{renderCell('schedules', row, 'timezone')}</td>
                      <td className="p-2 text-gray-300">{row.enabled ? "✅" : "❌"}</td>
                      <td className="p-2 text-gray-200">{renderCell('schedules', row, 'max_concurrency')}</td>
                      <td className="p-2 text-gray-300">{row.backfill_policy}</td>
                      <td className="p-2">
                        <button onClick={() => setEditingSchedule(row)} className="px-2 py-1 text-xs bg-purple-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-purple-500 mr-1">Edit</button>
                        <button onClick={() => handleDelete('schedules', row.id)} className="px-2 py-1 text-xs bg-red-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-red-500">Del</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
          </>
        )}

        {/* Systems View */}
        {view === 'systems' && (
          <>
            {sys.error && sys.error.action !== "setup_required" && <ErrorBox message={sys.error.message} onClose={sys.clearError} />}
            <h2 className="text-2xl font-semibold text-rust-light mb-4">Systems</h2>
            <button onClick={() => setEditingSystem({})} className="mb-3 px-3 py-2 bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500">Add System</button>
            {sys.loading ? <p className="text-gray-400">Loading…</p> : (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {sys.data.map(row => {
                  const isDatabricks = row.kind === 'Databricks';
                  const showDatabase = ['Postgres', 'SQLServer', 'MySQL', 'Netezza'].includes(row.kind);
                  return (
                    <div key={row.id} className="bg-charcoal-500 border border-charcoal-200 rounded-lg p-4">
                      <h3 className="text-lg font-semibold text-rust-light mb-2">{row.name}</h3>
                      <p className="text-gray-400 text-sm mb-1"><strong>Type:</strong> {row.kind}</p>
                      
                      {isDatabricks ? (
                        /* Databricks: Only show catalog */
                        <p className="text-gray-400 text-sm mb-3"><strong>Catalog:</strong> {row.catalog || <span className="text-gray-600">-</span>}</p>
                      ) : (
                        /* Other systems: Show connection details */
                        <>
                          {row.host && <p className="text-gray-400 text-sm mb-1"><strong>Host:</strong> {row.host}</p>}
                          {row.port && <p className="text-gray-400 text-sm mb-1"><strong>Port:</strong> {row.port}</p>}
                          {showDatabase && row.database && <p className="text-gray-400 text-sm mb-1"><strong>Database:</strong> {row.database}</p>}
                          {row.user_secret_key && <p className="text-gray-400 text-sm mb-1"><strong>User Secret:</strong> {row.user_secret_key}</p>}
                          {row.jdbc_string && <p className="text-gray-400 text-sm mb-1 font-mono text-xs break-all"><strong>JDBC:</strong> {row.jdbc_string.substring(0, 50)}...</p>}
                        </>
                      )}
                      
                      <div className="flex gap-2 mt-3">
                        <button onClick={() => setEditingSystem(row)} className="px-2 py-1 text-xs bg-purple-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-purple-500">Edit</button>
                        <button onClick={() => handleDelete('systems', row.id)} className="px-2 py-1 text-xs bg-red-600 text-gray-100 border-0 rounded cursor-pointer hover:bg-red-500">Del</button>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </>
        )}

        {view === 'setup' && (
          <>
            <h2 className="text-2xl font-semibold text-rust-light mb-4">Database Setup</h2>
            
            <div className="max-w-2xl">
              {/* Initial Setup Section */}
              <div className="bg-purple-900/30 border-2 border-purple-600 rounded-lg p-6 mb-6">
                <h3 className="text-purple-400 font-bold text-xl mb-4">🚀 Initial Setup</h3>
                <p className="text-gray-300 mb-6">
                  Follow these steps to set up a <strong className="text-purple-400">fresh deployment</strong>. 
                  Each step only needs to be completed once per database instance.
                </p>

                {/* Step 1 */}
                <div className="mb-5 pb-5 border-b border-gray-700">
                  <h4 className="text-purple-300 font-semibold text-lg mb-3">
                    <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">1</span>
                    Run SQL Commands as Database Creator
                  </h4>
                  <p className="text-gray-300 text-sm mb-3 ml-9">
                    The <strong>creator</strong> of the Lakebase PostgreSQL instance must run the following commands in the <strong>Databricks SQL Editor</strong>:
                  </p>
                  <pre className="bg-charcoal-800 p-3 rounded text-xs text-gray-200 overflow-x-auto border border-gray-700 ml-9">
{`CREATE USER apprunner WITH PASSWORD 'beepboop123';

CREATE SCHEMA IF NOT EXISTS control;
GRANT USAGE ON SCHEMA control to apprunner;
GRANT apprunner TO CURRENT_USER;
ALTER SCHEMA control OWNER TO apprunner;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA control TO apprunner;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA control TO apprunner;`}
                  </pre>
                </div>

                {/* Step 2 */}
                <div className="mb-5 pb-5 border-b border-gray-700">
                  <h4 className="text-purple-300 font-semibold text-lg mb-3">
                    <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">2</span>
                    Enable Postgres Native Role Login
                  </h4>
                  <p className="text-gray-300 text-sm mb-2 ml-9">
                    In the Databricks UI, enable native Postgres role login for your database instance.
                  </p>
                  <p className="text-gray-400 text-sm ml-9">
                    📖 <a 
                      href="https://docs.databricks.com/aws/en/oltp/instances/authentication#authenticate-with-postgres-roles-and-passwords" 
                      target="_blank" 
                      rel="noopener noreferrer"
                      className="text-purple-400 hover:text-purple-300 underline"
                    >
                      View Databricks documentation for detailed instructions →
                    </a>
                  </p>
                </div>

                {/* Step 3 */}
                <div className="mb-5 pb-5 border-b border-gray-700">
                  <h4 className="text-purple-300 font-semibold text-lg mb-3">
                    <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">3</span>
                    Initialize the Database
                  </h4>
                  <p className="text-gray-300 text-sm mb-3 ml-9">
                    Click the button below to create the database schema and tables.
                  </p>
                  <ul className="text-gray-400 text-sm mb-4 ml-9 list-disc list-inside space-y-1">
                    <li>Creates all tables from <code className="bg-charcoal-600 px-1 rounded">backend/sql/ddl.sql</code></li>
                    <li><strong className="text-green-400">Safe:</strong> Uses <code className="bg-charcoal-600 px-1 rounded">IF NOT EXISTS</code> - won't destroy existing data</li>
                  </ul>
                  <button 
                    onClick={async () => {
                      try {
                        await apiCall('POST', '/api/setup/initialize-database', {});
                        alert('✅ Database initialized successfully!\n\nSchema and tables have been created.');
                        // Refresh all data
                        tbl.refresh();
                        qs.refresh();
                        sc.refresh();
                        sys.refresh();
                      } catch (err) {
                        alert(`❌ Database initialization failed:\n\n${err.message}`);
                      }
                    }}
                    className="ml-9 px-6 py-3 bg-purple-600 text-white font-bold border-0 rounded-lg cursor-pointer hover:bg-purple-500 text-lg shadow-lg"
                  >
                    ✨ Initialize Database
                  </button>
                </div>

                {/* Step 4 */}
                <div className="mb-5 pb-5 border-b border-gray-700">
                  <h4 className="text-purple-300 font-semibold text-lg mb-3">
                    <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">4</span>
                    Configure Source and Target Systems
                  </h4>
                  <p className="text-gray-300 text-sm mb-2 ml-9">
                    Go to the <strong>Systems</strong> tab and add your source and target systems with their connection details.
                  </p>
                  <p className="text-gray-400 text-sm ml-9">
                    💡 <em>You'll need at least one source and one target system before creating tables or queries.</em>
                  </p>
                </div>

                {/* Step 5 */}
                <div className="mb-5 pb-5 border-b border-gray-700">
                  <h4 className="text-purple-300 font-semibold text-lg mb-3">
                    <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">5</span>
                    Create Schedules
                  </h4>
                  <p className="text-gray-300 text-sm mb-2 ml-9">
                    Go to the <strong>Schedules</strong> tab and add schedules with cron expressions for automated validation runs.
                  </p>
                  <p className="text-gray-400 text-sm ml-9">
                    💡 <em>Example: "0 2 * * *" runs daily at 2 AM UTC.</em>
                  </p>
                </div>

                {/* Step 6 */}
                <div className="mb-4">
                  <h4 className="text-purple-300 font-semibold text-lg mb-3">
                    <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">6</span>
                    Add Tables and Queries
                  </h4>
                  <p className="text-gray-300 text-sm mb-2 ml-9">
                    Go to the <strong>Tables</strong> or <strong>Queries</strong> tabs to define your validation rules and bind them to schedules.
                  </p>
                  <ul className="text-gray-400 text-sm ml-9 list-disc list-inside space-y-1">
                    <li><strong>Tables:</strong> Schema-driven comparisons between source and target tables</li>
                    <li><strong>Queries:</strong> Custom SQL comparisons for complex validation logic</li>
                    <li><strong>Schedule Binding:</strong> Select which schedules should trigger each validation</li>
                  </ul>
                </div>
        </div>

              {/* Danger Zone Section */}
              <div className="bg-red-900/30 border-2 border-red-600 rounded-lg p-6 mb-6">
                <h3 className="text-red-400 font-bold text-xl mb-3">⚠️ Danger Zone</h3>
                <p className="text-gray-300 mb-4">
                  This will <strong className="text-red-400">permanently delete ALL data</strong> in the control schema and recreate the tables from scratch using <code className="bg-charcoal-600 px-2 py-1 rounded">ddl.sql</code>.
                </p>
                <ul className="text-gray-400 text-sm mb-4 list-disc list-inside space-y-1">
                  <li>All Tables, Queries, Schedules, Systems, and Bindings will be deleted</li>
                  <li>This cannot be undone</li>
                  <li>Database will be recreated from <code className="bg-charcoal-600 px-1 rounded">backend/sql/ddl.sql</code></li>
                  <li>Grants will be re-run from <code className="bg-charcoal-600 px-1 rounded">backend/sql/grants.sql</code></li>
        </ul>
                <p className="text-gray-500 text-xs mb-4">
                  Use this for initial setup or after updating the database schema in code.
                </p>
              </div>

              <button 
                onClick={async () => {
                  if (!confirm('⚠️ Are you absolutely sure?\n\nThis will DELETE ALL DATA and cannot be undone.\n\nType "DELETE" in the next prompt to confirm.')) {
                    return;
                  }
                  const confirmation = prompt('Type "DELETE" (all caps) to confirm:');
                  if (confirmation !== 'DELETE') {
                    alert('Reset cancelled.');
                    return;
                  }
                  
                  try {
                    await apiCall('POST', '/api/setup/reset-database', {});
                    alert('✅ Database reset successfully!\n\nAll data has been cleared and tables recreated.');
                    // Refresh all data
                    tbl.refresh();
                    qs.refresh();
                    sc.refresh();
                    sys.refresh();
                  } catch (err) {
                    alert(`❌ Database reset failed:\n\n${err.message}`);
                  }
                }}
                className="px-6 py-3 bg-red-600 text-white font-bold border-0 rounded-lg cursor-pointer hover:bg-red-500 text-lg shadow-lg"
              >
                🗑️ Reset Database (Delete All Data)
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
