import React, { useState, useEffect, useRef, createContext, useContext } from 'react';
import { flushSync } from 'react-dom';

// Hooks
import { useFetch } from './hooks/useFetch';

// Create context for current user
const CurrentUserContext = createContext(null);
export const useCurrentUser = () => useContext(CurrentUserContext);

// Permission check helpers
export const canCreate = (role) => {
  if (!role) return false;
  return ['CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE'].includes(role);
};

export const canRun = (role) => {
  if (!role) return false;
  return ['CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE'].includes(role);
};

export const canEdit = (role, createdBy, currentUserEmail) => {
  if (!role) return false;
  if (role === 'CAN_VIEW') return false;
  if (['CAN_EDIT', 'CAN_MANAGE'].includes(role)) return true;
  // CAN_RUN can only edit their own creations
  return role === 'CAN_RUN' && createdBy === currentUserEmail;
};

export const canManageSystems = (role) => {
  return role === 'CAN_MANAGE';
};

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

// Views
import {
  ValidationResultsView,
  DashboardView,
  DashboardDirectoryView,
  TablesView,
  QueriesView,
  QueueView,
  SchedulesView,
  SystemsView,
  SetupView,
  ConfigurationView,
  TypeMappingsView,
  AdminView
} from './views';

// Services
import { apiCall } from './services/api';

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
  const [view, setView] = useState('results');
  const [conflict, setConflict] = useState(null);
  const [notification, setNotification] = useState(null); // { type: 'success' | 'error', message: string }
  const showNotification = (message, type = 'success', details = null) => {
    setNotification({ message, type, details });
    if (type !== 'error') setTimeout(() => setNotification(null), 5000);
  };
  const [highlightId, setHighlightId] = useState(null); // For highlighting specific validation run
  const [highlightEntityId, setHighlightEntityId] = useState(null); // For highlighting specific entity in tables/queries
  const [currentUser, setCurrentUser] = useState(null); // { email, role }
  const [selectedDashboardId, setSelectedDashboardId] = useState(null);
  
  // Fetch current user
  useEffect(() => {
    fetch('/api/current_user')
      .then(r => r.json())
      .then(setCurrentUser)
      .catch(err => console.error('Failed to fetch current user:', err));
  }, []);
  
  // Data fetching
  const tbl = useFetch(`/api/tables`, []);
  const qs = useFetch(`/api/queries`, []);
  const sc = useFetch(`/api/schedules`, []);
  const sys = useFetch(`/api/systems`, []);
  const triggers = useFetch(`/api/triggers`, []);
  const queueStats = useFetch(`/api/queue-status`, {});
  const dashboards = useFetch(`/api/dashboards`, []);
  
  useEffect(() => {
    if (view === 'queue') {
      triggers.refresh();
      queueStats.refresh();
      const interval = setInterval(() => {
        triggers.refresh();
        queueStats.refresh();
      }, 5000);
      return () => clearInterval(interval);
    }
  }, [view]);
  
  // Check if database needs initialization
  const setupRequired = [tbl.error, qs.error, sc.error, sys.error, dashboards.error].some(
    err => err?.action === "setup_required"
  );
  
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
  };

  // Inline cell editing handler
  const handleCellEdit = async (type, row, field, newValue) => {
    if (newValue === row[field]) { setEditingCell(null); return; }
    try {
      const endpoint = `/api/${type}/${row.id}`;
      const body = { [field]: newValue, version: row.version };
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
  const handleTableSave = async (form, selectedSchedules, tags) => {
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
        const currentBindings = await fetch(`/api/bindings/table/${tableId}`).then(r => r.json()).catch(() => []);
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
              entity_type: 'table',
              entity_id: tableId
            });
          }
        }
      }
      
      // Sync tags
      if (tableId && tags !== undefined) {
        await apiCall("POST", `/api/tags/entity/table/${tableId}`, { tags });
      }
      
      refreshAll();
      setEditingTable(null);
    } catch (err) {
      if (err.message.includes("version_conflict")) {
        setConflict({
          row: editingTable, 
          onRefresh: () => { refreshAll(); setConflict(null); setEditingTable(null); }, 
          onCancel: () => setConflict(null)
        });
      } else {
        // Re-throw so modal can display validation errors nicely
        throw err;
      }
    }
  };

  // Query save handler
  const handleQuerySave = async (form, selectedSchedules, tags) => {
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
      
      // Sync tags
      if (queryId && tags !== undefined) {
        await apiCall("POST", `/api/tags/entity/query/${queryId}`, { tags });
      }
      
      refreshAll();
      setEditingQuery(null);
    } catch (err) {
      if (err.message.includes("version_conflict")) {
        setConflict({
          row: editingQuery, 
          onRefresh: () => { refreshAll(); setConflict(null); setEditingQuery(null); }, 
          onCancel: () => setConflict(null)
        });
      } else {
        // Re-throw so modal can display validation errors nicely
        throw err;
      }
    }
  };

  // Delete handler
  const handleDelete = async (type, id, skipConfirm = false) => {
    if (!skipConfirm && !confirm("Delete this record?")) return;
    try {
      await apiCall("DELETE", `/api/${type}/${id}`);
      refreshAll();
    } catch (err) {
      alert(`Error: ${err.message}`);
    }
  };


  // Trigger now (single)
  const triggerNow = async (entity_type, entity_id) => {
    try {
      const result = await apiCall("POST", `/api/triggers`, { entity_type, entity_id });
      if (result.databricks_run_id) {
        showNotification('Validation launched', 'success');
      } else if (result.queued_reason) {
        showNotification(`Queued: ${result.queued_reason}`, 'warning', 'Will launch when capacity is available.');
      } else {
        showNotification('Validation queued', 'success');
      }
      triggers.refresh();
      queueStats.refresh();
    } catch (err) {
      showNotification('Failed to trigger validation', 'error', err.message);
    }
  };

  // Bulk trigger - creates all triggers immediately, then launches jobs silently
  const bulkTriggerNow = async (entity_type, entity_ids) => {
    try {
      // Step 1: Bulk create triggers with status='running' (instant)
      const createResult = await apiCall("POST", "/api/triggers/bulk-create", { entity_type, entity_ids });
      const triggerIds = createResult.created;
      const skipped = createResult.skipped;
      
      if (triggerIds.length === 0) {
        showNotification(skipped > 0 ? `${skipped} already in queue` : 'No validations to trigger', 'warning');
        return;
      }
      
      // Show ONE toast and refresh
      showNotification(`Triggered ${triggerIds.length} validations`, 'success');
      triggers.refresh();
      queueStats.refresh();
      
      // Step 2: Launch jobs silently in background (no more toasts)
      for (const triggerId of triggerIds) {
        try {
          await apiCall("POST", `/api/triggers/${triggerId}/launch`);
        } catch { /* silent */ }
      }
      triggers.refresh();
    } catch (err) {
      showNotification(`Bulk trigger failed: ${err.message}`, 'error');
    }
  };

  // Navigate to validation result
  const navigateToResult = (validationId) => {
    setHighlightId(validationId);
    setView('results');
  };

  // Navigate to entity in tables or queries view
  const navigateToEntity = (entityType, entityId) => {
    setHighlightEntityId(entityId);
    setView(entityType === 'table' ? 'tables' : 'queries');
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
    <CurrentUserContext.Provider value={currentUser}>
      <div className="flex h-screen font-sans">
        <Sidebar view={view} setView={(v) => { setView(v); if (v === 'dashboard') setSelectedDashboardId(null); }} setupRequired={setupRequired} />
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

        {/* Notification Toast */}
        {notification && (
          <div className={`fixed top-4 right-4 z-50 max-w-lg rounded-lg shadow-2xl border-2 p-4 animate-slide-in ${
            notification.type === 'success' 
              ? 'bg-green-900/95 border-green-600 text-green-100' 
              : notification.type === 'warning'
              ? 'bg-yellow-900/95 border-yellow-600 text-yellow-100'
              : 'bg-red-900/95 border-red-600 text-red-100'
          }`}>
            <div className="flex items-start gap-3">
              <div className="flex-1">
                <p className="font-medium">{notification.message}</p>
                {notification.details && (
                  <details className="mt-2">
                    <summary className="text-sm opacity-80 cursor-pointer hover:opacity-100">View details</summary>
                    <pre className="mt-2 text-xs bg-black/30 rounded p-2 overflow-x-auto max-h-40 overflow-y-auto whitespace-pre-wrap break-words">
                      {notification.details}
                    </pre>
                  </details>
                )}
              </div>
              <button 
                onClick={() => setNotification(null)}
                className="text-gray-300 hover:text-white transition-colors flex-shrink-0"
              >
                ✕
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

        {/* Validation Results View (self-contained with server-side pagination) */}
        {view === 'results' && (
          <ValidationResultsView 
            highlightId={highlightId}
            onClearHighlight={() => setHighlightId(null)}
            onNavigateToEntity={navigateToEntity}
          />
        )}

        {/* Dashboard View (two-level routing) */}
        {view === 'dashboard' && !selectedDashboardId && (
          <DashboardDirectoryView
            dashboards={dashboards.data}
            loading={dashboards.loading}
            error={dashboards.error}
            onSelect={(id) => setSelectedDashboardId(id)}
            onRefresh={dashboards.refresh}
          />
        )}
        {view === 'dashboard' && selectedDashboardId && (
          <DashboardView
            dashboardId={selectedDashboardId}
            onNavigateToEntity={navigateToEntity}
            onBack={() => { setSelectedDashboardId(null); dashboards.refresh(); }}
          />
        )}

        {/* Tables View */}
        {view === 'tables' && (
          <TablesView 
            data={tbl.data}
            loading={tbl.loading}
            error={tbl.error}
            systems={sys.data}
            schedules={sc.data}
            onEdit={setEditingTable}
            onDelete={handleDelete}
            onTrigger={triggerNow}
            onBulkTrigger={(ids) => bulkTriggerNow('table', ids)}
            onUploadCSV={() => setUploadCSVType('tables')}
            onClearError={tbl.clearError}
            renderCell={renderCell}
            onNavigateToResult={navigateToResult}
            onRefresh={refreshAll}
            highlightEntityId={highlightEntityId}
            onClearEntityHighlight={() => setHighlightEntityId(null)}
          />
        )}

        {/* Queries View */}
        {view === 'queries' && (
          <QueriesView 
            data={qs.data}
            loading={qs.loading}
            error={qs.error}
            systems={sys.data}
            schedules={sc.data}
            onEdit={setEditingQuery}
            onDelete={handleDelete}
            onTrigger={triggerNow}
            onUploadCSV={() => setUploadCSVType('queries')}
            onClearError={qs.clearError}
            renderCell={renderCell}
            onNavigateToResult={navigateToResult}
            onRefresh={refreshAll}
            highlightEntityId={highlightEntityId}
            onClearEntityHighlight={() => setHighlightEntityId(null)}
          />
        )}

        {/* Queue View */}
        {view === 'queue' && (
          <QueueView 
            triggers={triggers}
            queueStats={queueStats}
            onRefresh={() => {
              triggers.refresh();
              queueStats.refresh();
            }}
            showNotification={showNotification}
          />
        )}

        {/* Configuration View */}
        {view === 'configuration' && (
          <ConfigurationView />
        )}

        {/* Type Mappings View */}
        {view === 'type-mappings' && (
          <TypeMappingsView />
        )}

        {/* Schedules View */}
        {view === 'schedules' && (
          <SchedulesView 
            data={sc.data}
            loading={sc.loading}
            error={sc.error}
            onEdit={setEditingSchedule}
            onDelete={handleDelete}
            onClearError={sc.clearError}
            renderCell={renderCell}
          />
        )}

        {/* Systems View */}
        {view === 'systems' && (
          <SystemsView 
            data={sys.data}
            loading={sys.loading}
            error={sys.error}
            onEdit={setEditingSystem}
            onDelete={handleDelete}
            onClearError={sys.clearError}
          />
        )}

        {/* Admin View */}
        {view === 'admin' && <AdminView />}

        {/* Setup View */}
        {view === 'setup' && <SetupView />}
      </div>
    </div>
    </CurrentUserContext.Provider>
  );
}
