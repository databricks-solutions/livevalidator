import React, { useState, useMemo, useRef, useEffect, useCallback } from 'react';
import { useCurrentUser } from '../App';
import { Checkbox } from '../components/Checkbox';
import { dashboardService, apiCall } from '../services/api';

const NameCell = ({ dashboard, isEditing, editingValue, editRef, onSelect, onStartEdit, onEditChange, onCommit, onCancel }) => {
  const clickTimeout = useRef(null);
  const handleClick = useCallback((e) => {
    e.stopPropagation();
    if (clickTimeout.current) {
      clearTimeout(clickTimeout.current);
      clickTimeout.current = null;
      onStartEdit(dashboard.id, dashboard.name);
    } else {
      clickTimeout.current = setTimeout(() => {
        clickTimeout.current = null;
        onSelect(dashboard.id);
      }, 250);
    }
  }, [dashboard.id, dashboard.name, onSelect, onStartEdit]);

  if (isEditing) {
    return (
      <td className="px-3 py-1.5 text-gray-200 font-medium text-sm truncate" onClick={(e) => e.stopPropagation()}>
        <input
          ref={editRef}
          type="text"
          value={editingValue}
          onChange={(e) => onEditChange(e.target.value)}
          onBlur={onCommit}
          onKeyDown={(e) => {
            if (e.key === 'Enter') onCommit();
            if (e.key === 'Escape') onCancel();
          }}
          className="w-full px-1.5 py-0.5 bg-charcoal-700 border border-purple-500 rounded text-gray-200 text-sm focus:outline-none"
        />
      </td>
    );
  }
  return (
    <td className="px-3 py-1.5 text-gray-200 font-medium text-sm truncate" onClick={handleClick} title="Click to open, double-click to rename">
      {dashboard.name}
    </td>
  );
};

export function DashboardDirectoryView({ dashboards, loading, error, onSelect, onRefresh }) {
  const currentUser = useCurrentUser();
  const [createError, setCreateError] = useState(null);
  const [collapsedSections, setCollapsedSections] = useState({});
  const [initLoading, setInitLoading] = useState(false);
  const [selectedIds, setSelectedIds] = useState(new Set());
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [filterText, setFilterText] = useState('');
  const [filterProject, setFilterProject] = useState('');
  const [editingId, setEditingId] = useState(null);
  const [editingValue, setEditingValue] = useState('');
  const [editingProject, setEditingProject] = useState(null);
  const [editingProjectValue, setEditingProjectValue] = useState('');
  const editRef = useRef(null);
  const editProjectRef = useRef(null);

  useEffect(() => {
    if (editingId && editRef.current) { editRef.current.focus(); editRef.current.select(); }
  }, [editingId]);

  useEffect(() => {
    if (editingProject && editProjectRef.current) { editProjectRef.current.focus(); editProjectRef.current.select(); }
  }, [editingProject]);

  const toggleSection = (key) => {
    setCollapsedSections(prev => ({ ...prev, [key]: !prev[key] }));
  };

  const allDashboardItems = useMemo(() => dashboards || [], [dashboards]);

  const allProjects = useMemo(() => {
    const projects = new Set();
    allDashboardItems.forEach(d => {
      if (d.project && d.project !== 'General') projects.add(d.project);
    });
    return ['General', ...Array.from(projects).sort()];
  }, [allDashboardItems]);

  const filteredItems = useMemo(() => {
    let items = allDashboardItems;
    if (filterText) {
      const lower = filterText.toLowerCase();
      items = items.filter(d => d.name.toLowerCase().includes(lower));
    }
    if (filterProject) {
      items = items.filter(d => d.project === filterProject);
    }
    return items;
  }, [allDashboardItems, filterText, filterProject]);

  const grouped = useMemo(() => {
    const myDashboards = [];
    const projectMap = {};

    filteredItems.forEach(d => {
      if (d.project === 'General' && d.created_by === currentUser?.email) {
        myDashboards.push(d);
      } else if (d.project !== 'General') {
        if (!projectMap[d.project]) projectMap[d.project] = [];
        projectMap[d.project].push(d);
      }
    });

    const projectSections = Object.entries(projectMap)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([name, items]) => ({ name, items }));

    return { myDashboards, projectSections };
  }, [filteredItems, currentUser]);

  const handleCreate = async () => {
    setCreateError(null);
    try {
      let name = 'Untitled Dashboard';
      const existing = new Set(allDashboardItems.map(d => d.name));
      if (existing.has(name)) {
        let i = 2;
        while (existing.has(`Untitled Dashboard ${i}`)) i++;
        name = `Untitled Dashboard ${i}`;
      }
      const dash = await dashboardService.create({ name });
      onRefresh();
      onSelect(dash.id);
    } catch (err) {
      setCreateError(err.message);
    }
  };

  const formatDate = (d) => {
    if (!d) return '-';
    return new Date(d).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  };

  const handleSelectAllInSection = (items, checked) => {
    const next = new Set(selectedIds);
    items.forEach(d => {
      if (checked) next.add(d.id);
      else next.delete(d.id);
    });
    setSelectedIds(next);
  };

  const handleSelectRow = (id, checked) => {
    const next = new Set(selectedIds);
    if (checked) next.add(id);
    else next.delete(id);
    setSelectedIds(next);
  };

  const handleBulkDelete = async () => {
    try {
      await Promise.all(Array.from(selectedIds).map(id => dashboardService.delete(id)));
      setSelectedIds(new Set());
      setShowDeleteConfirm(false);
      onRefresh();
    } catch (err) {
      alert(`Delete failed: ${err.message}`);
    }
  };

  const commitRename = async (id) => {
    const trimmed = editingValue.trim();
    const dash = allDashboardItems.find(d => d.id === id);
    if (trimmed && dash && trimmed !== dash.name) {
      try {
        await dashboardService.update(id, { name: trimmed, version: dash.version });
        onRefresh();
      } catch (err) { alert(`Rename failed: ${err.message}`); }
    }
    setEditingId(null);
  };

  const commitProjectRename = async (oldName) => {
    const trimmed = editingProjectValue.trim();
    if (trimmed && trimmed !== oldName) {
      try {
        const items = allDashboardItems.filter(d => d.project === oldName);
        await Promise.all(items.map(d => dashboardService.update(d.id, { project: trimmed, version: d.version })));
        onRefresh();
      } catch (err) { alert(`Project rename failed: ${err.message}`); }
    }
    setEditingProject(null);
  };

  const someSelected = selectedIds.size > 0;

  const DashboardTable = ({ items }) => {
    const allInSectionSelected = items.length > 0 && items.every(d => selectedIds.has(d.id));
    return (
      <table className="w-full table-fixed">
        <thead className="bg-charcoal-400 border-b border-charcoal-200">
          <tr>
            <th className="text-left px-2 py-1.5 text-sm text-gray-300 font-semibold w-10">
              <Checkbox
                checked={allInSectionSelected}
                onChange={(e) => handleSelectAllInSection(items, e.target.checked)}
              />
            </th>
            <th className="text-left px-3 py-1.5 text-sm text-gray-300 font-semibold w-[40%]">Name</th>
            <th className="text-left px-3 py-1.5 text-sm text-gray-300 font-semibold w-[10%]">Charts</th>
            <th className="text-left px-3 py-1.5 text-sm text-gray-300 font-semibold w-[28%]">Created By</th>
            <th className="text-left px-3 py-1.5 text-sm text-gray-300 font-semibold w-[17%]">Last Updated</th>
          </tr>
        </thead>
        <tbody>
          {items.length === 0 ? (
            <tr>
              <td colSpan={5} className="text-center py-6 text-gray-500 text-sm italic">
                No dashboards
              </td>
            </tr>
          ) : (
            items.map(d => {
              const isSelected = selectedIds.has(d.id);
              return (
                <tr
                  key={d.id}
                  onClick={() => onSelect(d.id)}
                  className={`border-b border-charcoal-300/20 hover:bg-charcoal-400/50 transition-colors cursor-pointer ${
                    isSelected ? 'bg-purple-900/20' : ''
                  }`}
                >
                  <td className="px-2 py-1.5 text-sm" onClick={(e) => e.stopPropagation()}>
                    <Checkbox
                      checked={isSelected}
                      onChange={(e) => handleSelectRow(d.id, e.target.checked)}
                    />
                  </td>
                  <NameCell
                    dashboard={d}
                    isEditing={editingId === d.id}
                    editingValue={editingValue}
                    editRef={editRef}
                    onSelect={onSelect}
                    onStartEdit={(id, name) => { setEditingId(id); setEditingValue(name); }}
                    onEditChange={setEditingValue}
                    onCommit={() => commitRename(d.id)}
                    onCancel={() => setEditingId(null)}
                  />
                  <td className="px-3 py-1.5 text-gray-400 text-sm">{d.chart_count ?? 0}</td>
                  <td className="px-3 py-1.5 text-gray-400 text-sm truncate">{d.created_by}</td>
                  <td className="px-3 py-1.5 text-gray-400 text-sm">{formatDate(d.updated_at)}</td>
                </tr>
              );
            })
          )}
        </tbody>
      </table>
    );
  };

  const SectionHeader = ({ title, count, sectionKey, isProject }) => {
    const isCollapsed = collapsedSections[sectionKey];
    return (
      <div
        onClick={() => toggleSection(sectionKey)}
        className="w-full flex items-center gap-2 px-3 py-2.5 bg-charcoal-400 border-b border-charcoal-200 hover:bg-charcoal-350 transition-colors text-left cursor-pointer"
      >
        <span className={`text-gray-400 text-xs transition-transform duration-200 ${isCollapsed ? '' : 'rotate-90'}`}>
          ▶
        </span>
        {isProject && editingProject === title ? (
          <input
            ref={editProjectRef}
            type="text"
            value={editingProjectValue}
            onChange={(e) => setEditingProjectValue(e.target.value)}
            onClick={(e) => e.stopPropagation()}
            onBlur={() => commitProjectRename(title)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') commitProjectRename(title);
              if (e.key === 'Escape') setEditingProject(null);
            }}
            className="px-1.5 py-0.5 bg-charcoal-700 border border-purple-500 rounded text-gray-200 text-sm font-semibold focus:outline-none w-48"
          />
        ) : isProject ? (
          <span
            onClick={(e) => e.stopPropagation()}
            onDoubleClick={() => { setEditingProject(title); setEditingProjectValue(title); }}
            className="text-sm font-semibold text-gray-200 cursor-text"
            title="Double-click to rename project"
          >
            {title}
          </span>
        ) : (
          <span className="text-sm font-semibold text-gray-200">{title}</span>
        )}
        <span className="text-xs text-gray-500 ml-1">({count})</span>
      </div>
    );
  };

  const handleInitialize = async () => {
    setInitLoading(true);
    try {
      await apiCall('POST', '/api/setup/initialize-database');
      onRefresh();
    } catch (err) {
      alert(`Initialization failed: ${err.message}`);
    } finally {
      setInitLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-96">
        <div className="w-16 h-16 border-4 border-charcoal-300 border-t-rust-light rounded-full animate-spin" />
        <p className="text-gray-400 mt-6 text-lg">Loading dashboards...</p>
      </div>
    );
  }

  if (error?.action === 'setup_required') {
    return (
      <div>
        <div className="mb-4">
          <h2 className="text-3xl font-bold text-rust-light mb-1">Dashboards</h2>
          <p className="text-gray-400 text-base">
            Create, organize, and share dashboards across projects
          </p>
        </div>
        <div className="my-4 p-5 bg-charcoal-500 border border-charcoal-200 rounded-lg">
          <p className="text-gray-300 mb-4">
            <span className="font-semibold text-yellow-400">[ NOTICE ]</span>{' '}
            <span className="font-semibold text-gray-100">Database Update Required:</span>{' '}
            In order to access the updated Dashboards tab, please click the button below to update the system tables. This will not affect your existing data.
          </p>
          <button
            onClick={handleInitialize}
            disabled={initLoading}
            className="px-4 py-2 bg-purple-600 text-gray-100 font-semibold border-0 rounded-md cursor-pointer hover:bg-purple-500 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {initLoading ? 'Initializing...' : 'Initialize Dashboard Tables'}
          </button>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="mb-4">
        <h2 className="text-3xl font-bold text-rust-light mb-1">Dashboards</h2>
        <p className="text-gray-400 text-base">
          Create, organize, and share dashboards across projects
        </p>
      </div>

      <div className="mb-3 flex gap-2 items-center">
        <button
          onClick={handleCreate}
          className="px-3 py-2 text-base bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500 transition-colors font-medium"
        >
          + New Dashboard
        </button>
        <button
          onClick={onRefresh}
          className="px-3 py-2 text-base bg-purple-600 text-gray-100 border-0 rounded-md cursor-pointer hover:bg-purple-500 transition-colors font-medium ml-auto"
        >
          Refresh
        </button>
      </div>

      {createError && (
        <div className="mb-4 p-3 bg-red-900/30 border border-red-600/50 rounded-lg text-red-300 text-sm">
          {createError}
        </div>
      )}

      {showDeleteConfirm && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={() => setShowDeleteConfirm(false)}>
          <div className="bg-charcoal-500 border border-red-700 rounded-lg p-4 max-w-md" onClick={e => e.stopPropagation()}>
            <h3 className="text-xl font-bold text-red-400 mb-2">Confirm Delete</h3>
            <p className="text-gray-300 mb-4">
              Are you sure you want to delete <strong>{selectedIds.size}</strong> dashboard{selectedIds.size !== 1 ? 's' : ''}? This action cannot be undone.
            </p>
            <div className="flex gap-2 justify-end">
              <button
                onClick={() => setShowDeleteConfirm(false)}
                className="px-3 py-1.5 bg-charcoal-600 text-gray-200 rounded hover:bg-charcoal-500"
              >
                Cancel
              </button>
              <button
                onClick={handleBulkDelete}
                className="px-3 py-1.5 bg-red-600 text-white rounded hover:bg-red-500"
              >
                Delete {selectedIds.size} item{selectedIds.size !== 1 ? 's' : ''}
              </button>
            </div>
          </div>
        </div>
      )}

      <div className="bg-charcoal-500 border border-charcoal-200 rounded-lg overflow-hidden">
        {/* Filter bar */}
        <div className="p-2 bg-charcoal-400 border-b border-charcoal-200">
          <div className="flex gap-2 items-center">
            <input
              type="text"
              placeholder="Filter by name..."
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              className="w-[24rem] px-3 py-1.5 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light"
            />
            <select
              value={filterProject}
              onChange={(e) => setFilterProject(e.target.value)}
              className="w-40 px-2 py-1.5 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm focus:outline-none focus:border-rust-light cursor-pointer"
            >
              <option value="">All Projects</option>
              {allProjects.map(p => (
                <option key={p} value={p}>{p}</option>
              ))}
            </select>

            <div className="ml-auto flex items-center gap-1.5">
              {someSelected ? (
                <>
                  <span className="text-purple-300 font-medium text-sm whitespace-nowrap mr-1">{selectedIds.size} selected</span>
                  <button
                    onClick={() => setShowDeleteConfirm(true)}
                    className="px-2 py-1 text-sm bg-red-600 text-gray-100 rounded hover:bg-red-500 transition-colors"
                    title="Delete selected"
                  >
                    Del
                  </button>
                  <button
                    onClick={() => setSelectedIds(new Set())}
                    className="px-2 py-1 text-sm bg-charcoal-600 text-gray-300 rounded hover:bg-charcoal-500 transition-colors whitespace-nowrap"
                  >
                    Clear
                  </button>
                </>
              ) : (
                <span className="text-gray-500 text-sm">Select items for bulk actions</span>
              )}
            </div>
          </div>
        </div>

        {/* My Dashboards */}
        {SectionHeader({ title: 'My Dashboards', count: grouped.myDashboards.length, sectionKey: 'my' })}
        {!collapsedSections['my'] && DashboardTable({ items: grouped.myDashboards })}

        {/* Project Sections */}
        {grouped.projectSections.map(section => (
          <React.Fragment key={section.name}>
            {SectionHeader({ title: section.name, count: section.items.length, sectionKey: `project-${section.name}`, isProject: true })}
            {!collapsedSections[`project-${section.name}`] && DashboardTable({ items: section.items })}
          </React.Fragment>
        ))}

        {grouped.myDashboards.length === 0 && grouped.projectSections.length === 0 && (
          <div className="text-center py-12 text-gray-500">
            <p className="text-base mb-1">No dashboards found</p>
            <p className="text-sm">
              {filterText || filterProject
                ? 'Try adjusting your filters.'
                : 'Click "+ New Dashboard" to create your first one.'}
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
