import React, { useState, useMemo } from 'react';
import { useCurrentUser } from '../App';
import { dashboardService, apiCall } from '../services/api';

export function DashboardDirectoryView({ dashboards, loading, error, onSelect, onRefresh }) {
  const currentUser = useCurrentUser();
  const [createError, setCreateError] = useState(null);
  const [collapsedSections, setCollapsedSections] = useState({});
  const [initLoading, setInitLoading] = useState(false);

  const toggleSection = (key) => {
    setCollapsedSections(prev => ({ ...prev, [key]: !prev[key] }));
  };

  const grouped = useMemo(() => {
    const myDashboards = [];
    const projectMap = {};

    (dashboards || []).forEach(d => {
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
  }, [dashboards, currentUser]);

  const handleCreate = async () => {
    setCreateError(null);
    try {
      const dash = await dashboardService.create({ name: 'Untitled Dashboard' });
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

  const DashboardTable = ({ items }) => (
    <table className="w-full">
      <thead>
        <tr className="border-b border-charcoal-300/50">
          <th className="text-left px-3 py-2 text-sm text-gray-400 font-semibold">Name</th>
          <th className="text-left px-3 py-2 text-sm text-gray-400 font-semibold">Charts</th>
          <th className="text-left px-3 py-2 text-sm text-gray-400 font-semibold">Created By</th>
          <th className="text-left px-3 py-2 text-sm text-gray-400 font-semibold">Last Updated</th>
        </tr>
      </thead>
      <tbody>
        {items.length === 0 ? (
          <tr>
            <td colSpan={4} className="text-center py-6 text-gray-500 text-sm italic">
              No dashboards
            </td>
          </tr>
        ) : (
          items.map(d => (
            <tr
              key={d.id}
              onClick={() => onSelect(d.id)}
              className="border-b border-charcoal-300/20 hover:bg-charcoal-400/50 transition-colors cursor-pointer"
            >
              <td className="px-3 py-2.5 text-gray-200 font-medium text-sm">{d.name}</td>
              <td className="px-3 py-2.5 text-gray-400 text-sm">{d.chart_count ?? 0}</td>
              <td className="px-3 py-2.5 text-gray-400 text-sm">{d.created_by}</td>
              <td className="px-3 py-2.5 text-gray-400 text-sm">{formatDate(d.updated_at)}</td>
            </tr>
          ))
        )}
      </tbody>
    </table>
  );

  const SectionHeader = ({ title, count, sectionKey }) => {
    const isCollapsed = collapsedSections[sectionKey];
    return (
      <button
        onClick={() => toggleSection(sectionKey)}
        className="w-full flex items-center gap-2 px-3 py-2.5 bg-charcoal-400 border-b border-charcoal-200 hover:bg-charcoal-350 transition-colors text-left"
      >
        <span className={`text-gray-400 text-xs transition-transform duration-200 ${isCollapsed ? '' : 'rotate-90'}`}>
          ▶
        </span>
        <span className="text-sm font-semibold text-gray-200">{title}</span>
        <span className="text-xs text-gray-500 ml-1">({count})</span>
      </button>
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

      <div className="mb-3 flex gap-2">
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

      {/* My Dashboards */}
      <div className="mb-4 bg-charcoal-500 border border-charcoal-200 rounded-lg overflow-hidden">
        <SectionHeader
          title="My Dashboards"
          count={grouped.myDashboards.length}
          sectionKey="my"
        />
        {!collapsedSections['my'] && (
          <DashboardTable items={grouped.myDashboards} />
        )}
      </div>

      {/* Project Sections */}
      {grouped.projectSections.map(section => (
        <div key={section.name} className="mb-4 bg-charcoal-500 border border-charcoal-200 rounded-lg overflow-hidden">
          <SectionHeader
            title={section.name}
            count={section.items.length}
            sectionKey={`project-${section.name}`}
          />
          {!collapsedSections[`project-${section.name}`] && (
            <DashboardTable items={section.items} />
          )}
        </div>
      ))}

      {grouped.myDashboards.length === 0 && grouped.projectSections.length === 0 && (
        <div className="text-center py-16 text-gray-500">
          <p className="text-lg mb-2">No dashboards yet</p>
          <p className="text-sm">Click "+ New Dashboard" to create your first one.</p>
        </div>
      )}
    </div>
  );
}
